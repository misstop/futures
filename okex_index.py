import requests
import json
import logging
import time
import yaml
import os
# import execjs
from kafka import KafkaProducer
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path))
kafka_con = x['INDEX']['KAFKA']['HOST']
kafka_topic = x['INDEX']['KAFKA']['topic']


# 日志设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='okex_index.log',
                    filemode='a')

symbol_ls = ['f_usd_btc', 'f_usd_ltc', 'f_usd_eth', 'f_usd_bch', 'f_usd_xrp', 'f_usd_eos', 'f_usd_etc', 'f_usd_btg']
# last_url = 'https://www.okex.com/v2/futures/pc/market/tickers.do?symbol=f_usd_btc{post}'
# index_url = 'https://www.okex.com/v2/market/index/ticker?symbol=f_usd_btc{get}'


# 获取最新价格
def get_last(s):
    try:
        re = requests.post('https://www.okex.com/v2/futures/pc/market/tickers.do', data={'symbol': s}, timeout=20)
    except Exception as e:
        logging.info("error-->%s" % e)
        time.sleep(2)
    res = json.loads(re.text)['data']
    this_week = res[0]['last']
    next_week = res[1]['last']
    quarter = res[2]['last']
    coin = s.split('_')[2].upper()
    return coin, this_week, next_week, quarter


# 获取现货指数
def get_index(s):
    try:
        re = requests.get('https://www.okex.com/v2/market/index/ticker?symbol=%s' % s, timeout=20)
        res = json.loads(re.text)['data']
        xh_last = res['last']
    except Exception as e:
        logging.info("error-->%s" % e)
        time.sleep(2)
    return xh_last


# 启动函数
def run():
    # 连接kafka
    producer = KafkaProducer(bootstrap_servers=kafka_con,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logging.info('kafka已连接')
    for s in symbol_ls:
        coin, this_week, next_week, quarter = get_last(s)
        logging.info('%s-----最新价格抓取成功' % s)
        xh_last = get_index(s)
        logging.info('%s-----现货指数抓取成功' % s)
        dic = {
            'coin': coin,
            'thisWeek': this_week,
            'nextWeek': next_week,
            'quarter': quarter,
            'spotIndex': xh_last,
        }
        producer.send(kafka_topic, dic)
        producer.flush()
        logging.info('-------send success------')
        time.sleep(5)
    logging.info('-----------------------------------------end-----------------------------------------')
    producer.close()


SCHEDULER = BlockingScheduler()
if __name__ == '__main__':
    SCHEDULER.add_job(func=run, trigger='interval', minutes=1)
    SCHEDULER.start()
