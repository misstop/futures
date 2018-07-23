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
kafka_con = x['TOP']['KAFKA']['HOST']
kafka_topic = x['TOP']['KAFKA']['topic']


# 日志设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='okex_top.log',
                    filemode='a')

symbol_ls = ['f_usd_btc', 'f_usd_ltc', 'f_usd_eth', 'f_usd_bch', 'f_usd_xrp', 'f_usd_eos', 'f_usd_etc', 'f_usd_btg']
# holdAmount_url = 'https://www.okex.com/v2/futures/pc/market/tickers.do{post symbol f_usd_eos}'
# line_url = 'https://www.okex.com/v2/futures/pc/public/futureVolume.do?symbol=f_usd_eos&type=1{get}'
# top_url = 'https://www.okex.com/v2/futures/pc/public/futureTop.do?symbol=f_usd_eos&type=0{get}'
mid_type = [1, 2, 3]


# 获取头部持仓量
def get_head(s):
    try:
        re = requests.post('https://www.okex.com/v2/futures/pc/market/tickers.do', data={'symbol': s}, timeout=20)
    except Exception as e:
        logging.info("error-->%s" % e)
        time.sleep(2)
    res = json.loads(re.text)['data']
    holdAmount = res[0]['holdAmount']+res[1]['holdAmount']+res[2]['holdAmount']
    volume = res[0]['volume']+res[1]['volume']+res[2]['volume']
    coin = s.split('_')[2].upper()
    return holdAmount, volume, coin


# 获取中间的图表数据
def mid_msg(s):
    mid_ls = {}
    try:
        for t in mid_type:
            re = requests.get('https://www.okex.com/v2/futures/pc/public/futureVolume.do?symbol=%s&type=%s' % (s, t), timeout=20)
            res = json.loads(re.text)['data']
            mid_ls['data%s' % t] = res
            time.sleep(3)
    except Exception as e:
        logging.info("error-->%s" % e)
        time.sleep(2)
    return mid_ls


# 获取底部数据
def foot_msg(s):
    try:
        re = requests.get('https://www.okex.com/v2/futures/pc/public/futureTop.do?symbol=%s&type=0' % s, timeout=20)
    except Exception as e:
        logging.info("error-->%s" % e)
        time.sleep(2)
    res = json.loads(re.text)['data']
    dateArry = res['dateArry']
    list1 = [l1['coinVolume'] for l1 in res['marketDataTopList1']]
    list2 = [l2['coinVolume'] for l2 in res['marketDataTopList2']]
    list3 = [l3['coinVolume'] for l3 in res['marketDataTopList3']]
    list4 = [l4['coinVolume'] for l4 in res['marketDataTopList4']]
    list5 = [l5['coinVolume'] for l5 in res['marketDataTopList5']]
    list6 = [l6['coinVolume'] for l6 in res['marketDataTopList6']]
    return dateArry, list1, list2, list3, list4, list5, list6


# 启动函数
def run():
    # 连接kafka
    producer = KafkaProducer(bootstrap_servers=kafka_con,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logging.info('kafka已连接')
    for s in symbol_ls:
        holdAmount, volume, coin = get_head(s)
        logging.info('%s---head抓取成功' % s)
        mid_ls = mid_msg(s)
        logging.info('%s---mid抓取成功' % s)
        dateArry, list1, list2, list3, list4, list5, list6 = foot_msg(s)
        logging.info('%s---foot抓取成功' % s)
        dic = {
            'coin': coin,
            'holdAmount': holdAmount,
            'volume': volume,
            'line': mid_ls,
            'dateArry': dateArry,
            'list1': list1,
            'list2': list2,
            'list3': list3,
            'list4': list4,
            'list5': list5,
            'list6': list6,
        }
        producer.send(kafka_topic, dic)
        producer.flush()
        logging.info('-------send success------')
        time.sleep(5)
    logging.info('-----------------------------------------end-----------------------------------------')
    producer.close()


SCHEDULER = BlockingScheduler()
if __name__ == '__main__':
    SCHEDULER.add_job(func=run, trigger='interval', minutes=5)
    SCHEDULER.start()
