import requests
import json
import logging
import time
import os
import yaml
from kafka import KafkaProducer
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path))
kafka_con = x['HOLDAMOUNT']['KAFKA']['HOST']
kafka_topic = x['HOLDAMOUNT']['KAFKA']['topic']


# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='okex_holdAmount.log',
                    filemode='a')


# 本地时间转换为13位
def cur_time():
    t1 = time.time()
    t2 = int(t1 * 1000)
    return t2


# 交易对参数
symbols = ['f_usd_btc', 'f_usd_ltc', 'f_usd_eth', 'f_usd_etc', 'f_usd_bch', 'f_usd_btg', 'f_usd_xrp', 'f_usd_eos']


# send to Kafka
def send_msg():
    producer = KafkaProducer(bootstrap_servers=kafka_con,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logging.info('kafka已连接')
    for s in symbols:
        try:
            re = requests.post('https://www.okex.com/v2/futures/pc/market/tickers.do', data={"symbol": s})
        except Exception as e:
            logging.info("访问失败----%s" % e)
            time.sleep(5)
            continue
        data = json.loads(re.text)
        if data['data']:
            d = data['data']
            msg = {
                'holdAmount': d[0]['holdAmount']+d[1]['holdAmount']+d[2]['holdAmount'],
                'coin': s[-3:].upper(),
                'timestamp': cur_time(),
            }
            producer.send(kafka_topic, msg)
            time.sleep(5)
            logging.info('success to send kafka>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    producer.close()
    logging.info('kafka已断开连接')


# 函数入口
SCHEDULER = BlockingScheduler()
if __name__ == "__main__":
    SCHEDULER.add_job(func=send_msg, trigger='interval', minutes=3)
    SCHEDULER.start()
