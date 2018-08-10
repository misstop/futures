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
kafka_con = x['ELITE']['KAFKA']['HOST']
kafka_topic = x['ELITE']['KAFKA']['topic']

# 日志设置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='okex_elite.log',
                    filemode='a')

symbol_ls = ['f_usd_btc', 'f_usd_ltc', 'f_usd_eth', 'f_usd_bch', 'f_usd_xrp', 'f_usd_eos', 'f_usd_etc', 'f_usd_btg']
chart_type = [0, 1, 2]
# 折线统计图
# url1 = "https://www.okex.com/v2/futures/pc/public/eliteScale.do?symbol=f_usd_btc&type=1"
# 条形统计图
# url2 = "https://www.okex.com/v2/futures/pc/public/getFuturePositionRatio.do?symbol=f_usd_btc&type=2"
maps = {
    0: "5m",
    1: "15m",
    2: "1h",
}


# 获取条形
def get_msg(s):
    alldata = {}
    dmi = {}
    avePosition = {}
    try:
        for t in chart_type:
            res1 = requests.get('https://www.okex.com/v2/futures/pc/public/eliteScale.do?symbol=%s&type=%s' % (s, t),
                                timeout=30)
            res1 = json.loads(res1.text)['data']
            dmi["buydata"] = res1['buydata']
            dmi["timedata"] = res1['timedata']
            time.sleep(5)
            res2 = requests.get(
                'https://www.okex.com/v2/futures/pc/public/getFuturePositionRatio.do?symbol=%s&type=%s' % (s, t),
                timeout=30)
            res2 = json.loads(res2.text)['data']
            avePosition['buydata'] = res2['buydata']
            avePosition['selldata'] = res2['selldata']
            avePosition['timedata'] = res2['timedata']
            time.sleep(5)
            alldata[maps[t]] = {
                "dmi": dmi,
                "avePosition": avePosition,
            }
    except Exception as e:
        logging.info("error-->%s" % e)
        time.sleep(2)

    return alldata


# 启动函数
def run():
    # 连接kafka
    producer = KafkaProducer(bootstrap_servers=kafka_con,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    logging.info('kafka connect')
    for s in symbol_ls:
        alldata = get_msg(s)
        logging.info('%s---crawl success' % s)
        dic = {
            'coin': s,
            'alldata': alldata,
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

