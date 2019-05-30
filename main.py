# -*- coding:utf-8 -*-



import sys

from kafkaMonitor import KafkaMonitor
from log import logger



if __name__ == '__main__':
    logger.info('--------------------------开始运行--------------------------')
    try:
        logger.info('执行操作：执行性能监控')
        interval, time = '1', '1'
        if len(sys.argv) == 2:
            interval = sys.argv[1]
        elif len(sys.argv) == 3:
            interval, time = sys.argv[1], sys.argv[2]

        exclustion = []
        if len(sys.argv) > 3:
            interval, time = sys.argv[1], sys.argv[2]
            exclustion = sys.argv[3:]

        if interval != '1':
            if not interval.isdigit():
                logger.error('采样时间间隔只能为数字')
                exit()

        if time != '1':
            temp = time.replace('+', '').replace(' ', '')
            if not temp.isdigit():
                print('采样持续时间只能为数字\算术表达式')
                exit()

        interval = int(interval)
        if interval <=0:
            interval = 1

        time = eval(time)
        if time <=0:
            time = 1

        count = int(time/interval)
        if count <=0:
            count = 1

        mornitor = KafkaMonitor(interval, count)
        mornitor.run()
        logger.info('程序运行结束')
    except Exception as e:
        logger.error('运行出错：%s' % e)
