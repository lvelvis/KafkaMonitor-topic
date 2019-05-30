# -*- coding:utf-8 -*-

__author__ = 'elvis'

import configparser
from influxdb import InfluxDBClient
from log import logger

if __name__ == '__main__':

    try:
        logger.info('正在读取influxdb数据库初始化配置')
        config = configparser.ConfigParser()
        config.read('./conf/influxDB.conf', encoding='utf-8')
        influxdb_host = config['INFLUXDB']['influxdb_host']
        influxdb_port = int(config['INFLUXDB']['influxdb_port'])

        influx_db_client = InfluxDBClient(influxdb_host, influxdb_port, timeout=10)
        database_list = influx_db_client.get_list_database()
        database_list = [dic_item['name'] for dic_item in database_list]
        logger.info('获取到的初始数据库列表：%s' % database_list)

        index_set = set()
        choice = None
        while choice not in index_set:
            logger.info('-----------------------------------------')
            for item in database_list:
                if item not in ['_internal', 'jmeter', 'collectd']:
                    logger.info('id：%s db：%s' % (database_list.index(item), item))
                    index_set.add(database_list.index(item))
            index_set.add('all')
            index_set.add('q')
            logger.info('-----------------------------------------')
            logger.info('请输入要删除目标数据库的id（删除全部，请输入 all 退出，请输入 q）：')
            choice = input()
            if choice.isdigit():
                choice = int(choice)
            else:
                choice = choice.lower()

        if choice not in ['all', 'q']:
            influx_db_client.drop_database(database_list[choice])
            logger.info('成功删除数据库：%s' % database_list[choice])
        elif choice == 'all':
            index_set.remove('all')
            index_set.remove('q')
            for index in index_set:
                db = database_list[index]
                logger.info('正在删除数据库：%s' % db)
                influx_db_client.drop_database(db)
            logger.info('成功删除全部数据库')

        influx_db_client.close()


    except Exception as e:
        logger.error('程序初始化操作失败：%s' % e)
        exit()
