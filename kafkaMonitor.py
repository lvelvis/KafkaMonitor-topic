#-*- encoding:utf-8 -*-



import configparser
import threading
import time

from collections import deque
from kafkaClient import MyKafkaClient
from influxdb import InfluxDBClient
from log import logger



class KafkaMonitor:
    data_queue = deque()
    def __init__(self, interval=1, count=1): # interval:指定采样时间间隔  count:指定采样次数
        self.interval = interval
        self.count = count

        self.brokers_dict = {} # {cluster1：brokers}
        self.topics_dict = {} # {cluster1：[topic1,topic2,...,topicN]}
        self.consumer_groups_dict = {} # {cluster1：{topic1：[consumer_group1, consumer_groups2, ..., consumer_groupN]}}
        try:
            config = configparser.ConfigParser()
            logger.info('正在读取brokers.conf配置')
            config.read('./conf/brokers.conf', encoding='utf-8')

            for section in config.sections():
                brokers_list  = [config[section][key].strip() for key in config.options(section)]
                brokers = str(brokers_list).strip().lstrip('[').rstrip(']').strip("'")
                self.brokers_dict[section] = brokers
        except Exception as e:
            logger.error('读取brokers.conf配置文件失败：%s' % e)
            exit(1)

        try:
            config.clear()
            logger.info('正在读取topics.conf配置')
            config.read('./conf/topics.conf', encoding='utf-8')
            for section in config.sections():
                if section in self.brokers_dict.keys():
                    topics_list  = [config[section][key].strip() for key in config.options(section)]
                    self.topics_dict[section] = topics_list
                else:
                    logger.warn('topics.conf配置错误，brokers.conf配置文件中未找到名称为%s的集群' % section)
        except Exception as e:
            logger.error('读取topics.conf配置文件失败：%s' % e)
            exit(1)


        try:
            config.clear()
            logger.info('正在读取consumer_groups.conf配置')
            config.read('./conf/consumer_groups.conf', encoding='utf-8')
            for section in config.sections():
                if section in self.brokers_dict.keys():
                    temp = {}
                    topic_with_consumer_groups_list  = [config[section][key].strip() for key in config.options(section)]
                    for item in topic_with_consumer_groups_list:
                        topic, consumer_groups = item.split('|')
                        topic = topic.strip()
                        consumer_group_list = [ item.strip() for item in consumer_groups.split(',') if item.strip()]
                        if topic in self.topics_dict[section]:
                            if consumer_group_list:
                                temp[topic]=consumer_group_list
                                self.consumer_groups_dict[section] = temp
                        else:
                            logger.warn('consumer_groups.conf配置错误，topics.conf配置文件未找到归属集群为%s,名称为%s的主题' % (section, topic))
                else:
                    logger.warn('配置错误，brokers.conf配置文件中未找到名称为%s的集群' % section)
        except Exception as e:
            logger.error('读取consumer_groups.conf配置文件失败：%s' % e)
            exit(1)


        try:
            config.clear()
            logger.info('正在读取influxdb数据库初始化配置')
            config = configparser.ConfigParser()
            config.read('./conf/influxDB.conf', encoding='utf-8')
            influxdb_host = config['INFLUXDB']['influxdb_host']
            influxdb_port = int(config['INFLUXDB']['influxdb_port'])

            self.influx_db_client = InfluxDBClient(influxdb_host, influxdb_port, timeout=10)

            self.database_list = self.influx_db_client.get_list_database()
            self.database_list = [dic_item['name'] for dic_item in self.database_list]
            logger.info('获取到的初始数据库列表：%s' % self.database_list)

            for key in self.brokers_dict.keys():
                key = 'db_%s' % key
                if key not in self.database_list:
                    logger.info('influxdb数据库 %s 不存在，正在创建数据库' % key)
                    self.influx_db_client.create_database(key)
        except Exception as e:
            logger.error('初始化influxDbClient失败：%s' % e)
            exit(1)


    def sample_topic_latest_offset(self, kafka_client, cluster, topic):
        topic_latest_offset_lasttime = {} # {cluster1_topic1:offset_data,} 存放上次采集的topic latest offset数据信息
        for i in range(0, self.count):
            timetuple = time.localtime() # 记录采样时间
            second_for_localtime1 = time.mktime(timetuple) # UTC时间（秒）
            sample_time = time.strftime('%Y-%m-%d %H:%M:%S', timetuple)

            result = kafka_client.get_topic_latest_offset(topic)
            topic_name = topic.name.decode('utf-8')

            if result[0]:
                topic_latest_offset = result[1]
                logger.debug('集群 %s 主题 %s offset：%s ' % (cluster, topic_name, topic_latest_offset))

                key = cluster + '_' + topic_name
                if key not in topic_latest_offset_lasttime:
                    topic_latest_offset_lasttime[key] = topic_latest_offset
                    topic_latest_offset_lasttime['sample_time_in_second'] = second_for_localtime1
                else:
                    temp_dict = {}
                    temp_dict['db'] = 'db_' + cluster # 采样存放数据库
                    temp_dict['topic'] = topic_name
                    temp_dict['sample_time'] = sample_time # 采样时间字符串
                    temp_dict['sample_time_in_second'] = second_for_localtime1 # 采样时间，单位 秒
                    temp_dict['sample_type'] = 'topic'  # 采样类型
                    temp_dict['sample_data'] = [topic_latest_offset_lasttime['sample_time_in_second'], topic_latest_offset_lasttime[key], topic_latest_offset] # 采样数据
                    KafkaMonitor.data_queue.append(temp_dict)

                    topic_latest_offset_lasttime[key] = topic_latest_offset
                    topic_latest_offset_lasttime['sample_time_in_second'] = second_for_localtime1
            else:
                logger.error('获取集群 %s 对应主题 %s 的latest offset失败:%s' % (cluster, topic_name, result[1]))

            second_for_localtime2 = time.mktime(time.localtime()) # UTC时间（秒）
            time_difference = second_for_localtime2 - second_for_localtime1
            if time_difference < self.interval: # 仅在耗时未超过指定时间间隔才进行休眠
                time.sleep(self.interval - time_difference)

    def sample_consumer_group_latest_offset(self, kafka_client, cluster, topic_name, consumer_group, auto_commit_interval):
        consumer_group_latest_offset_lasttime = {} # {cluster1_topic1_consumer_group1:offset_data} 存放上次采集的consumer group latest offset数据信息

        result = kafka_client.get_topic(topic_name) # 根据topic名称获取topic
        if result[0]:
            topic =  result[1]
        else:
            logger.error('根据主题名称%s获取主题失败' % topic_name)
            return

        key = cluster + '_' + topic_name + '_' + consumer_group
        for i in range(0, self.count):
            timetuple = time.localtime() # 记录采样时间
            second_for_localtime1 = time.mktime(timetuple) # UTC时间（秒）
            sample_time = time.strftime('%Y-%m-%d %H:%M:%S', timetuple)
            result = kafka_client.get_topic_latest_offset(topic)
            if result[0]:
                topic_latest_offset = result[1]
            else:
                logger.error('正在采集集群%s对应主题%s的latest offset失败：%s,跳过本次采集' % (cluster, topic_name, result[1]))
                continue

            result = kafka_client.get_consumer_group_latest_offset(consumer_group, topic_name)
            if result[0]:
                consumer_group_latest_offset = result[1]
                logger.debug('集群 %s 主题 %s 消费组 %s offset：%s ' % (cluster, topic_name, consumer_group, consumer_group_latest_offset))

                if key not in consumer_group_latest_offset_lasttime:
                    consumer_group_latest_offset_lasttime[key] = consumer_group_latest_offset
                    consumer_group_latest_offset_lasttime['sample_time_in_second'] = second_for_localtime1
                else:
                    temp_dict = {}
                    temp_dict['db'] = 'db_' + cluster # 采样存放数据库
                    temp_dict['topic'] = topic_name
                    temp_dict['consumer_group'] = consumer_group
                    temp_dict['sample_time'] = sample_time # 采样时间字符串
                    temp_dict['sample_time_in_second'] = second_for_localtime1 # 采样时间，单位 秒
                    temp_dict['sample_type'] = 'consumer_group'  # 采样类型
                    temp_dict['sample_data'] = [consumer_group_latest_offset_lasttime['sample_time_in_second'], consumer_group_latest_offset_lasttime[key], consumer_group_latest_offset] # 采样数据
                    temp_dict['topic_latest_offset'] = topic_latest_offset
                    KafkaMonitor.data_queue.append(temp_dict)
                    consumer_group_latest_offset_lasttime[key] = consumer_group_latest_offset
                    consumer_group_latest_offset_lasttime['sample_time_in_second'] = second_for_localtime1
            else:
                logger.error('获取集群 %s 对应主题 %s 对应消费组 %s latest offset失败:%s' % (cluster, topic_name, consumer_group, result[1]))

            second_for_localtime2 = time.mktime(time.localtime()) # UTC时间（秒）
            time_difference = second_for_localtime2 - second_for_localtime1
            if time_difference < auto_commit_interval:
                time.sleep(auto_commit_interval - time_difference)


    def sample_consumer_groups_latest_offset(self, kafka_client, cluster, topic_name, consumer_group_list, max_auto_commit_interval):
        consumer_groups_latest_offset_lasttime = {} # {cluster1_topic1:[consumer_group1_latest_offset,consumer_group2_latest_offset]} 存放上次采集的consumer group latest offset数据信息

        key = cluster + '_' + topic_name
        for i in range(0, self.count):
            timetuple = time.localtime() # 记录采样时间
            second_for_localtime1 = time.mktime(timetuple) # UTC时间（秒）
            sample_time = time.strftime('%Y-%m-%d %H:%M:%S', timetuple)


            error = False
            temp_list = []
            for consumer_group in consumer_group_list:
                result = kafka_client.get_consumer_group_latest_offset(consumer_group, topic_name)
                if result[0]:
                    consumer_group_latest_offset = result[1]
                    temp_list.append(consumer_group_latest_offset)
                else:
                    logger.error('获取集群 %s 对应主题 %s 对应消费组 %s latest offset失败:%s,结束本次采集' % (cluster, topic_name, consumer_group, result[1]))
                    error = True
                    break
            if error:
                continue

            if key not in consumer_groups_latest_offset_lasttime.keys():
                consumer_groups_latest_offset_lasttime[key] = temp_list
                consumer_groups_latest_offset_lasttime['sample_time_in_second'] = second_for_localtime1
            else:
                temp_dict = {}
                temp_dict['db'] = 'db_' + cluster # 采样存放数据库
                temp_dict['topic'] = topic_name
                temp_dict['sample_time'] = sample_time # 采样时间字符串
                temp_dict['sample_time_in_second'] = second_for_localtime1 # 采样时间，单位 秒
                temp_dict['sample_type'] = 'consumer_groups'  # 采样类型
                temp_dict['sample_data'] = [consumer_groups_latest_offset_lasttime['sample_time_in_second'], consumer_groups_latest_offset_lasttime[key], temp_list] # 采样数据
                KafkaMonitor.data_queue.append(temp_dict)
                consumer_groups_latest_offset_lasttime[key] = temp_list
                consumer_groups_latest_offset_lasttime['sample_time_in_second'] = second_for_localtime1


            second_for_localtime2 = time.mktime(time.localtime()) # UTC时间（秒）
            time_difference = second_for_localtime2 - second_for_localtime1
            if time_difference < max_auto_commit_interval:
                time.sleep(max_auto_commit_interval - time_difference)

    def sample_topic_partitions_latest_offset(self, kafka_client, cluster, topic_name, partition_id_list):
            for i in range(0, self.count):
                timetuple = time.localtime()
                second_for_localtime1 = time.mktime(timetuple) # UTC时间（秒）
                sample_time = time.strftime('%Y-%m-%d %H:%M:%S', timetuple) # 记录采样时间
                temp_dict = {}
                temp_dict['db'] = 'db_' + cluster # 采样存放数据库
                temp_dict['topic'] = topic_name
                temp_dict['sample_time'] = sample_time # 采样时间
                temp_dict['sample_type'] = 'topic_partitions'  # 采样类型
                temp_dict['sample_data'] = [] # 采样数据
                for partition_id in partition_id_list:
                    result = kafka_client.get_partition_latest_offset(partition_id, topic_name)
                    if result[0]:
                        partition_latest_offset = result[1]
                        temp_dict['sample_data'].append({str(partition_id):partition_latest_offset-1})
                        logger.debug('集群 %s 主题 %s 分区 %s offset：%s ' % (cluster, topic_name, partition_id, str(partition_latest_offset)))
                    else:
                        logger.error('获取集群 %s 对应主题 %s 对应分区 %s latest offset失败:%s' % (cluster, topic_name, partition_id, result[1]))

                KafkaMonitor.data_queue.append(temp_dict)

                second_for_localtime2 = time.mktime(time.localtime()) # UTC时间（秒）
                time_difference = second_for_localtime2 - second_for_localtime1
                if time_difference < self.interval: # 仅在耗时未超过指定时间间隔才进行休眠
                    time.sleep(self.interval - time_difference)


    def parse_data(self, running_batch_no):
        while len(KafkaMonitor.data_queue):
            data = KafkaMonitor.data_queue.popleft()
            database =  data['db']
            sample_type = data.get('sample_type')
            sample_data = data.get('sample_data')
            sample_time = data.get('sample_time')
            sample_time_currtime = data.get('sample_time_in_second')

            timetuple = time.strptime(sample_time, '%Y-%m-%d %H:%M:%S')
            second_for_sampletime_utc = int(time.mktime(timetuple)) - 8 * 3600 # UTC时间（秒）
            timetuple = time.localtime(second_for_sampletime_utc)
            date_for_data = time.strftime('%Y-%m-%d', timetuple)
            time_for_data = time.strftime('%H:%M:%S', timetuple)
            datetime_for_data = '%sT%sZ' % (date_for_data, time_for_data)

            if sample_type == 'topic': # 计算主题生产速率
                topic = data.get('topic')
                sample_time_lasttime = sample_data[0] # 采样时间对应秒数
                sample_data_lasttime = sample_data[1]
                topic_offset_lasttime = 0 # 记录上次的偏移记录
                for partition_id, offset_partition_response in sample_data_lasttime.items():
                    # logger.info(offset_partition_response.offset) # list
                    # logger.info(offset_partition_response.err)
                    topic_offset_lasttime += offset_partition_response.offset[0]

                sample_data_currtime = sample_data[2]
                topic_offset_currtime = 0 # 记录当前的偏移记录
                for partition_id, offset_partition_response in sample_data_currtime.items():
                    topic_offset_currtime += offset_partition_response.offset[0]

                temp = topic_offset_currtime - topic_offset_lasttime
                time_diff = sample_time_currtime - sample_time_lasttime
                if time_diff:
                    topic_consume_rate =  temp / time_diff
                else:
                    logger.warn('采样频率太小，无法获取主题生产速率')
                    continue
                fields = {'produce_rate':topic_consume_rate}

                json_records_list = [{
                    "measurement": 'topic_production_rate',
                    "tags": {
                        "batchNo": running_batch_no,
                        "topic":topic
                    },
                    "fields":fields
                }]

            elif sample_type == 'consumer_group':
                topic = data.get('topic')
                consumer_group = data.get('consumer_group')
                sample_time_lasttime = sample_data[0] # 采样时间对应秒数
                sample_data_lasttime =  sample_data[1]
                topic_latest_offset =  data.get('topic_latest_offset')

                topic_latest_offset_int = 0 # 记录当前主题的偏移记录
                for partition_id, offset_partition_response in topic_latest_offset.items():
                    topic_latest_offset_int += offset_partition_response.offset[0]

                consumer_group_offset_lasttime = 0 # 记录上次的偏移记录
                for topic, offset_info in  sample_data_lasttime.items():
                    for partition_id, offset_partition_response in offset_info.items():
                        consumer_group_offset_lasttime += offset_partition_response.offset

                sample_data_currtime = sample_data[2]
                consumer_group_offset_currtime = 0 # 记录当前的偏移记录
                consumer_group_offset = 0 # 记录当前的偏移记录，和consumer_group_offset_currtime不一样的地方在于当消费组offset为-1的，强制换为0
                for topic, offset_info in  sample_data_currtime.items():
                    for partition_id, offset_partition_response in offset_info.items():
                        consumer_group_offset_currtime += offset_partition_response.offset
                        if offset_partition_response.offset == -1:
                            consumer_group_offset += 0
                        else:
                            consumer_group_offset += offset_partition_response.offset

                temp = consumer_group_offset_currtime - consumer_group_offset_lasttime
                time_diff = sample_time_currtime - sample_time_lasttime
                if time_diff:
                    consumer_group_consume_rate =  temp / time_diff
                else:
                    logger.warn('采样频率太小，无法获取消费组消费速率')
                    continue

                remain = topic_latest_offset_int - consumer_group_offset # 计算待消费剩余量

                fields = {'consume_rate':consumer_group_consume_rate, 'remain':remain}
                json_records_list = [{
                    "measurement": 'consumer_group_consume_rate',
                    "tags": {
                        "batchNo": running_batch_no,
                        "topic":topic,
                        "consumerGroup": consumer_group
                    },
                    "fields":fields
                }]
            elif sample_type == 'topic_partitions':
                topic = data.get('topic')
                fields = {}
                prefix = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
                i = 0
                temp = 10
                for item in  sample_data:
                    for key, offset in item.items():
                        fields['%s-P%s' % (str(prefix[i]), key)] = offset
                        prefix[i] += temp * 9
                    i += 1
                    if i % 10 == 0:
                        i = 0
                        temp = temp * 10

                json_records_list = [{
                    "measurement": 'topic_partition_offset',
                    "tags": {
                        "batchNo": running_batch_no,
                        "topic":topic
                    },
                    "fields":fields
                }]
            elif sample_type == 'consumer_groups':
                topic = data.get('topic')
                sample_time_lasttime = sample_data[0] # 采样时间对应秒数
                sample_data_lasttime =  sample_data[1]

                consumer_groups_offset_lasttime = 0 # 记录上次的偏移记录
                for item in sample_data_lasttime:
                    for topic, offset_info in  item.items():
                        for partition_id, offset_partition_response in offset_info.items():
                            consumer_groups_offset_lasttime += offset_partition_response.offset

                sample_data_currtime = sample_data[2]
                consumer_groups_offset_currtime = 0 # 记录当前的偏移记录
                for item in sample_data_currtime:
                    for topic, offset_info in  item.items():
                        for partition_id, offset_partition_response in offset_info.items():
                            consumer_groups_offset_currtime += offset_partition_response.offset

                temp = consumer_groups_offset_currtime - consumer_groups_offset_lasttime
                time_diff = sample_time_currtime - sample_time_lasttime
                if time_diff:
                    topic_consume_rate =  temp / time_diff
                else:
                    logger.warn('采样频率太小，无法获取所有主题消费速率(所有消费组的组合消费速率)')
                    continue

                fields = {'consume_rate':topic_consume_rate}
                json_records_list = [{
                    "measurement": 'topic_consume_rate',
                    "tags": {
                        "batchNo": running_batch_no,
                        "topic":topic
                    },
                    "fields":fields
                }]

            result = self.influx_db_client.write_points(json_records_list, database=database)
            if not result:
                logger.error('采集性能数据失败-往InfluxDB数据库：%s\n 写入失败数据：%s\n' % (database, str(json_records_list)))
            else:
                logger.info('往数据库:%s 成功写入数据' % database)

    def run(self):
        thread_group_list1 = [] # 存放数据采集线程
        timetuple = time.localtime()
        second_for_localtime = int(time.mktime(timetuple))
        TIME_CONSTANT = 99999999999999 # 常量值
        sample_time = time.strftime('%Y-%m-%d %H:%M:%S', timetuple)
        running_batch_no = str(TIME_CONSTANT - second_for_localtime) + '-' + sample_time
        for cluster, hosts in self.brokers_dict.items():
            if not hosts:
                logger.warn('请注意：构建集群(%s)对应的kafka客户端失败：brokers.conf broker配置为空,不对该集群监控集群' % cluster)
                continue
            kafka_client = MyKafkaClient(cluster, hosts)

            # 采集主题偏移信息
            topic_name_list = self.topics_dict.get(cluster)
            for topic_name in topic_name_list:
                result = kafka_client.get_topic(topic_name) # 根据topic名称获取topic
                if result[0]:
                    topic =  result[1]
                else:
                    logger.error('根据主题名称%s获取主题失败' % topic_name)
                    continue
                logger.info('正在采集集群%s对应主题%s的latest offset' % (cluster, topic_name))
                thread = threading.Thread(target=self.sample_topic_latest_offset, args=(kafka_client, cluster, topic))
                thread.start()
                thread_group_list1.append(thread)

            # 采集与主题对应的消费组的偏移信息
            topic_consumer_groups_dict = self.consumer_groups_dict.get(cluster)
            if topic_consumer_groups_dict:
                for topic, consumer_group_list in topic_consumer_groups_dict.items():
                    consumer_group_name_list = [] # 存放话题对应的消费组（不含 自动提交offset值
                    if topic in topic_name_list:
                        has_config_error = False
                        max_auto_commit_interval = self.interval
                        for consumer_group_info in consumer_group_list:
                            auto_commit_interval = self.interval
                            consumer_group_info = consumer_group_info.replace('：', ':') # 转换配置文件中用户不小心输入的中文冒号
                            temp_list  = consumer_group_info.split(':')
                            consumer_group = temp_list[0]
                            consumer_group_name_list.append(consumer_group)
                            if len(temp_list) == 2:
                                auto_commit_interval_ms = temp_list[1].strip().replace(' ', '')
                                if not auto_commit_interval_ms.isdigit():
                                    logger.error('集群 %s 对应主题 %s 对应消费组 %s auto_commit_interval_ms 配置错误：非数字，不采集该消费组的latest offset' % (cluster, topic, consumer_group))
                                    has_config_error = True
                                    continue
                                else:
                                    auto_commit_interval = int(auto_commit_interval_ms) / 1000 # 转 毫秒为秒，所以要 / 1000
                                    if max_auto_commit_interval < auto_commit_interval:
                                        max_auto_commit_interval = auto_commit_interval + 1
                            logger.info('正在采集集群 %s 对应主题 %s 消费组 %s 的latest offset' % (cluster, topic, consumer_group))

                            if auto_commit_interval > self.interval:
                                auto_commit_interval = auto_commit_interval + 1
                                logger.warn('集群 %s 对应主题 %s 对应消费组 %s auto_commit_interval_ms 大于采集频率:%s 秒,该消费组采集频率自动调整为：%s 秒' % (cluster, topic, consumer_group,  self.interval, auto_commit_interval))
                            else:
                                auto_commit_interval = self.interval
                            thread = threading.Thread(target=self.sample_consumer_group_latest_offset, args=(kafka_client, cluster, topic, consumer_group, auto_commit_interval))
                            thread.start()
                            thread_group_list1.append(thread)

                        if not has_config_error:
                            logger.info('正在采集计算集群 %s 对应主题 %s消费速率所需相关数据' % (cluster, topic_name))
                            thread = threading.Thread(target=self.sample_consumer_groups_latest_offset, args=(kafka_client, cluster, topic_name, consumer_group_name_list, max_auto_commit_interval))
                            thread.start()
                            thread_group_list1.append(thread)
                        else:
                            logger.error('集群 %s 对应主题 %s 对应的部分消费组存在配置错误，不采集计算该主题消费速率所需相关数据' % (cluster, topic_name))
                    else:
                        logger.warn('请注意：brokers.conf中未找到名称为%s的主题，已跳过' % topic_name)

                    # 采集主题分区offset
                    for topic_name in topic_name_list:
                        topic_partitions = kafka_client.get_topic_partitions_dict().get(topic_name)
                        if topic_partitions:
                            topic_partition_id_list = list(topic_partitions.keys())
                            logger.info('正在采集集群 %s 对应主题 %s 对所有分区的latest offset' % (cluster, topic_name))
                            thread = threading.Thread(target=self.sample_topic_partitions_latest_offset, args=(kafka_client, cluster, topic_name, topic_partition_id_list))
                            thread.start()
                            thread_group_list1.append(thread)


        active_thread_group_list1 = [t for t in thread_group_list1 if t.is_alive()] # 获取监控性采集线程组中还活着的线程
        active_thread_group_list2 = [] # 存储解析数据线程组中还活着的线程

        while active_thread_group_list1: # 还有存活的数据采集线程
            active_thread_group_list2 = [t for t in active_thread_group_list2 if t.is_alive()]
            thread_num_to_new = len(active_thread_group_list1) - len(active_thread_group_list2)

            # 根据差值动态创建线程
            for i in range(0, thread_num_to_new):
                thread = threading.Thread(target=self.parse_data,
                                          args=(running_batch_no,))
                thread.start()
                active_thread_group_list2.append(thread)
            active_thread_group_list1 = [t for t in thread_group_list1 if t.is_alive()]
        else:
            while KafkaMonitor.data_queue: # 队列还有数据
                thread_num_to_new = len(thread_group_list1) - len(active_thread_group_list2)

                # 根据差值动态创建线程
                for i in range(0, thread_num_to_new):
                    thread = threading.Thread(target=self.parse_data,
                                              args=(running_batch_no,))
                    thread.start()
                    active_thread_group_list2.append(thread)

        # 以下代码不可用，因为pykafka类库的原因，会生成daemon线程，threading.active_count()一直不为1，死循环
        # while current_active_thread_num != 1:
        #     active_thread_group_list2 = [t for t in active_thread_group_list2 if t.is_alive()]
        #     thread_num_to_new = len(active_thread_group_list1) - len(active_thread_group_list2)
        #
        #     # 根据差值动态创建线程
        #     for i in range(0, thread_num_to_new):
        #         thread = threading.Thread(target=self.parse_data,
        #                                   args=('',))
        #         thread.start()
        #         active_thread_group_list2.append(thread)
        #     current_active_thread_num = threading.active_count()
        #     active_thread_group_list1 = [t for t in thread_group_list1 if t.is_alive()]



