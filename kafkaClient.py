#-*- encoding:utf-8 -*-



from pykafka import KafkaClient,Cluster,handlers
from pykafka.protocol import  PartitionOffsetFetchRequest
import configparser

from log import logger


class MyKafkaClient:
    def __init__(self, cluster, hosts):
        try:
            logger.info('正在构建集群(%s)对应的kafka客户端' % cluster)
            self.client = KafkaClient(hosts=hosts)

            logger.info('正在获取cluster：%s' % cluster)
            handler = handlers.ThreadingHandler()
            self.cluster = Cluster(hosts=hosts, handler=handler)

            logger.info('正在获取集群 %s 对应的主题' % cluster)
            result = self.get_topics()
            if result[0]:
                self.topics = result[1]
            else:
                logger.error('获取集群 %s 对应的主题失败：%s，退出程序' % (cluster, result[1]))
                exit(1)

            self.topic_partitions_dict = {} # 存放主题分区
            for topic_name in self.topics.keys():
                topic = self.topics[topic_name]
                topic_name = topic_name.decode('utf-8')
                result = self.get_topic_partitions(topic)
                if result[0]:
                    self.topic_partitions_dict[topic_name] = result[1]
                else:
                    logger.error('获取集群 %s 对应主题 %s 的所有分区失败：%s，退出程序' % (cluster, topic_name, result[1]))
                    exit(1)
        except Exception as e:
            logger.error('初始化kafka客户端程序失败：%s，退出程序' % e)
            exit(1)


    def get_topics(self):
        '''获取所有topic'''
        try:
            topics = self.client.topics
            return [True, topics]
        except Exception as e:
            logger.error('获取所有topic失败：%s' % e)
            return [False, '%s' % e]

    def get_topic_partitions(self, topic):
        '''获取 topic partitions'''
        try:
            partitions = topic.partitions
            return [True, partitions]
        except Exception as e:
            logger.error('获取主题%s 分区失败：%s' % (topic.name.decode('utf-8'), e))
            return [False, '%s' % e]


    def get_topic(self, topic_name):
        '''根据topic名称获取topic'''
        try:
            topics = self.client.topics # topics结构： {b'MY_TOPIC1': None, b'MY_TOPIC2': None}
            topic_name = topic_name.encode('utf-8') #
            if topic_name in topics.keys():
                return [True, topics[topic_name]]
            else:
                logger.error('根据topic名称（%s）获取topic失败' % topic_name.decode('utf-8'))
                return [False, '%s 不存在' % topic_name]
        except Exception as e:
            logger.error('根据topic名称（%s）获取topic失败:%s' % (topic_name, e))
            return [False, '%s' % e]


    def get_topic_latest_offset(self, topic):
        ''' 获取topic latest offset
         Get the offset of the next message that would be appended to each partition of this topic.
        '''
        try:
            topic_latest_offset = topic.latest_available_offsets()
            return [True, topic_latest_offset]
        except Exception as e:
            logger.error('获取主题 %s latest offset失败：%s' % (topic.name.decode('utf-8'), e))
            return [False, '%s' % e]


    def get_partition_latest_offset(self, partition_id, topic_name):
        ''' 获取partition latest offset
            Get the offset of the next message that would be appended to this partition
            说明：此处通过self.topic_partitions_dict中保存的partition获取topic的值None,因此需要动态获取分区
        '''
        try:
            result = self.get_topic(topic_name)
            if result[0]:
                topic = result[1]
            else:
                logger.error('获取topic %s 失败：%s' % (topic_name, result[1]))
                return [False, '获取topic %s 失败：%s' % (topic_name, result[1])]

            result = self.get_topic_partitions(topic)
            if result[0]:
                topic_partitions = result[1]
            else:
                logger.error('获取主题 %s 的所有分区失败：%s' % (topic_name, result[1]))
                return [False, '获取topic %s 失败：%s' % (topic_name, result[1])]

            if topic_partitions:
                partition = topic_partitions.get(int(partition_id))
                if partition:
                    partition_latest_offset = partition.latest_available_offset()
                    return [True, partition_latest_offset]
                else:
                    logger.error('获取主题 %s 对应分区 %s latest offset 失败：分区 %s 不存在' % (topic_name, partition_id, partition))
                    return [False, '获取主题 %s 对应分区 %s latest offset 失败：分区 %s 不存在' % (topic_name, partition_id, partition)]
            else:
                logger.error('获取主题 %s 对应分区 %s latest offset 失败：%主题分区不存在' % (topic_name, partition_id, topic_name))
                return [False, '%s主题分区不存在' % topic_name]
        except Exception as e:
            logger.error('获取主题 %s 对应分区 %s latest offset失败：%s' % (topic_name, partition_id,  e))
            return [False, '%s' % e]

    def get_consumer_group_latest_offset(self, consumer_group, topic_name):
        ''' 获取consumer_group  latest offset
            Fetch the offsets stored in Kafka with the Offset Commit/Fetch API
        '''
        try:
            topic_partitions = self.topic_partitions_dict.get(topic_name)
            if topic_partitions:
                topic_partition_id_list = list(topic_partitions.keys())
                consumer_group_byte = consumer_group.encode('utf-8')
                topic_name_byte = topic_name.encode('utf-8')
                reqs_list = [PartitionOffsetFetchRequest(topic_name_byte, i) for i in topic_partition_id_list]
                offset_fetch_response = self.cluster.get_group_coordinator(consumer_group_byte).fetch_consumer_group_offsets(consumer_group_byte, reqs_list)
                consumer_group_offsets = offset_fetch_response.topics
                return [True, consumer_group_offsets]
            else:
                logger.error('获取主题 %s 对应消费组 %s latest offset 失败：主题%s不存在' % (topic_name, consumer_group, topic_name))
                return [False, '主题%s不存在' % topic_name]
        except Exception as e:
            logger.error( '获取主题 %s 对应消费组 %s latest offset 失败：%s' % (topic_name, consumer_group, e))
            return [False, '%s' % e]

    def get_topic_partitions_dict(self):
        return self.topic_partitions_dict



