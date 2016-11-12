__author__ = 'philipp'

from pykafka import KafkaClient
from scrapy.utils.serialize import ScrapyJSONEncoder
from scrapy import log

class KafkaPipeline(object):
    settings = None
    kafka = None
    topic = None
    encoder = None
    producer = None

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        ext.settings = crawler.settings

        kafkaHost = ""
        kafkaPort = ""
        kafkaTopic = ""

        if (ext.settings['KAFKA_HOST']):
            kafkaHost = ext.settings['KAFKA_HOST']

        if (ext.settings['KAFKA_PORT']):
            kafkaPort = ext.settings['KAFKA_PORT']

        if (ext.settings['KAFKA_TOPIC']):
            kafkaTopic = ext.settings['KAFKA_TOPIC']

        ext.kafka = KafkaClient(hosts=kafkaHost + ":" + str(kafkaPort))
        ext.topic = ext.kafka.topics[kafkaTopic]
        ext.producer = ext.topic.get_sync_producer(min_queued_messages=1)
        ext.encoder = ScrapyJSONEncoder()
        return ext

    def process_item(self, item, spider):
        item = dict(item)
        item['spider'] = spider.name
        msg = self.encoder.encode(item)
        self.producer.produce(msg)
        log.msg("Item sent to Kafka", log.DEBUG)
        return item