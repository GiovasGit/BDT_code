'''
This consumer was meant to be used in spark_data_model to
obtain events from kafka, which would be then passed straight to Spark.
'''

class KafkaWeatherConsumer:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.kafka_bookstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic


