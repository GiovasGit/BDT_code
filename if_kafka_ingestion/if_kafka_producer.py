'''
This consumer was meant to be used in spark_data_model to
obtain events from kafka, which would be then passed straight to Spark.
In this script a class to produce a kafka topic and send the data to kafka is created.
Inside this topic, partitions are made to be equivalent to the number of PAOI we have.
Finally the dictionary of the meteo.py script is sent through this class to kafka
'''
from confluent_kafka import Producer  # check your broker kafka version. It must be >= v0.8 to use confluent kafka
import json
from datetime import datetime
from BDT_code.connectors.weather import meteo_connector, filepath

class KafkaWeatherProducer:
    def __init__(self, bootstrap_servers):
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers
        })

    def send_weather_data(self, topic, key, weather_data):
        '''
        Args:
            topic:
            key:
            weather_data:

        Returns:
            nothing!
        '''
        try:
            self.producer.produce(topic, key=key.encode('utf-8'), value=json.dumps(weather_data).encode('utf-8'))
            self.producer.flush()
            print(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} (INFO) Data sent successfully to topic '{topic}' with key '{key}'")

        except Exception as e:
            print(f"Error sending data to topic '{topic}' with key '{key}': {e}")


kafka_bootstrap_servers = 'localhost:9092'  # replace based on the local host number you have (check it in the config folder, inside it there is a file called server or server properties.
# You should see the local host number inside that file
kafka_producer = KafkaWeatherProducer(
    bootstrap_servers=kafka_bootstrap_servers)  # creation of the producer with that local host

topic = 'weather_data_topic'
mc = meteo_connector(filepath)
for city_name, city_data in mc.info_dict().items():
    for key in ['min_temp', 'max_temp', 'radiations', 'wind_kmh',
                'prec', 'hr', 'altitude', 'latitude', 'longitude']:
        kafka_producer.send_weather_data(topic, key, city_data[key])  # send the data through the send_weather_data

# No need to explicitly close the producer since flush() does it.






