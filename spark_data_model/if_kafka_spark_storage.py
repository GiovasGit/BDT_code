'''
This had the same goals as spark_storage.py module, but it requires Kafka and
Spark to have an efficient connection which was not reached during the
coding of this project.
'''

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import expr, from_json, col, when
import json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, FloatType
from BDT_code.if_kafka_ingestion.if_kafka_consumer import KafkaWeatherConsumer
from BDT_code.connectors.weather import meteo_connector, filepath
from BDT_code.connectors.earthquake import earthquake_connector
# from pyspark.sql.types import StructType, StructField, StringType


class Sparkafkonsumer:

    def __init__(self):
        self.spark = self.start_spark_session()
        self.kafka_bootstrap_servers = 'localhost:9092'
        self.kafka_topic = 'weather_data_topic'
        self.kafka()

    def kafka(self):
        consumer = KafkaWeatherConsumer(self.kafka_bootstrap_servers,self.kafka_topic)
        return consumer
    def start_spark_session(self):
        '''
        Configures the Spark session to include the required package for integrating Spark with Kafka,
        with package spark-sql-kafka that allows Spark to read and write data from/to Kafka topics, then
        it either retrieves an existing Spark session or creates a new one if it doesn't exist.
        '''
        return SparkSession.builder\
            .appName("KafkaWeatherConsumer")\
            .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
            .getOrCreate()

    def read_from_kafka(self):
        '''
        This method reads continuously data from the specified Kafka topic
        '''
        return self.spark.readStream\
            .format('kafka')\
            .option('kafka.bootstrap.servers', self.kafka_bootstrap_servers)\
            .option("subscribe", self.kafka_topic)\
            .option('host', 'localhost')\
            .load()

    def process_data(self): #PREVIOUSLY from_dict_to_rows !!!
            '''
            Defines a schema and starts ordering data that is going to be fed to Spark
            '''
            schema = StructType([StructField('city', StringType(), True),
                                 StructField('min_temp', BooleanType(), True),
                                 StructField('max_temp', BooleanType(), True),
                                 StructField('radiations', BooleanType(), True),
                                 StructField('wind_kmh', BooleanType(), True),
                                 StructField("prec", FloatType(), True),
                                 StructField("hr", FloatType(), True),
                                 StructField('altitude', IntegerType(), True),
                                 StructField('latitude', FloatType(), True),
                                 StructField('longitude', FloatType(), True)
                                 ])
            kafka_data = self.read_from_kafka()

            # deserialize the Kafka event (must be in one type of json format)
            kafka_data = kafka_data.selectExpr("CAST(value AS STRING)")
            # parse JSON and apply schema
            parsed_data = kafka_data.selectExpr("from_json(value, {}) as data".format(schema.json())).select("data.*")
            #parsed_data = kafka_data.selectExpr(f"from_json(value, {json.dumps(schema.json())}) as data")\
            #    .select("data.*")

            # parsed_data = kafka_data.selectExpr("from_json(value, {}) as data".format(schema.json())).select("data.*")

            parsed_data.show()
            # apply transformations to the data (possibly this syntax could also work: parsed_data.methodvar1.methodvar2. ...)
            modified_data = parsed_data.withColumn("min_temp_modified", when(col("min_temp") <= 0, True).otherwise(False))
            modified_data = modified_data.withColumn("max_temp_modified", expr("IF(max_temp >= 35, true, false)"))
            modified_data = modified_data.withColumn("radiations_modified",expr("IF(radiations IN ('high', 'very high'), true, false)"))
            modified_data = modified_data.withColumn("wind_kmh_modified", expr("IF(wind_kmh >= 50, true, false)"))
            modified_data = modified_data.withColumn("prec_modified", expr("IF(prec >= 4, true, false)"))
            transformed_data = modified_data.withColumn("hr_modified", expr("IF(hr >= 70, true, false)"))

            output_data = modified_data.select(
                "city", "min_temp_modified", "max_temp_modified", "radiations_modified",
                "wind_kmh_modified", "prec_modified", "hr_modified"
            )
            query = transformed_data.writeStream.outputMode("append").format("console").start()
            return output_data

    def quietate(self):
        self.spark.stop()

session = Sparkafkonsumer()
session.start_spark_session()
print(session.process_data())


# TO DO: Connect earthquake data into this. IDEAS:
# Split the Spark_session class into different classes. One is for starting the Spark session and setting it up;
# then other classes perform data-specific operations and upload the data onto Spark to become a dataframe. It should
# require some kind of organization to distinguish between meteo and earthquake data i guess.

#TIP: i can make a parent (mother!) class that is called Spark_session and inside the classes for risk uploaders
#class Spark_session:
#   def __init__():
#       self.start_session()
#   class meteo_spark:
#        def bla bla()
#       def bla bla()
#   class earthquake_spark:
