'''
Computations in Spark. The original idea was to
Put in a dataframe the data coming from weather.py script,
then modify this dataframe in Spark putting 1 into the
cells containing risk values higher than their threshold,
0 otherwise. But I was not able to modify the Spark df
so I performed the changes on Spark Row objects. The
point is that I believe that this is like using Python
as the output of the Rows modifications are simple tuples.
A way to perform the original idea should be implemented
'''

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
#   class earthquake_spark:
#       def bla bla()

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, FloatType
from datetime import datetime
from BDT_code.connectors.weather import meteo_connector, filepath
from BDT_code.connectors.earthquake import earthquake_connector
# from pyspark.sql.types import StructType, StructField, StringType


class Spark_session:

    def __init__(self):
        self.spark = None
        self.start_session()

    def start_session(self):
        '''
        initalize Spark session
        '''
        self.spark = SparkSession.builder.getOrCreate()
        return self.spark

    def from_dict_to_rows(self, my_dict):
        '''
        turns a dictionary to Spark Rows
        '''
        ''' 
        in Spark:
        [ Row(city, min_temp_modified, max_temp_modified, radiations_modified, wind_kmh_modified),
          Row(...)           
            ]
        '''

        the_rows = []
        for key, values in my_dict.items():
            the_rows.append(Row(city=key, **values))
        return the_rows

    def modify_rows(self, my_rows) -> list:
        '''
        Dichotomizing values related to it being a risk or not.
        It returns a list with modified
        rows as list of tuples.
        '''
        res = []
        for row in my_rows:
            # Extract values from the row
            city = row['city']
            min_temp = row['min_temp']
            max_temp = row['max_temp']
            radiations = row['radiations']
            wind_kmh = row['wind_kmh']
            prec = row['prec']
            hr = row['hr']
            altitude = row['altitude']
            latitude = row['latitude']
            longitude = row['longitude']

            # TO ADD HUMIDITY, PRECIPITATION - rain_mm, hr

            # Modify values
            min_temp_modified = True if min_temp <= 0 else False
            max_temp_modified = True if max_temp >= 35 else False
            radiations_modified = True if radiations in ('high', 'very high') else False
            wind_kmh_modified = True if wind_kmh >= 50 else False
            prec_modified = True if prec >= 4.0 else False
            hr_modified = True if hr >= 70 else False
            # TO ADD HUMIDITY, PRECIPITATION

            # adding as a row into res
            res.append((city, min_temp_modified, max_temp_modified, radiations_modified,
                        wind_kmh_modified, prec, hr, altitude, latitude, longitude))
        return res

    def final_df(self, my_modified_rows):
        schema = StructType([StructField('city', StringType(), True),
                            StructField('min_temp', BooleanType(), True),
                            StructField('max_temp', BooleanType(), True),
                            StructField('radiations', BooleanType(), True),
                            StructField('wind_kmh', BooleanType(), True),
                            StructField("prec", FloatType(), True),
                            StructField("hr", FloatType(), True),
                            StructField('altitude', IntegerType(), True),
                            StructField('latitude', FloatType(), True),
                            StructField('longitude', FloatType(), True)])
        the_df = Spark_session().start_session().createDataFrame(my_modified_rows, schema)
        return the_df

    def quietate(self):
        self.spark.stop()

session = Spark_session()
session.start_session()

