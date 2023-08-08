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
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from BDT_code.connectors.weather import meteo_connector, filepath

# from pyspark.sql.types import StructType, StructField, StringType


my_connector = meteo_connector(filepath)
diz = my_connector.info_dict()

class Spark_session:

    def __init__(self):
        self.spark = None

    def start_session(self):
        '''
        initalize Spark session
        '''
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")  # Set log level to ERROR
        return self.spark

    def from_dict_to_rows(self, my_dict):
        '''
        from dictionary to Spark Rows,
        check the Row data's structure
        or object in the docs)
        '''
        the_rows = [Row(city=key, **values) for key, values in my_dict.items()]
        return the_rows

    def modify_rows(self, my_rows):
        '''
        function to modify all rows.
        Returns a list with modified
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

            # Modify values
            min_temp_modified = 1 if min_temp <= 0 else 0
            max_temp_modified = 1 if max_temp >= 35 else 0
            radiations_modified = 1 if radiations in ('high', 'very high') else 0
            wind_kmh_modified = 1 if wind_kmh >= 100 else 0

            res.append((city, min_temp_modified, max_temp_modified, radiations_modified, wind_kmh_modified))

        return res

    def final_df(self, my_modified_rows):
        schema = ('city', 'min_temp', 'max_temp', 'radiations', 'wind_kmh')
        the_df = Spark_session().start_session().createDataFrame(my_modified_rows, schema)
        return the_df

    def stop_session(self):
        spark_stop = SparkSession.stop()
        return spark_stop