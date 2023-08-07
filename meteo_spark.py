'''
Computations in Spark. The original idea was to
Put in a dataframe the data coming from meteo.py script,
then modify this dataframe in Spark putting 1 into the
cells containing risk values higher than their threshold,
0 otherwise. But I was not able to modify the Spark df
so I performed the changes on Spark Row objects. The
point is that I believe that this is like using Python
as the output of the Rows modifications are simple tuples.
A way to perform the original idea should be implemented
'''
from pyspark.sql import SparkSession
from pyspark.sql import Row
from meteo import meteo_connector

class Spark_session:
    def start_session(self):
        '''
        starts Spark session
        '''
        spark = SparkSession.builder.getOrCreate()
        return spark

    def from_dict_to_rows(self, my_dict):
        '''
        from dictionary to Spark
        Rows (check the Row data
        structure/object in the docs)
        '''
        # Convert the dictionary to a list of Row objects
        the_rows = [Row(city=key, **values) for key, values in my_dict.items()]
        return the_rows

    def modify_rows(self, my_rows):
        '''
        function to modify all rows.
        Returns a list with the
        modified rows (list of tuples)
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
