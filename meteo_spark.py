'''
Computations in Spark. Putting the data
coming from meteo.py script in a df.
Hoping to then modify it creating the
final df to send to Redis in another script
'''
from pyspark.sql import SparkSession
from pyspark.sql import Row
from meteo import meteo_connector

#from pyspark.sql.types import StructType, StructField, StringType

filepath = '/home/giovanni/Scrivania/documentazione3b/loc_gb.csv'
my_connector = meteo_connector(filepath)
diz = my_connector.info_dict()

class Spark_session:
    def start_session(self):
        '''
        starts Spark session
        '''
        spark = SparkSession.builder.getOrCreate()
        return spark
    
    def df_from_dict(self, my_dict):
        '''
        creates dataframe
        from dictionary
        and returns it
        '''
        # Convert the dictionary to a list of Row objects
        rows = [Row(city=key, **values) for key, values in my_dict.items()]
        # Step 4: Convert the RDD to a DataFrame
        the_df = Spark_session().start_session().createDataFrame(rows)

        return the_df
    
    def computations(self): #maybe more than one method will be needed
        pass
    
    def stop_session(self):
        SparkSession.stop()

session = Spark_session()
#start session
session.start_session()
#create and show the DataFrame
df = session.df_from_dict(diz)
df.show()
#stop session
#session.stop_session()
