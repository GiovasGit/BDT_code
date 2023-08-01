'''
Computations in Spark. Two ways:
- import in spark the data coming from meteo.py script
- redo the whole process did in the meteo.py script
  and then do the calculations
What's the right one?
'''
from pyspark.sql import SparkSession
from pyspark.sql import Row
from meteo import meteo_connector

#from pyspark.sql.types import StructType, StructField, StringType

filepath = '/home/giovanni/Scrivania/documentazione3b/loc_gb.csv'

spark = SparkSession.builder.getOrCreate()
my_connector = meteo_connector(filepath)
diz = my_connector.info_dict()

# Convert the dictionary to a list of Row objects
rows = [Row(city=key, **values) for key, values in diz.items()]

# Step 4: Convert the RDD to a DataFrame
df = spark.createDataFrame(rows)

# Show the DataFrame
df.show()