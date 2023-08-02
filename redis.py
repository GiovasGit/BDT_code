'''
The aim is to store in Redis the final
dataframe containing the computed risks for
each city. The df will be stored in form of
document. It is possible both store and
query the document through Redis OM.
Cooler thing would be to be able to fetch
from redis single rows for single city risk.
Maybe different implementation to do that
'''
from meteo import meteo_connector
from meteo_spark import Spark_session

# stuff to be imported from other scripts
# dictionary with data
filepath = '/home/giovanni/Scrivania/documentazione3b/loc_gb.csv' #to put in  'requirements'(?)
my_connector = meteo_connector(filepath)
diz = my_connector.info_dict()

# Spark dataframe
session = Spark_session()
session.start_session()
rows = session.from_dict_to_rows(diz)
modified_rows = session.modify_rows(rows) #tuples!
df = session.final_df(modified_rows) #dataframe to be converted to json
#df.show()

