'''
The aim is to store in Redis the final
dataframe containing the computed risks for
each city. The df will be stored in form of
document. It is possible to query the document
for all cities or one city.
'''
from meteo import meteo_connector
from meteo_spark import Spark_session
import json
import redis
from redis.commands.json.path import Path

# stuff to be imported from other scripts
# dictionary with data
filepath = '/home/giovanni/Scrivania/documentazione3b/loc_gb.csv'
my_connector = meteo_connector(filepath)
diz = my_connector.info_dict()

# Spark dataframe
session = Spark_session()
session.start_session()
rows = session.from_dict_to_rows(diz)
modified_rows = session.modify_rows(rows) #tuples!
df = session.final_df(modified_rows) #dataframe to be converted to json

def df_json(my_df):
    '''
    converts the Spark dataframe
    given as input into a JSON file
    '''
    res = {}
    json_str =  my_df.toJSON().collect() #list of JSON strings
    for my_json in json_str:
        json_dict = json.loads(my_json)
        res[json_dict['city']] = json_dict
    return res

# Store the resulting JSON into Redis and retrieve risk computations first for all cities, then for the desired city
data = df_json(df)

r = redis.Redis()
r.json().set('doc', '$', data) #putting data into redis

doc = r.json().get('doc', '$') #instantiating a variable with risk data for all cities
print(doc) #retrieving risk data for all cities

aberdeen = r.json().get('doc', '$.Aberdeen') #instantiating a variable with risk data for one city
print(aberdeen) #retrieving risk data for one city
