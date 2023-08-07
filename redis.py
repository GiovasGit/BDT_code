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

def df_json(my_df):
    res = {}
    json_str =  my_df.toJSON().collect() #list of JSON strings
    for my_json in json_str:
        json_dict = json.loads(my_json)
        res[json_dict['city']] = json_dict
    return res

#print(df_json(df))

data = df_json(df)

#print(data)

r = redis.Redis(host='localhost', port=6379, db=0)
r.json().set('doc', '$', data) #putting data into redis
doc = r.json().get('doc', '$') #instantiating a variable with all data

print(doc) #retrieving all data
#aberdeen = r.json().get('doc', '$.Aberdeen')
