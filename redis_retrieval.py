'''
The aim is to store in Redis the final
dataframe containing the computed risks for
each city. The df will be stored in form of
document. It is possible to query the document
for all cities or one city.
'''
import json
from BDT_code.connectors.weather import meteo_connector, filepath
from BDT_code.data_model.meteo_spark import Spark_session
from BDT_code.redis_configuration.redis_operations import RedisOperations


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

r_ops = RedisOperations()
r_ops.store_data(data)
doc = r_ops.retrieve_data()

