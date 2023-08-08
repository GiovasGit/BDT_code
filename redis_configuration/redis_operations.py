import json, os
import redis
from BDT_code.connectors.weather import meteo_connector, filepath
from BDT_code.redis_retrieval import df_json
class RedisOperations:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)

    def store_data(self, data):
        json_str = [json.dumps(value) for value in data.values()]
        for city, json_value in data.items():
            self.r.set(city, json_value)
    def retrieve_data(self):
        keys = self.r.keys()
        data = {}
        for key in keys:
            json_value = self.r.get(key).decode('utf-8')
            data[key.decode('utf-8')] = json_value
        return data

if __name__ == "__main__":

    my_connector = meteo_connector(filepath)
    data = df_json(my_connector.info_dict())

    r_ops = RedisOperations()
    r_ops.store_data()
    doc = r_ops.retrieve_data()

    print(doc)
