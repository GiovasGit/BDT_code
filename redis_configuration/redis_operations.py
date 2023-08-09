
from rejson import Path, Client
import json
from BDT_code.redis_configuration.data_conversion import DataPreparation
from BDT_code.connectors.weather import filepath

class RedIngestion:

    def __init__(self, host="localhost", port=6379):
        self.rj = Client(host=host, port=port, decode_responses=True)

    def store_data(self, data: object) -> object:
        '''
        Args:
            data in a json or bit-like object
        '''
        self.rj.jsonset('doc', Path.rootPath(), data)
        print("INFO: Data was successfully stored via Redis.")

    def retrieve_location_data(self, city_name):
        '''
        Args:
            city_name:

        Returns: json-like dictionary.
        '''
        # TO IMPROVE
        query = city_name.lower().strip()
        result = self.rj.jsonget('doc', f'.{query}')

        if result:
            return result
        else:
            return None
    def retrieve_all_location_data(self):
        result = self.rj.jsonget('doc', "$")
        if result:
            return result
        else:
            return None

#data_prep = DataPreparation(filepath)
#red_ingestion = RedIngestion()
#data_prep.prepare_data()
#data_json = data_prep.df_to_dict()
#print(type(data_json))
#red_ingestion.store_data(data_json)
#print(red_ingestion.retrieve_location_data('London'))

