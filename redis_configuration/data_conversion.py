import json

from BDT_code.connectors.weather import meteo_connector, filepath
from BDT_code.data_model.spark_setup import Spark_session

class DataPreparation:
    def __init__(self):
        self.connector_path = filepath
        self.diz = None
        self.df = None

    def prepare_data(self):
        self.fetch_ids()
        self.process_data()

    def fetch_ids(self):
        self.diz = meteo_connector(self.connector_path).info_dict()

    def process_data(self):
        session = Spark_session()
        session.start_session()
        rows = session.from_dict_to_rows(self.diz)

        modified_rows = session.modify_rows(rows)
        self.df = session.final_df(modified_rows)

    def df_to_dict(self):
        lookable_dictionary = {}
        json_str = self.df.toJSON().collect()
        for element in json_str:
                node = json.loads(element)
                lookable_dictionary[node['city'].lower()] = node
        return lookable_dictionary



#data_prep = DataPreparation(filepath)
#data_prep.prepare_data()
#print(data_prep.df_to_dict())
