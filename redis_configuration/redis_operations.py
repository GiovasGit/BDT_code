from rejson import Path, Client
from BDT_code.redis_configuration.data_conversion import DataPreparation
data = DataPreparation.df_to_dict()

class RedIngestion:

    def __init__(self, host="localhost", port=6379):
        self.rj = Client(host=host, port=port, decode_responses=True)

    def store_data(self, data):
        self.rj.jsonset('doc', Path.rootPath(), data)
        print("Data was successfully stored via Redis.")

    def retrieve_city_data(self, city_name):
        result = self.rj.jsonget('doc', f".$.{city_name.capitalize().strip()}")
        if result:
            return json.loads(result)
        else:
            return None

