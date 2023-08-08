import json
from rejson import Path, Client


class RedIngestion:

    def __init__(self, host="localhost", port=6379):
        self.rj = Client(host=host, port=port, decode_responses=True)

    def store_data(self, data: object) -> object:
        '''
        Args:
            data in a json or bit-like object
        '''
        self.rj.jsonset('doc', Path.rootPath(), data)
        print("Data was successfully stored via Redis.")

    def retrieve_city_data(self, city_name):
        '''
        Args:
            city_name:

        Returns: json-like dictionary.
        '''
        result = self.rj.jsonget('doc', f".$.{city_name.capitalize().strip()}")
        if result:
            return json.loads(result)
        else:
            return None

RedIngestion()
