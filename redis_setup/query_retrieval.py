'''
The aim is to store in Redis the final
dataframe containing the computed risks for
each city. The df will be stored in form of
document. It is possible to query the document
for all cities or one city.
'''
import redis.exceptions

from BDT_code.redis_configuration.redis_operations import RedIngestion
from BDT_code.redis_configuration.data_conversion import DataPreparation
from BDT_code.connectors.weather import filepath
from datetime import datetime
'''
#Putting data in a format that Redis will use without freaking out:


#Storing the data into Redis

'''
#Starting location lookup.
class Lookup:
    def __init__(self):

        self.data_prep = DataPreparation(filepath)
        self.data_prep.prepare_data()

        self.red_ingestion = RedIngestion()
        data_json = self.data_prep.df_to_dict()
        self.red_ingestion.store_data(data_json)

        city_demo = "To obtain city-specific results, please insert one location." + \
             "Find out from the documentation which cities are available but not list below." + \
             "Examples: Birmingham, Cambridge, Cardiff, London"
        print(city_demo)
        self.lookup(self.insert_location())

    def insert_location(self):
        user_input = input("Enter a city: ")
        return user_input

    class NoDataFoundException(Exception):
        pass

    def lookup(self, loc):
        try:
            result = self.red_ingestion.retrieve_location_data(loc)
            if result:
                list_of_risks = []
                for key, value in result.items():
                    if key != 'city' and value:  # this prints when value == 1 and key is not 'city' (not needed)
                        list_of_risks.append(human_readable[key])
                print(f"The risks for {result['city']} are: " + '; '.join(list_of_risks).capitalize())

        except redis.exceptions.ResponseError:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ERROR No data found for {loc.capitalize()}" + "\n :(")

#do not mind the following, it's to translate variable names into human readable names for risks
human_readable = {'min_temp': 'minimum temperature',
                  'max_temp': 'maximum temperature',
                  'radiations': 'solar radiation',
                  'wind_kmh':'wind gusts above 50 km/h'}

Lookup()
