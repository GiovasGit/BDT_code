'''
The aim is to store in Redis the final
dataframe containing the computed risks for
each city. The df will be stored in form of
document. It is possible to query the document
for all cities or one city.
'''

# TO DO: set datetime info and warnings about every long process involving platforms.
import json
from BDT_code.redis_configuration.redis_operations import RedIngestion
from BDT_code.redis_configuration.data_conversion import DataPreparation
from BDT_code.connectors.weather import filepath

#Putting data in a format that Redis will use without freaking out:
data_prep = DataPreparation(filepath)
data_prep.prepare_data()

#Storing the data into Redis
red_ingestion = RedIngestion()
data_json = data_prep.df_to_dict()
red_ingestion.store_data(data_json)

#Starting location lookup.
city_demo = "To obtain city-specific results, please insert one of the following locations" + \
             " or find out from the official documentation which cities we have but we did not list here." + \
             "Birmingham, Cambridge, Cardiff, London"
print(city_demo)
user_input = input("Enter a city: ")
print(f"You entered {user_input}.")
result = red_ingestion.retrieve_city_data(user_input)
if result:
    print(f"Data for {user_input.capitalize()}:\n", result)
else:
    print(f"No data found for {user_input.capitalize()}.")




