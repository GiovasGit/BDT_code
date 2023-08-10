'''
Data collected from 3B Meteo API. Parameters of Interest (PAOI):
- wind 'strength' ('raffica'', danger threshold to be set),
- frost occurrency ('t_min', threshold: <= 0),
- heat wave ('t_max', threshol: >= 35)
- solar radiation ('uv', threshold: 'descrizione uv'),
- altitude, latitude and longitude to join with other data.
NOTE: Had to exclude altitude, latitude, longitude
because I could not handle them in a Spark dataframe
TypeError: field latitude: Can not merge type <class 'pyspark.sql.types.StringType'> and <class 'pyspark.sql.types.DoubleType'>
'''

import csv
import os
import requests
from datetime import datetime
from tqdm import tqdm


script_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
filepath = os.path.join(script_directory, "documentazione3b", "loc_gb.csv")

class meteo_connector:
    def __init__(self, mypath):
       self.mypath = mypath

    def ids_cities(self):
        '''
        returns a list
        of all cities ids
        '''
        res =  []
        with open(self.mypath) as f:
            my_reader = csv.reader(f, delimiter = ';')
            next(my_reader)
            for row in my_reader:
                res.append(row[0])
        return res

    def info_dict(self):
        '''
        Return dictionary with
        location names as:
        {keys: {id:PAOI}}
        '''
        res={}
        print(f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} (INFO) Connecting to UK 3bMeteo API:")
        for id in tqdm(self.ids_cities()):
            url = f'https://api.3bmeteo.com/publicv3/bollettino_meteo/previsioni_localita/{id}/1/en/daily/1?format=json2&X-API-KEY=0iMs6figaXNyc8JxnrMHQyqvYrSNh3WuoFvIZkXn'
            response = requests.get(url)
            if response.status_code == 200:
                city_dict = response.json()
                #preparing key of the final dict
                city_name = city_dict['localita']['localita'] #KEY of the FINAL dict
                #creating the dict to be used as VALUE of the FINAL dict
                value_dict = {}
                city_id = city_dict['localita']['id']
                city_alt = city_dict['localita']['altitudine']
                city_lat, city_lon = float(city_dict['localita']['lat']), float(city_dict['localita']['lon'])
                day_forecast = city_dict['localita']['previsione_giorno']
                dict_day_forecast = day_forecast[0]
                city_tmin = dict_day_forecast['tempo_medio']['t_min']
                city_tmax = dict_day_forecast['tempo_medio']['t_max']
                city_uv = dict_day_forecast['tempo_medio']['descrizione_uv']['en']
                city_wind = dict_day_forecast['tempo_medio']['raffica']
                city_prec = dict_day_forecast['tempo_medio']['precipitazioni']
                city_hum = dict_day_forecast['tempo_medio']['hr']

                value_dict['id'] = city_id
                value_dict['altitude'] = city_alt
                value_dict['latitude'] = city_lat
                value_dict['longitude'] = city_lon
                value_dict['min_temp'] = city_tmin
                value_dict['max_temp'] = city_tmax
                value_dict['radiations'] = city_uv
                value_dict['wind_kmh'] = city_wind
                value_dict['prec'] = float(city_prec)
                value_dict['hr'] = float(city_hum)

                res[city_name] = value_dict
        return res

#my_connector = meteo_connector(filepath)
#print(type(my_connector.info_dict()))
#print(my_connector.info_dict().items())
