import requests
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup
'''
All raw data is stored in an xml tree-like structure.
Inside it, the most useful element is "description" and it looks like:
  <description>
   Origin date/time: Tue, 30 May 2023 07:51:27 ; Location: MULL,ARGYLL AND BUTE ; Lat/long: 56.585,-6.219 ; Depth: 7 km ; Magnitude: 1.8
  </description>
'''
source = "https://quakes.bgs.ac.uk/feeds/MhSeismology.xml"
response = requests.get(source)
raw_data = dict()


if response.status_code == 200:  # 200 is desired availability
    # Parse the XML semi-structured content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'xml')
    descriptions = soup.findAll("description")
    print(f"{datetime.now()} (INFO): Connecting to UK Earthquakes API:")
    for item in tqdm(descriptions):
        item = str(item)
        if ";" not in item:
            continue
        item = item.strip("<description>").strip("</description>")
        elements = item.split(";")

        # cleaning elements
        for element in elements:
            if "Origin" in element:
                date = element[22:-10].strip()
                time = element[-9:].strip()
            elif "Lat/long" in element:
                lat_long = element.split(":")
                latitude = float(lat_long[1].split(",")[0].strip())
                longitude = float(lat_long[1].split(",")[1].strip())
                location = (latitude, longitude)
            elif "Depth" in element:
                depth = float(element.split(":")[-1].split(" ")[1])
            elif "Magnitude" in element:
                magnitude = float(element.split(":")[-1][:5].strip())
            elif "Location" in element:
                name = element.split(":")[-1].lower().strip()
            # the data is then inserted in the raw_data dictionary.
            # The key is a unique tuple (name, time) and value is the data for that event
        if (name, time) not in raw_data.keys():
            raw_data[(name, time)] = {"location": location, "name": name,
                                      "date": [date], "time": [time],
                                      "depth": [depth], "magnitude": magnitude}

else:
    print("Failed to fetch the XML content. Status code:", response.status_code)

print(raw_data)