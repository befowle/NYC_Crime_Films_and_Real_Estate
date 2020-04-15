#Property Data: pull, clean
pip install sodapy

import pandas as pd
from sodapy import Socrata
import config #contains username, password, app token
import numpy as np

#pull from NYC Open data
client = Socrata("data.cityofnewyork.us", '7YJroGSBVCt6gzuLz6whih0yc')

# Example authenticated client (needed for non-public datasets):
client = Socrata('data.cityofnewyork.us',
                  config.app_token,
                  username=config.app_user,
                  password=config.app_pw)

# First 5000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
property_values_results = client.get("8vgb-zm6e", limit=50000)

# Convert to pandas DataFrame
property_values_results_df = pd.DataFrame.from_records(property_values_results)

#remove non_NYC properties ('borough data empty')
#replace '' with nan
property_values_results_df['borough'] = property_values_results_df['borough'].replace('',np.nan)

#drop nans
property_values_results_df = property_values_results_df.dropna(axis=0, subset=['borough'])

#confirm rows were deleted
property_values_results_df.shape

#reindex DataFrame
property_values_results_df = property_values_results_df.reset_index(drop=True)

#confirm reset_index
property_values_results_df

#save datafram to .csv
property_values_results_df.to_csv('property_values.csv')

#Film permit data pull
client = Socrata('data.cityofnewyork.us',
                  config.app_token,
                  username=config.app_user,
                  password=config.app_pw)
permit_results = client.get("tg4x-b46p", limit=50000)
permit_results_df = pd.DataFrame.from_records(permit_results)

#save to to_csv
results_df.to_csv('film_permits.csv')

#Pull film permit results
film_permit_results = client.get("tg4x-b46p", limit=50000)

# Convert to pandas DataFrame
film_permit_results_df = pd.DataFrame.from_records(film_permit_results)

#filter to Manhattan permits only
m_film_permit_results_df = film_permit_results_df[film_permit_results_df.values == 'Manhattan']

#save as csv
m_film_permit_results_df.to_csv('manhattan_film_permits')

#group by precinct
permits_per_pct = manhattan_film_permit_results_df.groupby('policeprecinct_s')

#count permits per precinct
num_permits_per_pct = permits_per_pct.count()
num_permits_per_pct
num_permits_per_pct.to_csv('num_permits_per_pct.csv')

#count permits per zipcode
permits_per_zipcode = manhattan_film_permit_results_df.groupby('zipcode_s')
permits_per_zip_df = permits_per_zipcode.count()
permits_per_zip_df.to_csv('permits_per_zip_df.csv')




#merge permits with property values df
permits_and_property_value = pd.merge(manhattan_film_permit_results_df, )
