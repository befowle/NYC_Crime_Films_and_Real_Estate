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

#clean felony data
felony_df = pd.read_excel(r'felony_clean.xls')
felony_df['PCT'].fillna( method ='ffill', inplace = True)
felony_df = felony_df[felony_df['CRIME']=='TOTAL SEVEN MAJOR FELONY OFFENSES']

#clean other felony data
other_felony_df = pd.read_excel(r'other_felony.xls')
other_felony_df['PCT'].fillna( method ='ffill', inplace = True)
other_felony_df = other_felony_df[other_felony_df['CRIME']=='TOTAL NON-SEVEN MAJOR FELONY OFFENSES']

#clean misdemeanor data
misdemeanor_df = pd.read_excel(r'misdemeanor_pct.xls')
misdemeanor_df['PCT'].fillna( method = 'ffill', inplace = True)
misdemeanor_df = misdemeanor_df[misdemeanor_df['CRIME']=='TOTAL MISDEMEANOR OFFENSES']

#clean violation data
violation_df = pd.read_excel(r'violation_pct.xls')
violation_df['PCT'].fillna( method = 'ffill', inplace = True)
violation_df = violation_df[violation_df['CRIME']=='TOTAL VIOLATION OFFENSES']

#add mean rent data
rent_df = pd.read_csv("median_rent.csv")
rent_df = rent_df.dropna()
mean_rent_df = rent_df.filter(['areaName', 'Borough', 'areaType'])

#make column for each year
df2010 = rent_df.filter(['2010-01', '2010-02', '2010-03',
                        '2010-04', '2010-05', '2010-06', '2010-07', '2010-08', '2010-09', '2010-10',
                         '2010-11', '2010-12'])
#df2010.head(20)
series2010 = df2010.mean(axis = 1, skipna = True)

df2011 = rent_df.filter(['2011-01', '2011-02', '2011-03',
                        '2011-04', '2011-05', '2011-06', '2011-07', '2011-08', '2011-09', '2011-10',
                         '2011-11', '2011-12'])
#df2011.head(20)
series2011 = df2011.mean(axis = 1, skipna = True)

df2012 = rent_df.filter(['2012-01', '2012-02', '2012-03',
                        '2012-04', '2012-05', '2012-06', '2012-07', '2012-08', '2012-09', '2012-10',
                         '2012-11', '2012-12'])
#df2012.head(20)
series2012 = df2012.mean(axis = 1, skipna=True)

df2013 = rent_df.filter(['2013-01', '2013-02', '2013-03',
                        '2013-04', '2013-05', '2013-06', '2013-07', '2013-08', '2013-09', '2013-10',
                         '2013-11', '2013-12'])
series2013 = df2013.mean(axis = 1, skipna=True)

df2014 = rent_df.filter(['2014-01', '2014-02', '2014-03',
                        '2014-04', '2014-05', '2014-06', '2014-07', '2014-08', '2014-09', '2014-10',
                         '2014-11', '2014-12'])
series2014 = df2014.mean(axis = 1, skipna=True)

df2015 = rent_df.filter(['2015-01', '2015-02', '2015-03',
                        '2015-04', '2015-05', '2015-06', '2015-07', '2015-08', '2015-09', '2015-10',
                         '2015-11', '2015-12'])
series2015 = df2015.mean(axis = 1, skipna=True)

df2016 = rent_df.filter(['2016-01', '2016-02', '2016-03',
                        '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-10',
                         '2016-11', '2016-12'])
series2016 = df2016.mean(axis = 1, skipna=True)

df2017 = rent_df.filter(['2017-01', '2017-02', '2017-03',
                        '2017-04', '2017-05', '2017-06', '2017-07', '2017-08', '2017-09', '2017-10',
                         '2017-11', '2017-12'])
series2017 = df2017.mean(axis = 1, skipna=True)

df2018 = rent_df.filter(['2018-01', '2018-02', '2018-03',
                        '2018-04', '2018-05', '2018-06', '2018-07', '2018-08'])
series2018 = df2018.mean(axis = 1, skipna=True)

mean_rent_df['Year 2010'] = series2010
mean_rent_df['Year 2011'] = series2011
mean_rent_df['Year 2012'] = series2012
mean_rent_df['Year 2013'] = series2013
mean_rent_df['Year 2014'] = series2014
mean_rent_df['Year 2015'] = series2015
mean_rent_df['Year 2016'] = series2016
mean_rent_df['Year 2017'] = series2017
mean_rent_df['Year 2018'] = series2018

manhattandf = mean_rent_df[mean_rent_df['Borough'] == "Manhattan"]
manhattandf = manhattan_df[manhattan_df['areaType'] == 'neighborhood']
manhattan_rent_csv = manhattandf.to_csv(r'/Users/andrewtriola/Documents/flatiron/stats_project/manhattan_rent.csv', index=None, header=True)
