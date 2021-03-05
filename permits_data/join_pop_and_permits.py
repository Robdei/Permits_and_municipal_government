import pandas as pd
import os, glob
import re
from fuzzywuzzy import fuzz 
from tqdm import tqdm
from fuzzywuzzy import process 
import numpy as np
from collections import Counter

def hyphens_to_spaces(csv_filename):
	"""
	replaces the hyphens in metropolitan name column with 
	spaces to ensure consistency in the permits and population 
	dataframes
	"""
	csv = pd.read_csv(csv_filename)

	assert 'Name' in csv.columns, \
		"no column called 'Name' found in columns (this represents the name of metro areas)"

	#turn hyphens into spaces
	csv.Name = csv.Name.apply(lambda x: x.replace('-',' '))
	#space after commas for state location
	csv.Name = csv.Name.apply(lambda x: x.replace(',',', '))
	#write changes to file
	csv.to_csv(csv_filename, index=False)


def join_on_exact_match(permits_filename='all_permits.csv', population_filename='cbsa-population.csv'):
	"""
	Will join metro-level population and building permit census data on the exact metro name.
	This will generate some null values in the population_estimate columns since the metro naming 
	conventions between the datasets are slightly different.
	"""
	population = pd.read_csv(population_filename, encoding="ISO-8859-1")

	#filter down dataset
	population_est = (population[population.LSAD=='Metropolitan Statistical Area']
					[['NAME']+[col for col in population.columns if 'POPESTIMATE' in col]] # or 'NETMIG' in col]]
				 )

	#rename columns to years
	population_est.rename(columns={col:col[-4:] for col in population_est.columns},
					  inplace=True
					 )

	#unpivot population table
	population_est = (population_est
					  .set_index('NAME')
					  .unstack()
					  .reset_index(name='population_est')
					  .rename(columns={'level_0':'year', 'NAME':'Name'})
					 )

	#similarize naming conventions between datasets (not perfect)
	population_est.year = population_est.year.astype(int)
	population_est.population_est = population_est.population_est.astype(int)
	population_est.Name = population_est.Name.apply(lambda x: x.replace('-',' '))
	population_est.Name = population_est.Name.apply(lambda x: x.replace('St. ','St.'))

	#Import
	#load permits data and join tables
	permits = pd.read_csv(permits_filename)
	permits.year = permits.year.astype(int)
	permits_population = permits.merge(population_est, on=['Name','year'], how='left')
	print(f"There are {len(permits_population[permits_population.population_est.isna()])} null values in this joined dataset")

	permits_population.to_csv('Population_and_Permits_by_metro_(Exact match).csv',index=False)


def join_on_fuzzy_match(permits_filename='Population_and_Permits_by_metro_(Exact match).csv', 
						population_filename='cbsa-population.csv'):
	"""
	Join_on_exact_match() will produce a number of null values due to different naming conventions between
	the permit and population tables. This function will do a fuzzy string match between metro names in
	both tables to obtain more population-permit table matches on metro names.
	"""


	#import exact matches and search for metro names that were not matched to to 
	# population estimate in the population table
	permits_population = pd.read_csv(permits_filename)
	population = pd.read_csv(population_filename, encoding="ISO-8859-1")

	missing_names = permits_population[permits_population.population_est.isna()].Name.tolist()

	matched_names = permits_population[~permits_population.population_est.isna()].Name.tolist()

	permits_population['Match on'] = 'Exact'

	#fuzzy string match the metro names with no population estimate to metro naems in the population table
	dict_of_matches = {}
	for missing_name in tqdm(list(set(missing_names))):
		
		fuzzy = []
		missing_state = missing_name.split(',')[-1]
		
		#We only have to look through names that haven't already been matched in the exact 
		# match table (hence the set difference operator)
		for name in list(set(population.NAME) - set(matched_names)):
			if missing_state == name.split(',')[-1]:
				fuzzy.append((name,fuzz.ratio(missing_name,name)))

				#grab the most likely match
				fuzzy.sort(key = lambda x: x[1])
				dict_of_matches[missing_name] = (fuzzy[-1][0],fuzzy[-1][1])
	# append columns "fuzzy match name" and the type of string matching done for 
	# joining the permit and population table
	permits_population['fuzzy match name'] = permits_population.Name.apply(lambda x: dict_of_matches[x][0] if x in dict_of_matches else np.nan)

	# By hand, I verified that fuzzy string matching does not connect the city names in the permits table with the 
	# correct name in the population table for the following metro areas
	Failed_matches = ['Visalia Porterville, CA', 'Vallejo Fairfield, CA', 'Stockton Lodi, CA', 'Prescott, AZ', \
					  'Niles Benton Harbor, MI','Myrtle Beach Conway North Myrtle Beach, SC', \
					 'Mankato North Mankato, MN','Macon, GA','Grand Pass, OR','Dayton, OH','Bend Redmond, OR']

	for row in range(len(permits_population)):
		if type(permits_population['fuzzy match name'][row]) == str:
			if permits_population['Name'][row] in Failed_matches:
				permits_population['Match on'][row] = 'None'
			else:
				permits_population['Match on'][row] = 'Fuzzy'

	#filter down dataset
	population = (population[population.LSAD=='Metropolitan Statistical Area']
					[['NAME']+[col for col in population.columns if 'POPESTIMATE' in col]] # or 'NETMIG' in col]]
				 )

	#rename columns to years
	population.rename(columns={col:col[-4:] for col in population.columns},
					  inplace=True
					 )

	#unpivot population table
	population = (population
				  .set_index('NAME')
				  .unstack()
				  .reset_index(name='population_est')
				  .rename(columns={'level_0':'year', 'NAME':'Name'})
				 )

	#similarize naming conventions between datasets (not perfect)
	population.year = population.year.astype(int)
	population.population_est = population.population_est.astype(int)

	permits_population_fuzzy = permits_population[permits_population['Match on'] == 'Fuzzy']
	permits_population_None = permits_population[permits_population['Match on'] == 'None']
	permits_population = permits_population[permits_population['Match on'] == 'Exact']


	print(f"There are {len(permits_population_fuzzy)} fuzzy values in this joined dataset")
	print(f"There are {len(permits_population_None)} None values in this joined dataset")

	permits_population_fuzzy = permits_population_fuzzy \
								.merge(population.rename(columns={'Name':'fuzzy match name'}), on=['fuzzy match name', 'year']) \
								.drop('population_est_x',axis=1) \
								.rename(columns = {'population_est_y':'population_est'})

	permits_population = (pd.concat([permits_population,
									permits_population_None, 
									permits_population_fuzzy])
							.reset_index(drop=True)
							)

	return permits_population


# if __name__ == '__main__':
# 	hyphens_to_spaces('all_permits.csv')
# 	join_on_exact_match()
# 	join_on_fuzzy_match().to_csv('Population_and_Permits_by_metro_(Exact+Fuzzy_match).csv',index=False)