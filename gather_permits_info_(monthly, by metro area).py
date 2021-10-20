import permits_data.txt_processing as txt_funcs
import permits_data.join_pop_and_permits as join_funcs
import os, glob
import pandas as pd

months = ['01','02','03','04','05','06','07','08','09','10','11','12']
years = ['2014','2015','2016','2017','2018','2019']
year_months = [[year+'-'+month for month in months] for year in years]
retval = []
for x in year_months:
	retval+=x
print(retval[:-2])
for date in retval[:-2]:
	try:
		year = date.split('-')[0]
		month = date.split('-')[1]
		txt_funcs.download_permits_data(year,month)
		print(f'downloaded {year}/{month}')
		txt_funcs.preprocess_txt(f'{year}-{month}.txt')
		txt_funcs.txt_to_csv(f'{year}-{month}_processed.txt')
		txt_funcs.add_month_year_data(f'{year}-{month}_processed.csv')

	except ValueError:
		continue

#remove txt files since we have the csvs now 
for txt in glob.glob('*.txt'):
	os.remove(txt)
for csv in glob.glob('*.csv'):
	if csv != 'cbsa-population.csv' and len(pd.read_csv(csv).columns) != 12:
		os.remove(csv)

txt_funcs.join_csvs('permits_data/all_permits.csv')

join_funcs.hyphens_to_spaces('permits_data/all_permits.csv')

join_funcs.join_on_exact_match(permits_filename='permits_data/all_permits.csv',
								population_filename='permits_data/cbsa-population.csv')

join_funcs.join_on_fuzzy_match(permits_filename='Population_and_Permits_by_metro_(Exact match).csv',
								population_filename='permits_data/cbsa-population.csv')\
		  .to_csv('Population_and_Permits_by_metro_(Exact+Fuzzy_match).csv',index=False)

os.remove('Population_and_Permits_by_metro_(Exact match).csv')
