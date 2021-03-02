from txt_processing import *
from join_pop_and_permits import *

for month in ['01','02','03','04','05','06','07','08','09','10','11','12']:
	for year in range(2014,2020):
		try:
		    download_permits_data(str(year),month)
		    print(f'downloaded {year}/{month}')
		    preprocess_txt(f'{year}-{month}.txt')
		    txt_to_csv(f'{year}-{month}_processed.txt')
		    add_month_year_data(f'{year}-{month}_processed.csv')

		except ValueError:
			continue

#remove txt files since we have the csvs now 
for txt in glob.glob('*.txt'):
	os.remove(txt)
for csv in glob.glob('*.csv'):
	if csv != 'cbsa-population.csv' and len(pd.read_csv(csv).columns) != 12:
		os.remove(csv)

join_csvs()

hyphens_to_spaces('all_permits.csv')
join_on_exact_match()
join_on_fuzzy_match().to_csv('Population_and_Permits_by_metro_(Exact+Fuzzy_match).csv',index=False)