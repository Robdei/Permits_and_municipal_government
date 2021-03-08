import pandas as pd
import numpy as np
from tqdm import tqdm

# initialize an empy df to put our results into
ret_df = pd.DataFrame(columns = ['Permit Authority', 'County', 'Month', 'Year', 'Permit Type', '# of Permits'])
# path to the txt files with permit data
path_to_txts = "permits_data/permits_by_authority/"

for year in range(2014,2020):
	print(year)
	file = open(path_to_txts + f'CA {year} Permits.txt','r')

	# read the txt file and split strings by newline char
	text_data = [x.split('\t') for x in file.read().split('\n') if x!='Note: i = Imputed Value  ']

	# parse the permit data (number of permits by permit authority)
	list_of_permit_authorities = []
	for n, line in enumerate(text_data):
		if n % 26 == 0:
			list_of_permit_authorities.append(line[0] + f'|{text_data[n+1][0]}')

	single_family = []
	for n, line in enumerate(text_data):
		if (n-19) % 26 == 0:
			single_family.append(line)

	multi_family = []
	for n, line in enumerate(text_data):
		if (n-20) % 26 == 0:
			multi_family.append(line)

	two_unit = []
	for n, line in enumerate(text_data):
		if (n-21) % 26 == 0:
			two_unit.append(line)

	three_unit = []
	for n, line in enumerate(text_data):
		if (n-22) % 26 == 0:
			three_unit.append(line)

	five_unit = []
	for n, line in enumerate(text_data):
		if (n-23) % 26 == 0:
			five_unit.append(line)

	# create a dict that maps the permit authority to permit type (single family, etc.) that maps to month
	dict_of_permit_authorities = {xs: {} for xs in list_of_permit_authorities}

	months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Total']
	for n, x in enumerate(single_family):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month: x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n, x in enumerate(multi_family):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month: x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n, x in enumerate(two_unit):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month: x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n, x in enumerate(three_unit):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month: x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n, x in enumerate(five_unit):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month: x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months


	permit_types = ['Units in Single-Family Structures',
					'Units in All Multi-Family Structures',
					'Units in 2-unit Multi-Family Structures',
					'Units in 3- and 4-unit Multi-Family Structures',
					'Units in 5+ Unit Multi-Family Structures',
				   ]

	# put the dict information into a pandas dataframe and export to csv
	for authority in tqdm(dict_of_permit_authorities):
		permit_authority = authority.split('|')[0]
		county = authority.split('|')[1]
		for permit_type in permit_types:
			concat_df = pd.DataFrame(index=range(13), columns=['Permit Authority', 'County', 'Month', 'Year', 'Permit Type', '# of Permits'])

			for n,month in enumerate(months):
				permits = dict_of_permit_authorities[authority][permit_type][month]
				concat_df.loc[n] = [permit_authority, county, month, year, permit_type, permits]

			ret_df = pd.concat([ret_df, concat_df])

	ret_df.reset_index(drop=True, inplace=True)

# sometimes, the permit info is imputed. take note of this in the df
ret_df['notes'] = [np.nan if 'i' not in x else 'imputed permit count' for x in ret_df['# of Permits']]
ret_df['# of Permits'] = [x if 'i' not in x else x[:-1] for x in ret_df['# of Permits']]

# determine the gov organization of the permitting authority (charter, general law, or county)
permits_by_authority = ret_df

file = open(path_to_txts+'city_charters/California Charter Cities.txt','r')
charter_cities = file.read().split('\n')
file.close()

charter_cities_df = pd.DataFrame([(city.upper() + ', CA','Charter') for city in charter_cities],
								columns=["Permit Authority", "City gov type"]
								)
ret_df = (permits_by_authority
		  .merge(charter_cities_df, on='Permit Authority', how='left')
		  )

ret_df['City gov type'] = ret_df['City gov type'].fillna('General Law')

ret_df['City gov type'] = [form if 'UNINCORPORATED' not in authority else 'County' for form, authority
					  		 in zip(ret_df['City gov type'],ret_df['Permit Authority'])]

# determine organization of county
county_types = pd.read_excel('permits_data/permits_by_authority/city_charters/county government type (wikipedia).xlsx') \
                .dropna() \
                .rename(columns={'county':'County', 'gov type':'County gov type', 'population':'County population'}) \
                .reset_index(drop=True) \
                [['County', 'County gov type', 'County population']]

for row in range(len(county_types)):
    if county_types.County[row]=='San Francisco':
        county_types.County[row]='San Francisco County'

permits_by_authority = ret_df.merge(county_types, on='County', how='left')


permits_by_authority.to_csv(f'permits_by_permitting_authority.csv',index=False)