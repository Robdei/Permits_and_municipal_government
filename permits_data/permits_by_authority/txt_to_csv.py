import pandas as pd
import numpy as np
from tqdm import tqdm

ret_df = pd.DataFrame(columns = ['Permit Authority', 'County', 'Month', 'Year', 'Permit Type', '# of Permits'])


for year in range(2015,2020):
	print(year)
	file = open(f'CA {year} Permits.txt','r')

	text_data = [x.split('\t') for x in file.read().split('\n') if x!='Note: i = Imputed Value  ']

	list_of_permit_authorities = []
	for n,line in enumerate(text_data):
		if n%26==0:
			list_of_permit_authorities.append(line[0] + f'|{text_data[n+1][0]}')

	single_family = []
	for n,line in enumerate(text_data):
		if (n-19)%26==0:
			single_family.append(line)

	multi_family = []
	for n,line in enumerate(text_data):
		if (n-20)%26==0:
			multi_family.append(line)

	two_unit = []
	for n,line in enumerate(text_data):
		if (n-21)%26==0:
			two_unit.append(line)

	three_unit = []
	for n,line in enumerate(text_data):
		if (n-22)%26==0:
			three_unit.append(line)

	five_unit = []
	for n,line in enumerate(text_data):
		if (n-23)%26==0:
			five_unit.append(line)


	dict_of_permit_authorities = {xs:{} for xs in list_of_permit_authorities}

	months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Total']
	for n,x in enumerate(single_family):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month:x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n,x in enumerate(multi_family):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month:x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n,x in enumerate(two_unit):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month:x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n,x in enumerate(three_unit):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month:x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months

	for n,x in enumerate(five_unit):
		key = list(dict_of_permit_authorities.keys())[n]

		permit_months = {month:x[1:][n] for n,month in enumerate(months)}
		dict_of_permit_authorities[key][x[0]] = permit_months


	permit_types = ['Units in Single-Family Structures',
					'Units in All Multi-Family Structures',
					'Units in 2-unit Multi-Family Structures',
					'Units in 3- and 4-unit Multi-Family Structures',
					'Units in 5+ Unit Multi-Family Structures',
				   ]

	for authority in tqdm(dict_of_permit_authorities):
		permit_authority = authority.split('|')[0]
		county = authority.split('|')[1]
		for permit_type in permit_types:
			concat_df = pd.DataFrame(index = range(13), columns = ['Permit Authority', 'County', 'Month', 'Year', 'Permit Type', '# of Permits'])

			for n,month in enumerate(months):
				permits = dict_of_permit_authorities[authority][permit_type][month]
				concat_df.loc[n] = [permit_authority, county, month, year, permit_type, permits]

			ret_df = pd.concat([ret_df, concat_df])

	ret_df.reset_index(drop=True, inplace=True)

ret_df['notes'] = [np.nan if 'i' not in x else 'imputed permit count' for x in ret_df['# of Permits']]
ret_df['# of Permits'] = [x if 'i' not in x else x[:-1] for x in ret_df['# of Permits']]
ret_df.to_csv(f'permits_by_permitting_authority.csv',index=False)