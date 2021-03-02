
import pandas as pd
import csv
import os, glob
import re


def download_permits_data(year, month):
    """
    Function that will download monthly permits data (in .txt format) from the census bureau
    
    Inputs -
    year - str
        the year of data you want to download (2014 - 2019)
    month - str
        the month of data you want to download (01, 02, ..., 12)
        
    Output - 
    downloads txt file of data for the specified month and year to the working directory
    """
    
    # month and year must be strings
    assert type(year)==str and type(month)==str,         "year and month parameters should be strings"
    
    #check that month and year are in the correct format
    url = year+month
    check = re.compile("^[2][0][1][4-9][0-1][0-9]$") 
    assert len(check.findall(url))==1,         """year is confined to the years '2014' to '2019' and month is confined to '01' to '12' \n 
        (remember to put a zero before the month if it is before October)"""
    
    #download data
    if f"{year}-{month}.txt" in os.listdir():
        os.remove(f"{year}-{month}.txt")
    os.system(f"curl https://www.census.gov/construction/bps/txt/tb3u{url}.txt >> {year}-{month}.txt")


def preprocess_txt(path_to_txt_file):
    """
    remove newline characters and spaces to allow txt files to be space delimmited
    
    Inputs -
    path to unprocessed txt file directly downloaded from the census
    
    Output -
    new txt representing the raw data without the superfluous newline and whitespace characters
    """
    f_in = open(path_to_txt_file,'r')
    txt = f_in.read()
    
    #remove newline characters after metro names
    txt = (txt
    	.replace(',\n',',')              
	    .replace(', ',',')              
	    .replace(', ',',')              
	    .replace('\n ','')
	    .replace('St. ','St.')
	    )
    
    #find and remove whitespace characters between non-decimals
    non_decimal = re.compile('[a-zA-Z]\s[a-zA-Z]')
    matches = [m.start()+1 for m in re.finditer(non_decimal, txt)]
    new_txt = ""
    for n,char in enumerate(txt):
    	if n not in matches:
    		new_txt+=char
    	else:
    		new_txt+="-"
    txt = new_txt
    # for match in matches:
    # 	txt[match+1]="-"
    #txt = non_decimal.sub('-', txt, count=1)
    
    output_filename = f"{path_to_txt_file[:-4]}_processed.txt"
    if output_filename in os.listdir():
        os.remove(output_filename)
        
    f_out = open(output_filename,'w')
    f_out.write(txt)
    
    f_out.close()
    f_in.close()


def txt_to_csv(path_to_txt_file):
    """
    create csv through space delimitted txt file
    
    Input -
    processed txt of monthly permit data
    Output -
    csv of monthly permit data
    """
    csv_filename = f"{path_to_txt_file.split('/')[-1][:-4]}.csv"
    with open(path_to_txt_file.split('/')[-1]) as fin, open(csv_filename, 'w') as fout:
        
        o=csv.writer(fout)
        for line in fin:
            o.writerow(line.split())
    
    #the text processing ruins the columns names and so we'll add them back in
    data = pd.read_csv(csv_filename)
    #percent = data.With
    data = data.iloc[1:len(data)-1]
    data = data.dropna(axis=1)
    
    #data['Monthly Coverage Percent'] = percent
    data.columns = ['CSA','CBSA','Name','Total','1 Unit','2 Units','3 & 4 Units',
                     '5 or more','Number of structures with 5 units or more',
                     'Monthly Coverage Percent']
    
    data.to_csv(csv_filename, index=False)

def add_month_year_data(csv_filename):
    """
    adds month and year data to the monthly permits data
    
    Input-
    csv: str
        filename of csv in working directory
    output-
    input csv with temporal data added in columns
    """
    #import data into pandas
    csv_pd = pd.read_csv(csv_filename)
    
    #parse temportal data from csv filename (assumes file name is of the format YYYY-MM_processed)
    month = (csv_filename
            .split('-')[-1]
            .split('_')[0]
            )
    year  = csv_filename.split('-')[0]
    
    csv_pd['month'] = month
    csv_pd['year']  = year
    
    csv_pd.to_csv(csv_filename, index=False)
    

def join_csvs():
	"""
	Concat all csvs together
	"""
	merged_permits =  pd.concat([pd.read_csv(csv) for csv in glob.glob('*.csv') 
								if csv != 'all_permits.csv' and csv != 'cbsa-population.csv'])

	for csv in glob.glob('*.csv'):
		if csv != 'cbsa-population.csv':
			os.remove(csv)

	merged_permits.to_csv('all_permits.csv', index=False)


if __name__ == '__main__':
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

