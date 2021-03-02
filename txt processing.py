
import pandas as pd
import csv
import os
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
    txt = txt.replace(',\n',',')              .replace(', ',',')              .replace(', ',',')              .replace('\n ','')
    
    #find and remove whitespace characters between non-decimals
    non_decimal = re.compile('[a-zA-Z]\s[a-zA-Z]')
    txt = non_decimal.sub('-', txt)
    
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
    with open(path_to_txt_file.split('/')[-1]) as fin,         open(csv_filename, 'w') as fout:
        
        o=csv.writer(fout)
        for line in fin:
            o.writerow(line.split())
    
    #the text processing ruins the columns names and so we'll add them back in
    data = pd.read_csv(csv_filename)
    #percent = data.With
    data = data.iloc[1:len(data)-1]
    data = data.dropna(axis=1)
    
    #data['Monthly Coverage Percent'] = percent
    print(data)
    data.columns = ['CSA','CBSA','Name','Total','1 Unit','2 Units','3 & 4 Units',
                     '5 or more','Number of structures with 5 units or more',
                     'Monthly Coverage Percent Percent']
    
    data.to_csv(csv_filename, index=False)

if __name__ == 'main':
    download_permits_data('2014','01')
    preprocess_txt('2014-01.txt')
    txt_to_csv('2014-01_processed.txt')



