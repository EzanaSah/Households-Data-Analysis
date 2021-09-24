# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 23:08:12 2021

@author: SahleEza
"""

# Useful Links:
   # https://realpython.com/python-web-scraping-practical-introduction/#your-first-web-scraper
   # https://realpython.com/beautiful-soup-web-scraper-python/
   # https://towardsdatascience.com/a-guide-to-scraping-html-tables-with-pandas-and-beautifulsoup-7fc24c331cf7
   # https://stackoverflow.com/questions/54952391/pandas-version-of-if-true-vlookup-here-if-false-vlookup-somewhere-else
   
# Import libraries

import pyodbc # ODBC = Open Database Connectivity, which is the standard API for accessing DBs
import pandas as pd # for data analysis
import numpy as np # for numerical processing
import os
from urllib.request import urlopen # for web scraping
from bs4 import BeautifulSoup as soup # for web scraping

### ---------- CANADIAN FORCES BASES -----------

# =============================================================================
# 
# ---- SCRAPE CFB (ATTEMPT) WITH BEAUTIFULSOUP -----
# 
# # Scrap Statistics Canada website below for CFB bases 
# 
CFB_url = "https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations.html"
# 
# CFB_site = urlopen(CFB_url)
# CFB_html = CFB_site.read()
# 
# # HTML Parsing with Beautiful Soup
# 
# CFB_soup = soup(CFB_html, "html.parser")
# 
# # Location of the individual rows in the table 
# 
# CFB_soup.body.main.div.table.tbody.tr.td
# 
# # Inspect the HTML that you are scraping using soup.find.All()
#     # Find the specfic HTML section you want - the table
#     # e.g. soup.findAll("div", {"class","item-container"})
#         # specify the attributes in a dictionary, if applicable
#         # Use jsbeautifier.org to view the HTML in proper format
#         
# CFB_table = CFB_soup.body.main.div.table
# 
# CFB_row = CFB_soup.body.main.div.table.tbody.tr.text.strip()
# 
# count = 0
# for row in CFB_table.tbody.tr:
#     # Check whether the row is Ontario 
#     # if 'Ontario' in row: # in CFB_soup.body.main.div.table.tbody.tr.text.strip():
#     count+=1
#     print(count)
#     print(row.text.strip())
#     # Go to the site within the second column
#     next_page = 'www.canada.ca'+CFB_soup.body.main.div.table.tbody.tr.a["href"]  
#     
# =============================================================================

### ------ SCRAPING CFB JUST USING PANDAS -------

# Read in CFB HTML Table (creates a list of dfs)

CFB_locations = pd.read_html(CFB_url)

# Count of the total number of bases in Canada/the table (should be 27)

canada_count = CFB_locations[0]['Province/Territory'].count()

# Count the number of bases in Ontario/the table (should be 6)

ontario_count = 0
for i in CFB_locations[0]['Province/Territory']:
    if i == 'Ontario':
        ontario_count += 1
print(ontario_count)
                           
# Read HTML tables for each of the Ontario base locations

borden = pd.read_html('https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations/borden.html')
kingston = pd.read_html('https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations/kingston.html')
northbay = pd.read_html('https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations/north-bay.html')
ottawa = pd.read_html('https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations/ottawa.html')
petawawa = pd.read_html('https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations/petawawa.html')
trenton = pd.read_html('https://www.canada.ca/en/department-national-defence/services/benefits-military/military-housing/locations/trenton.html')

# Sum the total number of units column for each locations webpage 

borden_cfb = borden[0]['Total number of units'].sum() # called Essa Tp below 
kingston_cfb = kingston[0]['Total number of units'].sum()
northbay_cfb = northbay[0]['Total number of units'].sum()
ottawa_cfb = ottawa[0]['Total number of units'].sum()
petawawa_cfb = petawawa[0]['Total number of units'].sum()
trenton_cfb = trenton[0]['Total number of units'].sum() # called Quinte West C below

# Create a DataFrame with CFB sums | key 'name' matches households dataset column name

cfb_table = pd.DataFrame({'name':['Ottawa C','Kingston C','Quinte West C','Petawawa T','Essa Tp','North Bay C'],
             'CFBHH':[ottawa_cfb,kingston_cfb,trenton_cfb,petawawa_cfb,borden_cfb,northbay_cfb]})


### ---------- HOUSEHOLDS FROM DATABASE + TIMESHARE -----------


# Specify Database Details

server = 'CACBIKDCDBMSQ05'
ompf_database = 'PT_Bkup'

# Specify the driver, server, and database (trusted connection uses MS Authentication)

cnxn_str = ('DRIVER={ODBC Driver 17 for SQL Server}; \
            SERVER=' + server + '; \
            DATABASE=' + ompf_database + '; \
            Trusted_Connection=yes;')

# Import Households (RU,RDU,FRU) and Timeshare Data from SQL into Pandas dataframes

cnxn = pyodbc.connect(cnxn_str)

households_sql = '''select a.munid_cur as Munid, a.munupid_cur as Munupid, a.tier_cur as tier, a.munname_cur as name,
       sum(case when b.unitclass = 'RU' then 1 else 0 end) RUHH,
       sum(case when b.unitclass = 'RDU' then 1 else 0 end) RDUHH,
       sum(case when b.unitclass = 'FRU' then 1 else 0 end) FRUHH
 From v_munsearch a inner join Totals2021 b on a.xmunid = case when ut = '1950' then '1950' else ctymn end
 Where a.taxyear = '2021' and a.typeid in ('st-own','lt-own','ut-lt')
 Group by a.munid_cur, a.munupid_cur, a.tier_cur, a.munname_cur, a.sort_municipal_cur
 Order by a.sort_municipal_cur'''

timeshares_sql = '''select ctymn,
count(distinct roll15) 'Timeshare Props',
count(roll15) 'roll15 count',
count(roll15) - count (distinct roll15) 'Discount'
from Totals2021
where propcode in ('385', '386') and unitclass in ('RU', 'RDU', 'FRU')
group by ctymn '''

households = pd.read_sql(households_sql,cnxn)
timeshares = pd.read_sql(timeshares_sql,cnxn)

cnxn.close()

# Change the Column Name of Timeshares to match Households 

timeshares.rename(columns = {'ctymn':'Munid',
                             'Timeshare Props':'Timeshare_Props',
                             'roll15 count':'Timeshare_HH'}, inplace=True)

# Left Join the two datasets on 'name' 

hhs_and_cfb = pd.merge(households,cfb_table,on='name',how='left')

# Replace NaNs with Zeros for CFBHH

hhs_and_cfb['CFBHH'] = hhs_and_cfb['CFBHH'].fillna(0)

# Left Join the two datasets on 'Munid' (Yellow Columns in Excel)

total_hhs = pd.merge(hhs_and_cfb,timeshares,on='Munid',how='left')

# Replace NaNs with Zeros for Timeshare Props, roll15 count, and Discount columns

total_hhs['Timeshare_Props'] = total_hhs['Timeshare_Props'].fillna(0) # Timeshare Props is OMPF's count for time shares (lower)
total_hhs['Timeshare_HH'] = total_hhs['Timeshare_HH'].fillna(0) # roll15 count is MPACs count for time shares (higher)
total_hhs['Discount'] = total_hhs['Discount'].fillna(0) # Discount is the difference between the two 

# Roll Up Time Share Props and Time Share HH to their respective Upper Tiers 

ut_timeshare_prop = total_hhs.groupby('Munupid')['Timeshare_Props'].sum()
ut_timeshare_hh = total_hhs.groupby('Munupid')['Timeshare_HH'].sum()
ut_cfbhh = total_hhs.groupby('Munupid')['CFBHH'].sum()

# Merge these UT Rollup into a column with the LTs and STs for Times Shares and CFBHH
# Use np.where(condition, if true, if false) to replace VLOOKUP/SUMIF

rollup1 = pd.merge(total_hhs,ut_timeshare_prop,on='Munupid', how='left')
rollup1['Timeshare_Props_rollup'] = np.where(rollup1.Munid == rollup1.Munupid, rollup1.Timeshare_Props_y, rollup1.Timeshare_Props_x)

rollup2 = pd.merge(rollup1,ut_timeshare_hh,on='Munupid', how='left')
rollup2['Timeshare_HH_rollup'] = np.where(rollup2.Munid == rollup2.Munupid, rollup2.Timeshare_Props_y, rollup2.Timeshare_Props_x)

rollup3 = pd.merge(rollup2,ut_cfbhh,on='Munupid', how='left')
rollup3['CFBHH_rollup'] = np.where(rollup3.Munid == rollup3.Munupid, rollup3.Timeshare_Props_y, rollup3.Timeshare_Props_x)


final_hhs = rollup3

final_hhs.drop(['Timeshare_Props_x'], axis=1, inplace=True)
final_hhs.drop(['Timeshare_Props_y'], axis=1, inplace=True)
final_hhs.drop(['Timeshare_HH_x'], axis=1, inplace=True)
final_hhs.drop(['Timeshare_HH_y'], axis=1, inplace=True)
final_hhs.drop(['CFBHH_y'], axis=1, inplace=True)
final_hhs.drop(['CFBHH_x'], axis=1, inplace=True)


final_hhs = final_hhs.assign(Total_HHs = lambda x: x['RUHH'] + x['RDUHH'] + x['FRUHH'] +
                            x['Timeshare_Props_rollup'] + x['Timeshare_HH_rollup'] + x['CFBHH_rollup'])


final_hhs

### ---- SAVE DATAFRAME AS A CSV in Working Directory ----

workdir = os.getcwd()
final_hhs.to_csv(workdir+'\\hhs.csv')






