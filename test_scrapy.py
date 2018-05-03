# -*- coding: utf-8 -*-
"""
Created on Thu May  3 13:23:48 2018

@author: KLD300
"""

a = 'https://public.tableau.com/vizql/w/EVv3/v/Storytables/vud/sessions/78245D1BB6284BC1A70A72D0B36C4F53-0:0/views/8468050762830641998_12255308769217115928?csv=true&showall=true'

url = '''
https://public.tableau.com/vizql/w/EVv3/v/Storytables/vud/sessions/494EF9A4EB3241E886AB24091744FB9A-0:0/views/8468050762830641998_12255308769217115928?csv=true
'''

ev_url = '''
https://www.emi.ea.govt.nz/Retail/Download/DataReport/CSV/GUEHMT?Capacity=All_Drilldown&FuelType=All_Drilldown
'''
b = '''
https://www.emi.ea.govt.nz/Retail/Download/DataReport/CSV/GUEHMT?Capacity=All_Drilldown&FuelType=All_Drilldown&RegionType=NWKP

'''

import pandas as pd
#import selenium as sel

from selenium import webdriver
from bs4 import BeautifulSoup

import selenium
from pandas.io.html import read_html
from selenium import webdriver
from selenium.webdriver import Chrome # pip install selenium
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import sqlalchemy
from sqlalchemy import types

chromeOptions = webdriver.ChromeOptions()
prefs = {"download.default_directory" : r"\\tstrndsql01.jsds1.test\rawZone\SrcData\DG"}
chromeOptions.add_experimental_option("prefs",prefs)

# chromedriver = "path/to/chromedriver.exe"
# driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chromeOptions)

#options = webdriver.ChromeOptions() 
#options.add_argument("download.default_directory=\\Users\\KLD300\\Documents\\Python Scripts\\Web_Scrap_EV")

#down_load_dir = 'C:\\Users\\KLD300\\Documents\\Python Scripts\\Web_Scrap_EV'

#chromeOptions = webdriver.ChromeOptions()
#prefs1 = {"download.default_directory" : down_load_dir}
#chromeOptions.add_experimental_option("prefs",prefs1)

#browser = webdriver.Chrome(chrome_options=chromeOptions)#chrome_options=chromeOptions)
#browser.get(a)


MoT = pd.read_csv('C:\\Users\\KLD300\\Downloads\\Towns_by_reg_quarter_data.csv')

dtyp = {c:types.VARCHAR(int(MoT[c].str.len().max()))
        for c in MoT.columns[MoT.dtypes == 'object'].tolist()}

engine = sqlalchemy.create_engine("mssql+pyodbc://tstrndsql01.jsds1.test/rawZone?driver=SQL+Server+Native+Client+11.0")
destination_table_name='List_MoT_DATA' # table name
engine.connect()
MoT.to_sql(name=destination_table_name,con=engine,schema='srcDG',if_exists='replace',dtype=dtyp,index=False)


#browser.close()