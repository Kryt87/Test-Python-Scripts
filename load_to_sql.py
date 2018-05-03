# -*- coding: utf-8 -*-
"""
Created on Thu May  3 10:45:51 2018

@author: KLD300
"""

import pandas as pd

import sqlalchemy
from sqlalchemy import types

import selenium
from pandas.io.html import read_html
from selenium import webdriver
from selenium.webdriver import Chrome # pip install selenium
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

emi_url = 'https://www.emi.ea.govt.nz/Reports/Retail/Chart/GUEHMT?Capacity=Small&FuelType=solar'

emi = pd.read_html(emi_url)
emi_list = emi[0]

dtyp = {c:types.VARCHAR(emi_list[c].str.len().max())
        for c in emi_list.columns[emi_list.dtypes == 'object'].tolist()}

engine = sqlalchemy.create_engine("mssql+pyodbc://tstrndsql01.jsds1.test/rawZone?driver=SQL+Server+Native+Client+11.0")
destination_table_name='List_EV_Locations' # table name
engine.connect()
emi_list.to_sql(name=destination_table_name,con=engine,schema='srcDG',if_exists='replace',dtype=dtyp,index=False)