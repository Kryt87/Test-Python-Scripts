# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 13:06:08 2018

@author: KLD300
"""

import pypyodbc as da
import pandas as pd
import numpy as np

#CONNECTION_LAB = "Driver={SQL Server};SERVER=tstrndsql01.jsds1.test;DATABASE=rawZone;Trusted_Connection=yes"
#CONNECTION_BAM = "Driver={SQL Server};SERVER=PWDWHSQLDW;DATABASE=DW Public Arena;Trusted_Connection=yes"
CONNECTION_GIS = "Driver={SQL Server};SERVER=PWGISSQL01;DATABASE=PCOGIS;Trusted_Connection=yes"
CONNECTION_JDE = "Driver={SQL Server};SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;Trusted_Connection=yes"
#CONNECTION_CWMS = "Driver={SQL Server};SERVER=PWWMSSQL01;DATABASE=PowercoWMS;Trusted_Connection=yes"

def getSqlConnection(conn_string):
    return da.connect(conn_string)

def getSql(sql, conn_string,headers=None):
    cnxn = getSqlConnection(conn_string)
    data = pd.read_sql_query(sql,cnxn)
    if headers is not None:
        data.columns = headers
    return data

def tableSearch(data, val):
    sql_cmd = "SELECT count(" + str(data['column_name']) + """) AS 'Count'
    FROM """ + str(data['table_schema']) + "." + str(data['table_name']) + """
    WHERE """ + str(data['column_name']) + " = '" + val + "'"
    #print(sql_cmd)
    #print(str(data['data_type']))
    if str(data['data_type']) == 'nchar':
        col = getSql(sql_cmd, CONNECTION_JDE)
        new_col = col["count"].iloc[-1]
    else:
        new_col = 0
    return new_col

all_tabels = "SELECT * FROM information_schema.tables"

all_tabs_atts = """
SELECT C.Table_Catalog DB,C.Table_Schema, C.Table_Name, Column_Name, Data_Type
    FROM Information_Schema.Columns C JOIN Information_Schema.Tables T
    ON C.table_name = T.table_name
    WHERE Table_Type = 'BASE TABLE'
"""

table_schema = getSql(all_tabels, CONNECTION_JDE)
all_schema = getSql(all_tabs_atts, CONNECTION_JDE)


# search for HAWA
all_schema['HAWA Count'] = all_schema.apply(tableSearch, args=('HAWA',), axis = 1)

excel_writer = pd.ExcelWriter('searched.xlsx', engine='xlsxwriter')
all_schema.to_excel(excel_writer, sheet_name='Data', index=False )
excel_writer.save()