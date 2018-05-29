# -*- coding: utf-8 -*-
"""
Created on Tue Apr 10 14:38:44 2018

@author: KLD300
"""

import pypyodbc as da
import pandas as pd
import numpy as np

#CONNECTION_LAB = "Driver={SQL Server};SERVER=tstrndsql01.jsds1.test;DATABASE=rawZone;Trusted_Connection=yes"
#CONNECTION_BAM = "Driver={SQL Server};SERVER=PWDWHSQLDW;DATABASE=DW Public Arena;Trusted_Connection=yes"
CONNECTION_GIS = "Driver={SQL Server};SERVER=PWGISSQL01;DATABASE=PCOGIS;Trusted_Connection=yes"
#CONNECTION_JDE = "Driver={SQL Server};SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;Trusted_Connection=yes"
CONNECTION_JDE = "Driver={SQL Server};SERVER=TSTGENSQL01.jsds1.test;DATABASE=JDE_ARL;Trusted_Connection=yes"
#CONNECTION_CWMS = "Driver={SQL Server};SERVER=PWWMSSQL01;DATABASE=PowercoWMS;Trusted_Connection=yes"

def getSqlConnection(conn_string):
    return da.connect(conn_string)

def getSql(sql, conn_string,headers=None):
    cnxn = getSqlConnection(conn_string)
    data = pd.read_sql_query(sql,cnxn)
    if headers is not None:
        data.columns = headers
    return data

def stripTable(data):
    data = data[data['table_type'].str.contains("VIEW")==False]
    data = data[data['table_name'].str.contains("HISTORY")==False]
    return data

def tableCounting(data):
    sql_cmd = """SELECT count(*) AS 'Count'
    FROM """ + str(data['table_schema']) + "." + str(data['table_name'])
    col = getSql(sql_cmd, CONNECTION_JDE)
    new_col = col["count"].iloc[-1]
    return new_col

def tableSearch(data, val):
    sql_cmd = "SELECT count(" + str(data['column_name']) + """) AS 'Count'
    FROM """ + str(data['table_schema']) + "." + str(data['table_name']) + """
    WHERE """ + str(data['column_name']) + " = '" + val + "'"
    #print(sql_cmd)
    #print(str(data['data_type']))
    if str(data['data_type']) in ['char','nchar','nvarchar','varchar']:
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

table_schema = stripTable(table_schema)

table_schema['Row Count'] = table_schema.apply(tableCounting, axis = 1)
print(table_schema)



#all_schema['HAWA Count'] = all_schema.apply(tableSearch, args=('HAWA',), axis = 1)
#all_schema['PB1-INV12-B20-PV6 Count'] = all_schema.apply(tableSearch, args=('PB1-INV12-B20-PV6',), axis = 1)
#all_schema['%PV6 Count'] = all_schema.apply(tableSearch, args=('%PV6',), axis = 1)


excel_writer = pd.ExcelWriter('jde_tables2.xlsx', engine='xlsxwriter')
table_schema.to_excel(excel_writer, sheet_name='Tables', index=False )
#all_schema.to_excel(excel_writer, sheet_name='Data', index=False )
excel_writer.save()