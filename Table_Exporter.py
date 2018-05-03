# -*- coding: utf-8 -*-
"""
Created on Thu May  3 15:46:09 2018

@author: KLD300
"""

import pypyodbc as da
import pandas as pd
import datetime

def getSqlConnection(conn_string):
    return da.connect(conn_string);

def getSql(sql, conn_string,headers=None):
    cnxn = getSqlConnection(conn_string)
    data = pd.read_sql_query(sql,cnxn)
    if headers is not None:
        data.columns = headers
    return data

def sqlRunner(table, sql_ser):
    sql = {'JDE':['JDE_PRODUCTION.PRODDTA.',CONNECTION_LVJDE],
           'GIS':['PCOGIS.SDE.',CONNECTION_GIS]}
    sql_cmd = """SELECT *
    FROM """ + sql[sql_ser][0] + table
    try:
        mydf = getSql(sql_cmd, sql[sql_ser][1])
    except pyodbc.DatabaseError:
        print("Hey! " + table + " is not a table in " + sql_ser + ".")
    else:
        excel_writer = pd.ExcelWriter(table + '.xlsx', engine='xlsxwriter')
        mydf.to_excel(excel_writer, sheet_name='Data', index=False)
        excel_writer.save()

CONNECTION_GIS = "Driver={SQL Server};SERVER=PWGISSQL01;DATABASE=PCOGIS;Trusted_Connection=yes"
CONNECTION_LVJDE = "Driver={SQL Server};SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;Trusted_Connection=yes"

print('Starting ' + str(datetime.datetime.now()))


con_type = input("Are you more a JDE or GIS person? ").upper()

while True:
    if con_type not in ['GIS', 'JDE']:
        con_type = input("Hey you can't say that! Try again: ").upper()
    else:
        break

listTables = []
val = input("What table do you wish sir? (Press enter to end): ")



while True:
    if val == "":
        print("See ya sucker!")
        break
    else:
        listTables.append(val)
        val = input("Another sir? (Press enter to end): ")

if len(listTables) == 0:
    print("""
          -------------------------
          Why did you run me then?!
          -------------------------""")
else:
    for i in listTables:
        sqlRunner(i, con_type)


print('Ending ' + str(datetime.datetime.now()))