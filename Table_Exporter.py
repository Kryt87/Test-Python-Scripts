# -*- coding: utf-8 -*-
"""
Created on Thu May  3 15:46:09 2018

@author: KLD300
"""

import pypyodbc as da
import pandas as pd
import datetime

def getSqlConnection(conn_string):
    return da.connect(conn_string)

def getSql(sql, conn_string,headers=None):
    cnxn = getSqlConnection(conn_string)
    try:
        data = pd.read_sql_query(sql,cnxn)
    except pd.io.sql.DatabaseError:
        print("SQL DataBase Error")
        data = None
    else:
        if headers is not None:
            data.columns = headers
    return data

def sqlRunner(table, sql_ser, out_type):
    sql = {'JDE':['JDE_PRODUCTION.PRODDTA.',CONNECTION_LVJDE],
           'GIS':['PCOGIS.SDE.',CONNECTION_GIS]}
    sql_cmd = """SELECT *
    FROM """ + sql[sql_ser][0] + table
    print("Querying table " + table)
    mydf = getSql(sql_cmd, sql[sql_ser][1])
    if mydf is None:
        print("Hey! " + table + " is not a table in " + sql_ser + ".")
    else:
        print('Writing ' + table + ' to ' + out_type)
        if out_type == 'EXCEL':
            excel_writer = pd.ExcelWriter(table + '.xlsx', engine='xlsxwriter')
            mydf.to_excel(excel_writer, sheet_name='Data', index=False)
            excel_writer.save()
        elif out_type == 'CSV':
#            print('WIP')
            mydf.to_csv(table + '.csv')


CONNECTION_GIS = "Driver={SQL Server};SERVER=PWGISSQL01;DATABASE=PCOGIS;Trusted_Connection=yes"
CONNECTION_LVJDE = "Driver={SQL Server};SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;Trusted_Connection=yes"

print('Starting ' + str(datetime.datetime.now()))


con_type = input("Are you more a JDE or GIS person? ").upper()

while True:
    if con_type not in ['GIS', 'JDE']:
        con_type = input("Hey you can't say that! Try again: ").upper()
    else:
        break

outType = input("Is your perfered flavour EXCEL or CSV? ").upper()

while True:
    if outType not in ['EXCEL', 'CSV']:
        outType = input("I'm sorry I don't know that flavour. Try again: ").upper()
    else:
        break

listTables = []
val = input("What table do you wish sir? (Press enter to end): ")

while True:
    if val == "":
        print("Extracting now sir")
        break
    else:
        val = val.split('\n')
        listTables.extend(val)
        val = input("Another sir? (Press enter to end): ")

if len(listTables) == 0:
    print("""
          -------------------------
          Why did you run me then?!
          -------------------------
          """)
else:
    for i in listTables:
        sqlRunner(i, con_type, outType)

print("See ya sucker!" + str(datetime.datetime.now()))