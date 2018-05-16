# -*- coding: utf-8 -*-
"""
Created on Thu May  3 15:46:09 2018

@author: KLD300
"""

import datetime
import pandas as pd
import pypyodbc as da


def get_sql_connection(conn_string):
    """Connects to SQL."""
    return da.connect(conn_string)


def get_sql(sql, conn_string_part, headers=None):
    """Creats a dataframe from an SQL server.

    If there is a database error it returns None.
    """
    con_start = "Driver={SQL Server};"
    con_end = "Trusted_Connection=yes"
    conn_string = con_start + conn_string_part + con_end
    cnxn = get_sql_connection(conn_string)
    try:
        data = pd.read_sql_query(sql, cnxn)
    except pd.io.sql.DatabaseError:
        print("SQL DataBase Error")
        data = None
    else:
        if headers is not None:
            data.columns = headers
    return data


def sql_runner(table, sql_ser, out_type):
    """Prints out a file for each table requested."""
    sql = {'JDE': ['JDE_PRODUCTION.PRODDTA.', CONNECTION_LVJDE],
           'GIS': ['PCOGIS.SDE.', CONNECTION_GIS]}
    sql_cmd = """SELECT *
    FROM """ + sql[sql_ser][0] + table
    print("Querying table " + table)
    mydf = get_sql(sql_cmd, sql[sql_ser][1])
    if mydf is None:
        print('----------------------------------------')
        print("Hey! " + table + " is not a table in " + sql_ser + ".")
        print('----------------------------------------')
    else:
        print('Writing ' + table + ' to ' + out_type)
        if out_type == 'EXCEL':
            excel_writer = pd.ExcelWriter(table + '.xlsx', engine='xlsxwriter')
            mydf.to_excel(excel_writer, sheet_name='Data', index=False)
            excel_writer.save()
        elif out_type == 'CSV':
            mydf.to_csv(table + '.csv')


def run_runner():
    """Runs this module.

    Stars by asking which SQL server, then while file type to output to.
    You can add a list of tables as long as they are seperated by a new line.
    """
    con_type = input("Are you more a JDE or GIS person? ").upper()

    while True:
        if con_type not in ['GIS', 'JDE']:
            con_type = input("Hey you can't say that! Try again: ").upper()
        else:
            break

    out_type = input("Is your perfered flavour EXCEL or CSV? ").upper()

    while True:
        if out_type not in ['EXCEL', 'CSV']:
            out_type = input(
                "I'm sorry I don't know that flavour. Try again: ").upper()
        else:
            break

    list_tables = []
    val = input("What table do you wish sir? (Press enter to end): ")

    while True:
        if val == "":
            break
        else:
            val = val.split('\n')
            list_tables.extend(val)
            val = input("Another sir? (Press enter to end): ")

    if not list_tables:
        print("""
              -------------------------
              Why did you run me then?!
              -------------------------
              """)
    else:
        print("Extracting now sir")
        for i in list_tables:
            sql_runner(i, con_type, out_type)


CONNECTION_GIS = "SERVER=PWGISSQL01;DATABASE=PCOGIS;"
CONNECTION_LVJDE = "SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;"

print('Starting ' + str(datetime.datetime.now()))

run_runner()

print("See ya sucker! " + str(datetime.datetime.now()))
