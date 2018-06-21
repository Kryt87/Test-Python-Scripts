# -*- coding: utf-8 -*-
"""
Created on Fri Jun 15 11:47:19 2018

@author: KLD300
"""

import datetime
import os
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_col_to_name
import pypyodbc as da


CONNECTION_GIS = "SERVER=PWGISSQL01;DATABASE=PCOGIS;"
CONNECTION_LVJDE = "SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;"

LOCATED = 'P:\\NF\\Data Migration\\Data Decisions\\'
FILE_START = 'Data_Decisions_Summary-V'
FILE_END = '.xlsx'
DEST_LOCATION = 'P:\\NF\\Data Migration\\Data Decisions\\Archive\\'


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
        print(sql)
        print("-------------------")
        print("-------------------")
        data = None
    else:
        if headers is not None:
            data.columns = headers
    return data


def file_names(located, file_start, file_end):
    """Finds the highest version and next file version

    files have the format start + x.xx + end, where x represent intigers.
    """
    if located:
        file_list = os.listdir(located)
    else:
        file_list = os.listdir()
    sum_list = [x for x in file_list if file_start in x and '~$' not in x]
    vers_list = [x[len(file_start):-len(file_end)] for x in sum_list]
    split_list = [x.split('.') for x in vers_list]
    vers = max([int(x[0]) for x in split_list])
    rev = max([int(x[1]) for x in split_list if str(vers) in x[0]])

    infile = file_start + str(vers) + '.' + str(rev).zfill(2) + file_end
    outfile = file_start + str(vers) + '.' + str(rev+1).zfill(2) + file_end
    return infile, outfile


def file_loading(in_file):
    """Creats four data frames from three files."""
    print("Loading the Master excel file.")

    old_eg_data = pd.read_excel(LOCATED + in_file, sheet_name="GIS Data")

    strp_old = old_eg_data[['TABLE',
                            'COLUMN',
                            'GIS Type',
                            'GIS - Limit/Precision',
                            'DOMAIN LOOKUP',
                            'ELEC/GAS',
                            'FLOC/EQUIP',
                            'Master Location',
                            'Transforming',
                            'SAP Data Type',
                            'CWMS',
                            'CCMS',
                            'MIDDLEWARE',
                            'NOCVIEW',
                            'GOTHAM',
                            'GASVIEW',
                            'ELECTRICVIEW',
                            'PSSSINCAL',
                            'SPATIALVIEWS',
                            'VMS',
                            'DEFECTSVIEWER',
                            'OMS',
                            'AMT',
                            'EDW',
                            'UGLOCATIONS',
                            'PTREE',
                            'DRATCRITICALITY',
                            'GISPORTAL',
                            'GASHUB(SiteCore)',
                            'MDS CRITICAL',
                            'SAP',
                            'Date Changed',
                            'Incorrect Data',
                            'DR#',
                            'REF',
                            'Notes']]
    return strp_old


def up_merge(data1, data2):
    """Panda merge using uppercase."""
    data1['TABLE_up'] = data1['TABLE'].str.upper()
    data1['NAME_up'] = data1['COLUMN'].str.upper()
    data2['TABLE_up'] = data2['TABLE'].str.upper()
    data2['NAME_up'] = data2['COLUMN'].str.upper()
    strp_columns = list(data2)
    strp_columns.remove('TABLE')
    strp_columns.remove('COLUMN')
    data2 = data2[strp_columns]
    fin_data = pd.merge(data1, data2, how='left',
                        left_on=['TABLE_up', 'NAME_up'],
                        right_on=['TABLE_up', 'NAME_up'])
    fin_list = list(fin_data)
    fin_list.remove('TABLE_up')
    fin_list.remove('NAME_up')
    fin_data = fin_data[fin_list]
    return fin_data


def nonblank_sql(data):  # This causes a Warning!?
    """Quarries the number of non-nulls per attribute."""
    sql_cmd = "SELECT count(" + str(data['COLUMN']) + """) AS 'Count'
    FROM PCOGIS.SDE.""" + str(data['TABLE'])
    if data['Total'] == 0:
        new_col = 0
    else:
        col = get_sql(sql_cmd, CONNECTION_GIS)
        new_col = col["count"].iloc[-1]
    return new_col


def objectid_sql(table_name):
    """Quarries the total number of objects (rows) per attribute."""
    sql_cmd = """SELECT count(*) AS 'Count'
    FROM PCOGIS.SDE.""" + str(table_name)
    col = get_sql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col


def cont_null_df(data):
    """Counts values with a missing description."""
    count_sum = 0
    count_sum = data['count'][data['description'].isnull()].sum()
    if count_sum is False:
        count_sum = 0
    return count_sum


def missing_domain(data):
    """Creates a column of the number of values with a missing description."""
    if data['DOMAIN LOOKUP'] == 'N':
        if data['NULL'] == data['Total']:
            col = 0
        else:
            col = data['Incorrect Data']
    elif data['DOMAIN LOOKUP'] == 'Y':
        if data['NULL'] == data['Total']:
            col = 0
        else:
            sql_cmd = (
                    'SELECT ' + 'tab.' + data['COLUMN'] + '''
,dl.DESCRIPTION AS 'DESCRIPTION'
,COUNT(*) AS "Count"
FROM PCOGIS.SDE.''' + data['TABLE'] + ''' tab
LEFT JOIN PCOGIS.sde.DOMAIN_LOOKUP_PC dl ON tab.''' + data['COLUMN'] + ''' = dl.VALUE_
AND dl.TABLE_ = \'''' + data['TABLE'] + '''\'
AND dl.FIELD_NAME = \'''' + data['COLUMN'] + '''\'
WHERE tab.''' + data['COLUMN'] + ''' IS NOT NULL
GROUP BY tab.''' + data['COLUMN'] + ''', dl.DESCRIPTION
ORDER BY tab.''' + data['COLUMN'])
            col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
            col = cont_null_df(col_dom_df)
    # print(data['TABLE'], data['COLUMN'], col)
    return col


def final_order(data):
    """Sorts both cols and rows to desired order."""
    data = data[['TABLE',
                 'COLUMN',
                 'GIS Type',
                 'GIS - Limit/Precision',
                 'DOMAIN LOOKUP',
                 'ELEC/GAS',
                 'FLOC/EQUIP',
                 'Master Location',
                 'Transforming',
                 'SAP Data Type',
                 'CWMS',
                 'CCMS',
                 'MIDDLEWARE',
                 'NOCVIEW',
                 'GOTHAM',
                 'GASVIEW',
                 'ELECTRICVIEW',
                 'PSSSINCAL',
                 'SPATIALVIEWS',
                 'VMS',
                 'DEFECTSVIEWER',
                 'OMS',
                 'AMT',
                 'EDW',
                 'UGLOCATIONS',
                 'PTREE',
                 'DRATCRITICALITY',
                 'GISPORTAL',
                 'GASHUB(SiteCore)',
                 'MDS CRITICAL',
                 'SAP',
                 'Date Changed',
                 'NULL',
                 'Not-NULL',
                 'Incorrect Data',
                 'Total',
                 '% Not-NULL',
                 '% Complete',
                 'DR#',
                 'REF',
                 'Notes']]
    data = data.sort_values(by=['TABLE', 'COLUMN'])
    return data


def add_nn_comp_forms(worksheet, names_dict, num_rows):
    """Creates a percentage formula for two columns in excel."""
    notnull_let = xl_col_to_name(names_dict['Not-NULL'])
    incor_let = xl_col_to_name(names_dict['Incorrect Data'])
    total_let = xl_col_to_name(names_dict['Total'])

    for i in range(1, num_rows):
        nn_form_str = '=' + notnull_let + str(i+1) + '/' + total_let + str(i+1)
        comp_form_str = ('=(' + notnull_let + str(i+1) + '-' +
                         incor_let + str(i+1) + ')/' + total_let + str(i+1))
        worksheet.write_formula(i, names_dict['% Not-NULL'], nn_form_str)
        worksheet.write_formula(i, names_dict['% Complete'], comp_form_str)

    return worksheet


def excel_print(data, outfile_name):
    """This prints out the dataframe in the correct format."""
    excel_writer = pd.ExcelWriter(LOCATED + outfile_name, engine='xlsxwriter')
    data.to_excel(excel_writer, sheet_name='GIS Data',
                  index=False, freeze_panes=(1, 0))

    names_dict = dict((v, k) for k, v in dict(enumerate(list(data))).items())
    num_cols = len(list(data))
    num_rows = len(data)

    workbook = excel_writer.book
    worksheet = excel_writer.sheets['GIS Data']

    worksheet.autofilter(0, 0, 0, num_cols-1)
    worksheet.filter_column_list(names_dict['SAP'], ['Y'])

    worksheet = add_nn_comp_forms(worksheet, names_dict, num_rows)

    perc_format = workbook.add_format({'num_format': '0%'})
    bg_red = workbook.add_format({'bg_color': '#FF8080'})

    worksheet.set_column(names_dict['% Not-NULL'], names_dict['% Complete'],
                         None, perc_format)
    worksheet.conditional_format(1, names_dict['% Complete'],
                                 num_rows, names_dict['% Complete'],
                                 {'type': 'cell',
                                  'criteria': '<=',
                                  'value': 0.5,
                                  'format': bg_red})

    excel_writer.save()


def script_runner():
    """Runs this script."""

    infile, outfile = file_names(LOCATED, FILE_START, FILE_END)
    eg_data = file_loading(infile)



    print("--------------------------")
    print('Querying the total number of objects per table. As of '
          + str(datetime.datetime.now()))
    table_list = eg_data[['TABLE']].drop_duplicates()
    table_list['Total'] = table_list['TABLE'].apply(objectid_sql)
    eg_data = pd.merge(eg_data, table_list, how='left',
                       left_on=['TABLE'],
                       right_on=['TABLE'])

    print('Querying the number of non-nulls per attribute. As of '
          + str(datetime.datetime.now()))
    eg_data['Not-NULL'] = eg_data.apply(nonblank_sql, axis=1)
    eg_data['NULL'] = eg_data['Total'] - eg_data['Not-NULL']

    eg_data['% Not-NULL'] = np.nan
    eg_data['% Complete'] = np.nan

    print('Querying sum of values with no domain description. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(missing_domain, axis=1)

    eg_data = final_order(eg_data)

    excel_print(eg_data, outfile)

    os.rename(LOCATED + infile, DEST_LOCATION + infile)


print("--------------------------")
print('Starting ' + str(datetime.datetime.now()))
print("--------------------------")

script_runner()

print("--------------------------")
print('Ending ' + str(datetime.datetime.now()))
print("""--------------------------
--------------------------
      Share Workbook
--------------------------
--------------------------""")
