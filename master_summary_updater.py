# -*- coding: utf-8 -*-
"""
Created on Fri Jun 15 11:47:19 2018

@author: KLD300

This script counts Non-Nulls and table totals. It also counts the errors per
attribute for:
    * Everything with a domain description
    * Every date type column
    * Every SUBTYPECD column
    * Every STREETNO column
    * Every SYMBOLROTATION column
    * Every C_INTJDEID column
    * Every TXWEIGHT column
    * Every TAPCHANGEROILVOLUME and TXOILVOLUME column
    * Every SERIALNUMBER column
    * Every BRIDGENAME column
    * Every COOLINGTYPE (1, 2 and 3) column (Missing Domain Lookups)
    * Every MOUNTING column (Missing Domain Lookups)
    * Every WORKORDER and WORKORDERID column (non-numeric etc.)
"""

import datetime
import os
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_col_to_name
import pypyodbc as da


CONNECTION_GIS = "SERVER=PWGISSQL01;DATABASE=PCOGIS;"
CONNECTION_LVJDE = "SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;"

# LOCATED = 'P:\\NF\\Data Migration\\Data Decisions\\'
LOCATED = 'C:\\Users\\KLD300\\Documents\\Python Scripts\\Perc_Updater\\'
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

    old_eg_data = pd.read_excel(LOCATED + in_file, sheet_name="GIS Data",
                                keep_default_na=False)
    old_eg_data.replace('', np.nan, inplace=True)

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


def nonblank_sql(data):
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
            sql_cmd = """SELECT tab.{1}
,dl.DESCRIPTION AS 'DESCRIPTION'
,COUNT(*) AS "Count"
FROM PCOGIS.SDE.{0} tab
LEFT JOIN (SELECT DISTINCT TABLE_, FIELD_NAME, VALUE_, DESCRIPTION
FROM PCOGIS.sde.DOMAIN_LOOKUP_PC) dl ON tab.{1} = dl.VALUE_
AND dl.TABLE_ = '{0}'
AND dl.FIELD_NAME = '{1}'
WHERE tab.{1} IS NOT NULL
GROUP BY tab.{1}, dl.DESCRIPTION
ORDER BY tab.{1}""".format(data['TABLE'], data['COLUMN'])
            col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
            col = cont_null_df(col_dom_df)
    return col


def bad_dates(data):
    """Querrys the number of erroneous dates."""
    if data['GIS Type'] == 'datetime2' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT COUNT(*) AS "Count"
FROM PCOGIS.sde.{0}
WHERE {1} < '01/01/1900'
OR ({1} > '01/01/1900'
AND {1} < '01/01/1910')
OR {1} > '01/01/2019'""".format(data['TABLE'], data['COLUMN'])
        sql_data = get_sql(sql_cmd, CONNECTION_GIS)
        col = sql_data["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_subtypecd(data):
    """Querrys the number of subtypecds missing a description."""
    if data['COLUMN'] == 'SUBTYPECD' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT tab.SUBTYPECD
,dl.SUBTYPE_NAME AS 'DESCRIPTION'
,COUNT(*) AS "Count"
FROM PCOGIS.SDE.{0} tab
LEFT JOIN (SELECT DISTINCT TABLE_, SUBTYPE_CODE, SUBTYPE_NAME
FROM PCOGIS.sde.DOMAIN_LOOKUP_PC) dl ON tab.SUBTYPECD = dl.SUBTYPE_CODE
AND dl.TABLE_ = '{0}'
WHERE tab.SUBTYPECD IS NOT NULL
GROUP BY tab.SUBTYPECD, dl.SUBTYPE_NAME
ORDER BY tab.SUBTYPECD""".format(data['TABLE'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = cont_null_df(col_dom_df)
    else:
        col = data['Incorrect Data']
    return col


def check_streetno(data):
    """Querrys the number of erroneous streetno values."""
    if data['COLUMN'] == 'STREETNO' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT	COUNT(STREETNO) AS 'Count'
FROM PCOGIS.sde.{0}
WHERE STREETNO LIKE '%[a-z][a-z]%'
AND NOT STREETNO LIKE '% NO %'
AND NOT STREETNO LIKE '%unit %'
AND NOT STREETNO LIKE '%lot %'
AND NOT STREETNO LIKE '%apartment %'
AND NOT STREETNO LIKE '%flat %'
OR STREETNO = '99990000'""".format(data['TABLE'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_symbolrotation(data):
    """Querrys the number of symbolrotation values outside of -360-to-360."""
    if data['COLUMN'] == 'SYMBOLROTATION' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE SYMBOLROTATION < -360
OR SYMBOLROTATION > 360""".format(data['TABLE'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_c_intjdeid(data):
    """Querrys the number of JDE id not of length 7."""
    if data['COLUMN'] == 'C_INTJDEID' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE LEN(CAST(C_INTJDEID AS INT)) <> 7""".format(data['TABLE'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_txweight(data):
    """Querrys the number of erroneous TXWEIGHT values."""
    if data['COLUMN'] == 'TXWEIGHT' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE TXWEIGHT NOT LIKE '%[0-9]KG'""".format(data['TABLE'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_volumes(data):
    """Querrys the number of erroneous TXWEIGHT values."""
    if (data['COLUMN'] in ('TAPCHANGEROILVOLUME', 'TXOILVOLUME')
            and data['NULL'] != data['Total']):
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE {1} NOT LIKE '%[0-9]L'""".format(data['TABLE'], data['COLUMN'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_serialnumber(data):
    """Querrys the number of erroneous serialnumber values."""
    if (data['COLUMN'] in ('SERIALNUMBER', 'SERIALNO', 'SERIAL',
                           'TAPCHANGERSERIALNO')
            and data['NULL'] != data['Total']):
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE {1} IS NOT NULL
GROUP BY {1}
HAVING {1} NOT LIKE '%[A-Z][0-9]%'
AND {1} NOT LIKE '%[0-9][A-Z]%'
AND ISNUMERIC({1}) = 0
OR {1} LIKE '%?%'
OR COUNT(*) > 1
ORDER BY {1}""".format(data['TABLE'], data['COLUMN'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].sum()
        if not col:
            col = 0
    else:
        col = data['Incorrect Data']
    return col


def check_bridgename(data):
    """Querrys the number of erroneous BRIDGENAME values."""
    if data['COLUMN'] == 'BRIDGENAME' and data['NULL'] != data['Total']:
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE BRIDGENAME IN ('', 'NOT ACCURATELY RECORDED')""".format(data['TABLE'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
    return col


def check_coolingtypes(data):
    """Querrys the number of erroneous COOLINGTYPE values."""
    if (data['COLUMN'] in ('COOLINGTYPE', 'COOLINGTYPE2', 'COOLINGTYPE3')
            and data['TABLE'] in ('POWERTRANSFORMERUNIT',
                                  'VOLTAGEREGULATORUNIT')
            and data['NULL'] != data['Total']):
        sql_cmd = """SELECT tab.{1}
,dl.DESCRIPTION
,COUNT(*)  AS 'Count'
FROM PCOGIS.SDE.{0} tab
LEFT JOIN (SELECT DISTINCT TABLE_, FIELD_NAME, VALUE_, DESCRIPTION
FROM PCOGIS.sde.DOMAIN_LOOKUP_PC) dl ON tab.{1} = dl.VALUE_
AND dl.TABLE_ = 'DISTRIBUTIONTRANSFORMERUNIT'
AND dl.FIELD_NAME = 'COOLINGTYPE'
WHERE tab.{1} IS NOT NULL
GROUP BY tab.{1}, dl.DESCRIPTION
ORDER BY tab.{1}""".format(data['TABLE'], data['COLUMN'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = cont_null_df(col_dom_df)
    else:
        col = data['Incorrect Data']
    return col


def check_mounting(data):
    """Querrys the number of erroneous MOUNTING values."""
    if (data['COLUMN'] == 'MOUNTING'
            and data['TABLE'] in ('INSTRUMENTTRANSFORMERUNIT',
                                  'POWERTRANSFORMERUNIT',
                                  'RECLOSERUNIT',
                                  'VOLTAGEREGULATORUNIT')
            and data['NULL'] != data['Total']):
        sql_cmd = """SELECT tab.{1}
,dl.DESCRIPTION
,COUNT(*)  AS 'Count'
FROM PCOGIS.SDE.{0} tab
LEFT JOIN (SELECT DISTINCT TABLE_, FIELD_NAME, VALUE_, DESCRIPTION
FROM PCOGIS.sde.DOMAIN_LOOKUP_PC) dl ON tab.{1} = dl.VALUE_
AND dl.TABLE_ = '{2}'
AND dl.FIELD_NAME = '{1}'
WHERE tab.{1} IS NOT NULL
GROUP BY tab.{1}, dl.DESCRIPTION
ORDER BY tab.{1}""".format(data['TABLE'], data['COLUMN'],
                           data['TABLE'].replace('UNIT', ''))
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = cont_null_df(col_dom_df)
    else:
        col = data['Incorrect Data']
    return col


def check_workorders(data):
    """Querrys the number of erroneous workorder ID values.

    Only values that are not an integer of length 5-8."""
    if (data['COLUMN'] in ('WORKORDER', 'WORKORDERID')
            and data['NULL'] != data['Total']):
        sql_cmd = """SELECT COUNT(*) AS 'Count'
FROM PCOGIS.SDE.{0}
WHERE {1} NOT LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
AND {1} NOT LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
AND {1} NOT LIKE '[0-9][0-9][0-9][0-9][0-9][0-9]'
AND {1} NOT LIKE '[0-9][0-9][0-9][0-9][0-9]'
OR {1} IN ('99999999', '999990000', '9990000', '9999000')""".format(
    data['TABLE'], data['COLUMN'])
        col_dom_df = get_sql(sql_cmd, CONNECTION_GIS)
        col = col_dom_df["count"].iloc[-1]
    else:
        col = data['Incorrect Data']
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


def column_length_format(col_lengths, names_dict, worksheet):
    """Resizes specific columns columns."""
    for col_name, col_val in col_lengths:
        colnum = names_dict[col_name]
        worksheet.set_column(colnum, colnum, col_val)
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

    col_lengths = [['TABLE', 32.11],
                   ['COLUMN', 34.89],
                   ['GIS Type', 9.44],
                   ['GIS - Limit/Precision', 10.44],
                   ['DOMAIN LOOKUP', 17.56],
                   ['ELEC/GAS', 10.78],
                   ['FLOC/EQUIP', 12.89],
                   ['Master Location', 16.11],
                   ['Transforming', 5.89],
                   ['SAP Data Type', 15.78],
                   ['SAP', 8.11],
                   ['Date Changed', 14.89],
                   ['NULL', 6.89],
                   ['Not-NULL', 10.67],
                   ['Incorrect Data', 9.78],
                   ['Total', 6.67],
                   ['% Not-NULL', 12.56],
                   ['% Complete', 12.44],
                   ['DR#', 13.22],
                   ['REF', 7.67],
                   ['Notes', 84.33]]

    worksheet = add_nn_comp_forms(worksheet, names_dict, num_rows)
    worksheet = column_length_format(col_lengths, names_dict, worksheet)

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
    print('Querying number of erroneous dates. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(bad_dates, axis=1)
    print('Querying number of SUBTYPECDS with no descriptions. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_subtypecd, axis=1)
    print('Querying number of erroneous STREETNO values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_streetno, axis=1)
    print('Querying number of erroneous SYMBOLROTATION values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_symbolrotation, axis=1)
    print('Querying number of erroneous C_INTJDEID values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_c_intjdeid, axis=1)
    print('Querying number of erroneous TXWEIGHT values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_txweight, axis=1)
    print('Querying number of erroneous TAPCHANGEROILVOLUME and TXOILVOLUME' +
          ' values. As of ' + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_volumes, axis=1)
    print('Querying number of erroneous serial number values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_serialnumber, axis=1)
    print('Querying number of erroneous BRIDGENAME values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_bridgename, axis=1)
    print('Querying number of erroneous COOLINGTYPES (1, 2 & 3) values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_coolingtypes, axis=1)
    print('Querying number of erroneous MOUNTING values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_mounting, axis=1)
    print('Querying number of erroneous WORKORDER & WORKORDERID values. As of '
          + str(datetime.datetime.now()))
    eg_data['Incorrect Data'] = eg_data.apply(check_workorders, axis=1)

    eg_data = final_order(eg_data)

    excel_print(eg_data, outfile)

    # os.rename(LOCATED + infile, DEST_LOCATION + infile)


START_TIME = datetime.datetime.now()

print("--------------------------")
print('Starting ' + str(START_TIME))
print("--------------------------")

script_runner()

END_TIME = datetime.datetime.now()

print("--------------------------")
print('Ending ' + str(END_TIME))
print('Ran for ' + str(END_TIME - START_TIME))
print("""--------------------------
--------------------------
      Share Workbook
--------------------------
--------------------------""")
