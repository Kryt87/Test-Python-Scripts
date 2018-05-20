# -*- coding: utf-8 -*-
"""
Created on Thu May 17 11:28:19 2018

@author: KurtDrew
"""

import datetime
import os
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_col_to_name
import pypyodbc as da


CONNECTION_GIS = "SERVER=PWGISSQL01;DATABASE=PCOGIS;"
CONNECTION_LVJDE = "SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;"

# ELEC_FILE = r"Electricity GIS-SAP Attribute Mapping.xlsx"
ELEC_FILE = r"old_Electricity GIS-SAP Attribute Mapping.xlsx"
GAS_FILE = "Gas_Network_Model_1.7.xlsx"

LOCATED = 'P:\\NF\\Data Migration\\Data Decisions\\'
# LOCATED = ''
FILE_START = 'Data_Decisions_Summary-V'
FILE_END = '.xlsx'
DEST_LOCATION = 'P:\\NF\\Data Migration\\Data Decisions\\Archive\\'
# DEST_LOCATION = ''


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
    print("Loading the three excel files.")

    elec_data = pd.read_excel(ELEC_FILE,
                              sheet_name="GIS Attributes")
    gas_data = pd.read_excel(GAS_FILE,
                             sheet_name="ArcFM Model - Features & Object",
                             skiprows=1)
    old_eg_data = pd.read_excel(LOCATED + in_file, sheet_name="GIS Data")

    strp_elec = elec_data[['GIS Table', 'GIS Column Name', 'ALIAS ', 'E/FLOC',
                           'SAP required']]
    strp_elec = strp_elec.rename(columns={'GIS Table': 'TABLE',
                                          'GIS Column Name': 'NAME',
                                          'ALIAS ': 'ALIAS',
                                          'SAP required': 'SAP'})
    strp_elec['ELEC/GAS'] = 'E'

    strp_gas = gas_data[['OBJECTCLASSNAME', 'NAME2', 'FIELDALIAS',
                         'FLOC - EQUIP', 'Migrated GIS to SAP ',
                         'SAP Technical Object Description',
                         'SAP Field Name']]
    strp_gas = strp_gas.rename(columns={'OBJECTCLASSNAME': 'TABLE',
                                        'NAME2': 'NAME',
                                        'FIELDALIAS': 'ALIAS',
                                        'FLOC - EQUIP': 'E/FLOC',
                                        'Migrated GIS to SAP ': 'SAP'})
    strp_gas['ELEC/GAS'] = 'G'
    strp_old = old_eg_data[['TABLE', 'NAME', 'Incorrect Data', 'DR#', 'Notes']]
    strp_old = strp_old.drop_duplicates(subset=['TABLE', 'NAME'])
    old_sap = old_eg_data[['TABLE', 'NAME', 'SAP', 'Date Changed']]

    return strp_elec, strp_gas, strp_old, old_sap


def strip_sql(data):
    """Removes non-existent and corrects tablenames."""
    tab_fixs = [["PCOGIS.SDE.", ''],
                ["Auxiliary Equipment", "AUXILLARYEQUIPMENT"]]
    for old_str, new_str in tab_fixs:
        data['TABLE'] = data['TABLE'].str.replace(old_str, new_str)
    data = data.dropna(subset=['NAME'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area",
                "ACTUALLENGTH", "DECOMMISSIONINGDATE", "DECOMMISSIONINGREASON"]
    data = data[~data['NAME'].isin(bad_atts)]
    bad_tab_atts = [['SWITCHUNIT', 'INTERRUPTINGMEDIUM'],
                    ['DistributionMain', 'CROSSINGID'],
                    ['DistributionMain', 'MOUNTINGTYPE'],
                    ['DistributionMain', 'MOUNTINGPOSITION']]
    for tab_str, att_str in bad_tab_atts:
        data = data[~(data['TABLE'].str.contains(tab_str) &
                      data['NAME'].str.contains(att_str))]
    bad_doubles = [['Regulator', 'SUBTYPECD', 'y'],
                   ['RegulatorStation', 'EQUIPMENTID', 'N'],
                   ['SurfaceStructure', 'APPLICATION', 'N'],
                   ['SurfaceStructure', 'ENTRY', 'N'],
                   ['SurfaceStructure', 'FACILITYID', 'N'],
                   ['SurfaceStructure', 'MANUFACTURER', 'N'],
                   ['SurfaceStructure', 'MATERIAL', 'N'],
                   ['SurfaceStructure', 'MODEL', 'N'],
                   ['SurfaceStructure', 'STRUCTURESIZE', 'N'],
                   ['COMMSPOWERSUPPLY', 'BATTERYAMPERAGEHOURS', 'N'],
                   ['COMMSPOWERSUPPLY', 'BATTERYCOUNT', 'N']]
    for tab_str, att_str, sap_str in bad_doubles:
        data = data[~(data['TABLE'].str.contains(tab_str) &
                      data['NAME'].str.contains(att_str) &
                      data['SAP'].str.contains(sap_str))]
    bad_null = [['SurfaceStructure', 'ENCLOSURE'],
                ['SurfaceStructure', 'ENCLOSUREMANUFACTURER']]
    for tab_str, att_str in bad_null:
        data = data[~(data['TABLE'].str.contains(tab_str) &
                      data['NAME'].str.contains(att_str) &
                      data['SAP'].isnull())]
    return data


def diff_columns(data):  # This function needs to be updated to remove warning.
    """Determines if there is a SAP migration change and dates change."""
    n_date = datetime.date.today().strftime("%d-%m-%y")
    if ((data['SAP_x'] != data['SAP_y']) &
            (pd.notnull(data['SAP_x']) & pd.notnull(data['SAP_y']))):
        data['Date Changed'] = (str(data['SAP_y']) + ' to ' +
                                str(data['SAP_x']) + ' ' + n_date)
        print(str(data['TABLE']) + ' ' + str(data['NAME']) + ' ' +
              data['Date Changed'])
    return data


def des_table_sql():
    """Created a dataframe of all attributes with a domain lookup."""
    sql_cmd = """SELECT TABLE_, FIELD_NAME, 'Y' AS 'DOMAIN LOOKUP'
    FROM PCOGIS.SDE.DOMAIN_LOOKUP_PC"""
    col = get_sql(sql_cmd, CONNECTION_GIS)
    col = col.drop_duplicates()
    return col


def get_schema(connection):
    """Creates a dataframe of the current schema."""
    all_tabs_atts = """
SELECT C.Table_Catalog DB,C.Table_Schema, C.Table_Name, Column_Name,
       Data_Type, character_maximum_length, numeric_precision,
       datetime_precision
    FROM Information_Schema.Columns C JOIN Information_Schema.Tables T
    ON C.table_name = T.table_name
    WHERE Table_Type = 'BASE TABLE'
"""
    schema = get_sql(all_tabs_atts, connection)
    return schema


def null_sql(data):
    """Quarries the number of NULLS per attribute."""
    sql_cmd = """SELECT count(*) AS 'Count'
    FROM PCOGIS.SDE.""" + str(data['TABLE']) + """
    WHERE """ + str(data['NAME']) + " IS NULL"
    col = get_sql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col


def nonblank_sql(data):
    """Quarries the number of non-nulls per attribute."""
    sql_cmd = "SELECT count(" + str(data['NAME']) + """) AS 'Count'
    FROM PCOGIS.SDE.""" + str(data['TABLE'])
    col = get_sql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col


def objectid_sql(table_name):
    """Quarries the total number of objects (rows) per attribute."""
    sql_cmd = """SELECT count(OBJECTID) AS 'Count'
    FROM PCOGIS.SDE.""" + str(table_name)
    col = get_sql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col


def final_order(data):
    """Sorts both cols and rows to desired order."""
    data = data[['TABLE',
                 'NAME',
                 'ALIAS',
                 'E/FLOC',
                 'GIS Type',
                 'GIS - Limit/Precision',
                 'DOMAIN LOOKUP',
                 'ELEC/GAS',
                 'SAP',
                 'Date Changed',
                 'NULL',
                 'Not-NULL',
                 'Incorrect Data',
                 'Total',
                 '% Not-NULL',
                 '% Complete',
                 'DR#',
                 'Notes']]
    data = data.sort_values(by=['TABLE', 'NAME'])
    return data


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

    notnull_let = xl_col_to_name(names_dict['Not-NULL'])
    incor_let = xl_col_to_name(names_dict['Incorrect Data'])
    total_let = xl_col_to_name(names_dict['Total'])

    for i in range(1, num_rows):
        nn_form_str = '=' + notnull_let + str(i+1) + '/' + total_let + str(i+1)
        comp_form_str = ('=(' + notnull_let + str(i+1) + '-' +
                         incor_let + str(i+1) + ')/' + total_let + str(i+1))
        worksheet.write_formula(i, names_dict['% Not-NULL'], nn_form_str)
        worksheet.write_formula(i, names_dict['% Complete'], comp_form_str)

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

    strp_elec, strp_gas, strp_old, old_sap = file_loading(infile)

    eg_data = pd.concat([strp_elec, strp_gas], sort=True)
    eg_data = strip_sql(eg_data)
    eg_data = eg_data.drop_duplicates(subset=['TABLE', 'NAME', 'SAP'])

    eg_data = pd.merge(eg_data, strp_old, how='left',
                       left_on=['TABLE', 'NAME'], right_on=['TABLE', 'NAME'])

    new_sap = eg_data[['TABLE', 'NAME', 'SAP']]
    old_sap = strip_sql(old_sap)
    old_sap = old_sap.drop_duplicates(subset=['TABLE', 'NAME', 'SAP'])
    dif_sap = pd.merge(new_sap, old_sap, how='left', left_on=['TABLE', 'NAME'],
                       right_on=['TABLE', 'NAME'])
    dif_sap = dif_sap.apply(diff_columns, axis=1)
    new_dif_sap = dif_sap[['TABLE', 'NAME', 'Date Changed']]

    new_dif_sap = new_dif_sap.drop_duplicates(subset=['TABLE', 'NAME',
                                                      'Date Changed'])
    eg_data = pd.merge(eg_data, new_dif_sap, how='left',
                       left_on=['TABLE', 'NAME'],
                       right_on=['TABLE', 'NAME'])
    eg_data['% Not-NULL'] = np.nan
    eg_data['% Complete'] = np.nan

    dom_look = des_table_sql()
    dom_look = dom_look.rename(columns={'table_': 'TABLE',
                                        'field_name': 'NAME',
                                        'domain lookup': 'DOMAIN LOOKUP'})
    eg_data = pd.merge(eg_data, dom_look, how='left',
                       left_on=['TABLE', 'NAME'],
                       right_on=['TABLE', 'NAME'])
    eg_data['DOMAIN LOOKUP'] = eg_data['DOMAIN LOOKUP'].fillna('N')

    all_schema = get_schema(CONNECTION_GIS)
    all_schema['GIS - Limit/Precision'] = all_schema[
        'character_maximum_length'].fillna(all_schema[
            'numeric_precision']).fillna(all_schema[
                'datetime_precision'])
    strp_schema = all_schema[~all_schema['table_schema'].str.contains('jde')]
    strp_schema = strp_schema[['table_name', 'column_name',
                               'data_type', 'GIS - Limit/Precision']]
    strp_schema = strp_schema.rename(columns={'table_name': 'TABLE_up',
                                              'column_name': 'NAME_up',
                                              'data_type': 'GIS Type'})
    strp_schema['TABLE_up'] = strp_schema['TABLE_up'].str.upper()
    strp_schema['NAME_up'] = strp_schema['NAME_up'].str.upper()
    eg_data['TABLE_up'] = eg_data['TABLE'].str.upper()
    eg_data['NAME_up'] = eg_data['NAME'].str.upper()
    eg_data = pd.merge(eg_data, strp_schema, how='left',
                       left_on=['TABLE_up', 'NAME_up'],
                       right_on=['TABLE_up', 'NAME_up'])

    print('Querying the number of nulls per attribute. As of '
          + str(datetime.datetime.now()))
    eg_data['NULL'] = eg_data.apply(null_sql, axis=1)
    print('Querying the number of non-nulls per attribute. As of '
          + str(datetime.datetime.now()))
    eg_data['Not-NULL'] = eg_data.apply(nonblank_sql, axis=1)

    print('Querying the total number of objects per table. As of '
          + str(datetime.datetime.now()))
    eg_data['Total'] = eg_data['TABLE'].apply(objectid_sql)

    fin_data = final_order(eg_data)

    excel_print(fin_data, outfile)

    os.rename(LOCATED + infile, DEST_LOCATION + infile)


print('Starting ' + str(datetime.datetime.now()))

script_runner()

print('Ending ' + str(datetime.datetime.now()))
print("""--------------------------
      Share Workbook
--------------------------""")
