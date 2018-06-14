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

ELEC_FILE = r"Electricity GIS-SAP Attribute Mapping.xlsx"
ELEC_SHEET = "GIS Current State"
GAS_FILE = "Gas_Network_Model_1.7.xlsx"
GAS_SHEET = "ArcFM Model - Features & Object"
MAP_FILE = "GIS Integration Interfaces Attribute Mapping_Master.xlsx"
MAP_ELEC_SHEET = "Attributes Electricity"
MAP_GAS_SHEET = "Attributes_Gas2"

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
    print("Loading the three excel files.")

    elec_data = pd.read_excel(ELEC_FILE, sheet_name=ELEC_SHEET)
    gas_data = pd.read_excel(GAS_FILE, sheet_name=GAS_SHEET, skiprows=1,
                             keep_default_na=False)
    old_eg_data = pd.read_excel(LOCATED + in_file, sheet_name="GIS Data")

    strp_elec = elec_data[['Table', 'Column', 'Alias', 'SAP required']]
    strp_elec = strp_elec.rename(columns={'Table': 'TABLE',
                                          'Column': 'COLUMN',
                                          'Alias': 'ALIAS',
                                          'SAP required': 'SAP'})
    strp_elec['ELEC/GAS'] = 'E'

    strp_gas = gas_data[['OBJECTCLASSNAME', 'NAME2', 'FIELDALIAS',
                         'Migrated GIS to SAP ',
                         'SAP Technical Object Description',
                         'SAP Field Name',
                         'FLOC - EQUIP',
                         'SAP Data Type']]
    strp_gas = strp_gas.rename(columns={'OBJECTCLASSNAME': 'TABLE',
                                        'NAME2': 'COLUMN',
                                        'FIELDALIAS': 'ALIAS',
                                        'Migrated GIS to SAP ': 'SAP',
                                        'FLOC - EQUIP': 'FLOC/EQUIP'})
    strp_gas['ELEC/GAS'] = 'G'
    strp_old = old_eg_data[['TABLE', 'COLUMN', 'MDS CRITICAL',
                            'Incorrect Data', 'DR#', 'REF', 'Notes']]
    strp_old = strp_old.drop_duplicates(subset=['TABLE', 'COLUMN'])
    old_sap = old_eg_data[['TABLE', 'COLUMN', 'SAP', 'Date Changed']]

    return strp_elec, strp_gas, strp_old, old_sap


def load_gis_interface_file():
    """Loads the file with GIS intergration mapping."""
    elec_data = pd.read_excel(MAP_FILE, sheet_name=MAP_ELEC_SHEET)
    gas_data = pd.read_excel(MAP_FILE, sheet_name=MAP_GAS_SHEET)
    strp_elec = elec_data[['GIS Table',
                           'GIS Column Name',
                           'CCMS',
                           'MIDDLEWARE ',
                           'NOCVIEW',
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
                           'DRAT_CRITICALITY',
                           'GIS_PORTAL']]
    strp_elec = strp_elec.rename(columns={'GIS Table': 'TABLE',
                                          'GIS Column Name': 'COLUMN',
                                          'MIDDLEWARE ': 'MIDDLEWARE',
                                          'DRAT_CRITICALITY':
                                              'DRATCRITICALITY',
                                          'GIS_PORTAL': 'GISPORTAL'})
    strp_gas = gas_data[['GIS Table',
                         'GIS Column Name',
                         'CWMS',
                         'CCMS',
                         'MIDDLEWARE ',
                         'GOTHAM',
                         'GASVIEW',
                         'SPATIAL VIEWS',
                         'EDW',
                         'UGLOCATIONS',
                         'GISPORTAL',
                         'GASHUB(SiteCore)']]
    strp_gas = strp_gas.rename(columns={'GIS Table': 'TABLE',
                                        'GIS Column Name': 'COLUMN',
                                        'MIDDLEWARE ': 'MIDDLEWARE',
                                        'SPATIAL VIEWS': 'SPATIALVIEWS'})
    return strp_elec, strp_gas


def strip_sql(data, sap_stat=True):
    """Removes non-existent and corrects tablenames."""
    tab_fixs = [["PCOGIS.SDE.", ''],
                ["Auxiliary Equipment", "AUXILLARYEQUIPMENT"]]
    for old_str, new_str in tab_fixs:
        data['TABLE'] = data['TABLE'].str.replace(old_str, new_str)
    data = data.dropna(subset=['COLUMN'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area",
                "ACTUALLENGTH", "DECOMMISSIONINGDATE", "DECOMMISSIONINGREASON"]
    data = data[~data['COLUMN'].isin(bad_atts)]
    bad_tab_atts = [['SWITCHUNIT', 'INTERRUPTINGMEDIUM'],
                    ['DistributionMain', 'CROSSINGID'],
                    ['DistributionMain', 'MOUNTINGTYPE'],
                    ['DistributionMain', 'MOUNTINGPOSITION']]
    for tab_str, att_str in bad_tab_atts:
        data = data[~(data['TABLE'].str.contains(tab_str) &
                      data['COLUMN'].str.contains(att_str))]
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
    if sap_stat is True:
        for tab_str, att_str, sap_str in bad_doubles:
            data = data[~(data['TABLE'].str.contains(tab_str) &
                          data['COLUMN'].str.contains(att_str) &
                          data['SAP'].str.contains(sap_str))]
        bad_null = [['SurfaceStructure', 'ENCLOSURE'],
                    ['SurfaceStructure', 'ENCLOSUREMANUFACTURER']]
        for tab_str, att_str in bad_null:
            data = data[~(data['TABLE'].str.contains(tab_str) &
                          data['COLUMN'].str.contains(att_str) &
                          data['SAP'].isnull())]
    return data


def schema_list(data):
    """Creats a more complete list of fields from the schema."""
    all_schema = get_schema(CONNECTION_GIS)
    all_schema['GIS - Limit/Precision'] = all_schema[
        'character_maximum_length'].fillna(all_schema[
            'numeric_precision']).fillna(all_schema[
                'datetime_precision'])
    strp_schema = all_schema[~all_schema['table_schema'].str.contains('jde')]
    strp_schema = strp_schema[['table_name', 'column_name',
                               'data_type', 'GIS - Limit/Precision']]
    strp_schema = strp_schema.rename(columns={'table_name': 'TABLE',
                                              'column_name': 'COLUMN',
                                              'data_type': 'GIS Type'})
    strp_schema['TABLE_up'] = strp_schema['TABLE'].str.upper()
    strp_schema['NAME_up'] = strp_schema['COLUMN'].str.upper()
    data['TABLE_up'] = data['TABLE'].str.upper()
    data['NAME_up'] = data['COLUMN'].str.upper()
    unique_tabs = data['TABLE_up'].unique()

    fin_schema = strp_schema[strp_schema['TABLE_up'].isin(unique_tabs)]
    eg_stat = data[['TABLE_up', 'ELEC/GAS']]
    eg_stat = eg_stat.drop_duplicates(subset=['TABLE_up', 'ELEC/GAS'])
    fin_schema = pd.merge(fin_schema, eg_stat, how='left',
                          left_on=['TABLE_up'],
                          right_on=['TABLE_up'])

    strp_columns = list(data)
    strp_columns.remove('TABLE')
    strp_columns.remove('COLUMN')
    strp_columns.remove('ELEC/GAS')
    new_data = data[strp_columns]

    fin_data = pd.merge(fin_schema, new_data, how='left',
                        left_on=['TABLE_up', 'NAME_up'],
                        right_on=['TABLE_up', 'NAME_up'])
    fin_list = list(fin_data)
    fin_list.remove('TABLE_up')
    fin_list.remove('NAME_up')
    fin_data = fin_data[fin_list]
    return fin_data


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


def diff_columns(data):  # This function needs to be updated to remove warning.
    """Determines if there is a SAP migration change and dates change."""
    n_date = datetime.date.today().strftime("%d-%m-%y")
    if ((data['SAP_x'] != data['SAP_y']) &
            (pd.notnull(data['SAP_x']) & pd.notnull(data['SAP_y']))):
        data['Date Changed'] = (str(data['SAP_y']) + ' to ' +
                                str(data['SAP_x']) + ' ' + n_date)
        print(str(data['TABLE']) + ' ' + str(data['COLUMN']) + ' ' +
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
    WHERE """ + str(data['COLUMN']) + " IS NULL"
    col = get_sql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col


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


def final_order(data):
    """Sorts both cols and rows to desired order."""
    data = data[['TABLE',
                 'COLUMN',
                 'ALIAS',
                 'GIS Type',
                 'GIS - Limit/Precision',
                 'DOMAIN LOOKUP',
                 'ELEC/GAS',
                 'FLOC/EQUIP',
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

    strp_elec, strp_gas, strp_old, old_sap = file_loading(infile)
    strpmap_elec, strpmap_gas = load_gis_interface_file()

    eg_data = pd.concat([strp_elec, strp_gas], sort=True)
    map_data = pd.concat([strpmap_elec, strpmap_gas], sort=True)
    eg_data = strip_sql(eg_data)
    map_data = strip_sql(map_data, False)
    eg_data = eg_data.drop_duplicates(subset=['TABLE', 'COLUMN', 'SAP'])
    map_data = map_data.drop_duplicates(subset=['TABLE', 'COLUMN'])

    eg_data = schema_list(eg_data)

    eg_data = up_merge(eg_data, strp_old)
    eg_data = up_merge(eg_data, map_data)

    new_sap = eg_data[['TABLE', 'COLUMN', 'SAP']]
    old_sap = strip_sql(old_sap)
    old_sap = old_sap.drop_duplicates(subset=['TABLE', 'COLUMN', 'SAP'])
    dif_sap = up_merge(new_sap, old_sap)
    dif_sap = dif_sap.apply(diff_columns, axis=1)
    new_dif_sap = dif_sap[['TABLE', 'COLUMN', 'Date Changed']]

    new_dif_sap = new_dif_sap.drop_duplicates(subset=['TABLE', 'COLUMN',
                                                      'Date Changed'])
    eg_data = up_merge(eg_data, new_dif_sap)
    eg_data['% Not-NULL'] = np.nan
    eg_data['% Complete'] = np.nan

    dom_look = des_table_sql()
    dom_look = dom_look.rename(columns={'table_': 'TABLE',
                                        'field_name': 'COLUMN',
                                        'domain lookup': 'DOMAIN LOOKUP'})
    eg_data = up_merge(eg_data, dom_look)
    eg_data['DOMAIN LOOKUP'] = eg_data['DOMAIN LOOKUP'].fillna('N')

    # print("--------------------------")
    # print('Querying the number of nulls per attribute. As of '
    #       + str(datetime.datetime.now()))
    # eg_data['NULL'] = eg_data.apply(null_sql, axis=1)
    # print("--------------------------")

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
