# -*- coding: utf-8 -*-
"""
Created on Wed May 23 16:21:50 2018

@author: KLD300
"""


import pandas as pd
import pypyodbc as da

CONNECTION_GIS = "SERVER=PWGISSQL01;DATABASE=PCOGIS;"
CONNECTION_LVJDE = "SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;"

# ELEC_FILE = r"Electricity GIS-SAP Attribute Mapping.xlsx"
ELEC_FILE = r"old_Electricity GIS-SAP Attribute Mapping.xlsx"
GAS_FILE = "Gas_Network_Model_1.7.xlsx"


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


def file_loading():
    """Creats four data frames from three files."""
    print("Loading the two excel files.")

    elec_data = pd.read_excel(ELEC_FILE,
                              sheet_name="GIS Attributes")
    gas_data = pd.read_excel(GAS_FILE,
                             sheet_name="ArcFM Model - Features & Object",
                             skiprows=1)
    strp_elec = elec_data[['GIS Table', 'GIS Column Name',
                           'SAP required']]
    strp_elec = strp_elec.rename(columns={'GIS Table': 'TABLE',
                                          'GIS Column Name': 'COLUMN',
                                          'SAP required': 'SAP'})
    strp_elec['ELEC/GAS'] = 'E'

    strp_gas = gas_data[['OBJECTCLASSNAME', 'NAME2', 'Migrated GIS to SAP ']]
    strp_gas = strp_gas.rename(columns={'OBJECTCLASSNAME': 'TABLE',
                                        'NAME2': 'COLUMN',
                                        'Migrated GIS to SAP ': 'SAP'})
    strp_gas['ELEC/GAS'] = 'G'
    return strp_elec, strp_gas


def strip_sql(data):
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


def run_this():
    """Runs this script."""
    strp_elec, strp_gas = file_loading()
    eg_data = pd.concat([strp_elec, strp_gas], sort=True)
    eg_data = strip_sql(eg_data)
    eg_data = eg_data.drop_duplicates(subset=['TABLE', 'COLUMN', 'SAP'])

    eg_data = schema_list(eg_data)

    excel_writer = pd.ExcelWriter('new_test_schema.xlsx', engine='xlsxwriter')
    eg_data.to_excel(excel_writer, sheet_name='GIS Data',
                     index=False, freeze_panes=(1, 0))
    excel_writer.save()


run_this()
