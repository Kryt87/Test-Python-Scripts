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
ELEC_SHEET = "Electricity Characteristics"
GAS_FILE = "Gas Network Model 1.8.xlsx"
GAS_SHEET = "ArcFM Model - Features & Object"
MAP_FILE = "GIS Integration Interfaces Attribute Mapping_Master.xlsx"
MAP_ELEC_SHEET = "Attributes Electricity"
MAP_GAS_SHEET = "Attributes_Gas2"

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
    print("Loading the three excel files.")

    elec_data = pd.read_excel(ELEC_FILE, sheet_name=ELEC_SHEET, skiprows=1,
                              keep_default_na=False)
    elec_data.replace('', np.nan, inplace=True)
    gas_data = pd.read_excel(GAS_FILE, sheet_name=GAS_SHEET, skiprows=1,
                             keep_default_na=False)
    gas_data.replace('', np.nan, inplace=True)
    old_eg_data = pd.read_excel(LOCATED + in_file, sheet_name="GIS Data",
                                keep_default_na=False)
    old_eg_data.replace('', np.nan, inplace=True)
    com_old_data = old_eg_data[['TABLE',
                                'COLUMN',
                                'ELEC/GAS',
                                'FLOC/EQUIP',
                                'Master Location',
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
                                'SAP']]
    com_old_data['Old/New'] = 'Old'

    strp_elec = elec_data[['Source Table', 'Source Column', 'SAP required',
                           'Master System', 'SAP Field Type',
                           'Technical Object']]
    strp_elec = strp_elec.rename(columns={'Source Table': 'TABLE',
                                          'Source Column': 'COLUMN',
                                          'SAP required': 'SAP',
                                          'Master System': 'Master Location',
                                          'SAP Field Type': 'SAP Data Type',
                                          'Technical Object': 'FLOC/EQUIP'})
    strp_elec['ELEC/GAS'] = 'E'

    strp_gas = gas_data[['OBJECTCLASSNAME', 'NAME2', 'Migrated GIS to SAP ',
                         'Data Master', 'SAP Data Type', 'FLOC - EQUIP']]
    strp_gas = strp_gas.rename(columns={'OBJECTCLASSNAME': 'TABLE',
                                        'NAME2': 'COLUMN',
                                        'Migrated GIS to SAP ': 'SAP',
                                        'Data Master': 'Master Location',
                                        'FLOC - EQUIP': 'FLOC/EQUIP'})
    strp_gas['ELEC/GAS'] = 'G'
    strp_old = old_eg_data[['TABLE', 'COLUMN', 'MDS CRITICAL', 'Transforming',
                            'Incorrect Data', 'DR#', 'REF', 'Notes']]
    strp_old = strp_old.drop_duplicates(subset=['TABLE', 'COLUMN'])
    old_sap = old_eg_data[['TABLE', 'COLUMN', 'SAP', 'Date Changed']]
    return strp_elec, strp_gas, strp_old, old_sap, com_old_data


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


def old_strip_sql(data, sap_stat=True):
    """Removes non-existent and corrects tablenames."""
    tab_fixs = [["PCOGIS.SDE.", ''],
                ["Auxiliary Equipment", "AUXILLARYEQUIPMENT"]]
    for old_str, new_str in tab_fixs:
        data['TABLE'] = data['TABLE'].str.replace(old_str, new_str)
    data = data.dropna(subset=['COLUMN'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area",
                "ACTUALLENGTH", "DECOMMISSIONINGDATE", "DECOMMISSIONINGREASON"]
    data = data[~data['COLUMN'].isin(bad_atts)]
    bad_tab_atts = [['SWITCHUNIT$', 'INTERRUPTINGMEDIUM$'],
                    ['DistributionMain$', '^CROSSINGID$'],
                    ['DistributionMain$', '^MOUNTINGTYPE$'],
                    ['DistributionMain$', '^MOUNTINGPOSITION$']]
    for tab_str, att_str in bad_tab_atts:
        data = data[~(data['TABLE'].str.match(tab_str) &
                      data['COLUMN'].str.match(att_str))]
    bad_doubles = [['Regulator$', '^SUBTYPECD$', 'y'],
                   ['RegulatorStation$', '^EQUIPMENTID$', 'N'],
                   ['SurfaceStructure$', '^APPLICATION$', 'N'],
                   ['SurfaceStructure$', '^ENTRY$', 'N'],
                   ['SurfaceStructure$', '^FACILITYID$', 'N'],
                   ['SurfaceStructure$', '^MANUFACTURER$', 'N'],
                   ['SurfaceStructure$', '^MATERIAL$', 'N'],
                   ['SurfaceStructure$', '^MODEL$', 'N'],
                   ['SurfaceStructure$', '^STRUCTURESIZE$', 'N'],
                   ['COMMSPOWERSUPPLY$', '^BATTERYAMPERAGEHOURS$', 'N'],
                   ['COMMSPOWERSUPPLY$', '^BATTERYCOUNT$', 'N']]
    if sap_stat is True:
        for tab_str, att_str, sap_str in bad_doubles:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].str.match(sap_str))]
        bad_null = [['SurfaceStructure$', '^ENCLOSURE$'],
                    ['SurfaceStructure$', '^ENCLOSURETYPE$'],
                    ['SurfaceStructure$', '^ENCLOSUREMANUFACTURER$']]
        for tab_str, att_str in bad_null:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].isnull())]
    return data


def strip_sql(data, sap_stat=True):
    """Removes non-existent and corrects tablenames."""
    tab_fixs = [["PCOGIS.SDE.", ''],
                ["Auxiliary Equipment", "AUXILLARYEQUIPMENT"]]
    for old_str, new_str in tab_fixs:
        data['TABLE'] = data['TABLE'].str.replace(old_str, new_str)
    data = data.dropna(subset=['COLUMN'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area",
                "None", "ACTUALLENGTH", "DECOMMISSIONINGDATE",
                "DECOMMISSIONINGREASON", 'LOTS YET TO ADD']
    data = data[~data['COLUMN'].isin(bad_atts)]
    bad_tabs = ['LocationAttributes', 'CustomerConnections', 'TBD']
    data = data[~data['TABLE'].isin(bad_tabs)]
    bad_tab_atts = [['SWITCHUNIT$', 'INTERRUPTINGMEDIUM$'],
                    ['DistributionMain$', 'CROSSINGID$'],
                    ['DistributionMain$', 'MOUNTINGTYPE$'],
                    ['DistributionMain$', 'MOUNTINGPOSITION$']]
    for tab_str, att_str in bad_tab_atts:
        data = data[~(data['TABLE'].str.match(tab_str) &
                      data['COLUMN'].str.match(att_str))]
    bad_doubles = [['Regulator$', 'SUBTYPECD$', 'y'],
                   ['RegulatorStation$', 'EQUIPMENTID$', 'N'],
                   ['SurfaceStructure$', 'APPLICATION$', 'N'],
                   ['SurfaceStructure$', 'ENTRY$', 'N'],
                   ['SurfaceStructure$', 'FACILITYID$', 'N'],
                   ['SurfaceStructure$', 'MANUFACTURER$', 'N'],
                   ['SurfaceStructure$', 'MATERIAL$', 'N'],
                   ['SurfaceStructure$', 'MODEL$', 'N'],
                   ['SurfaceStructure$', 'STRUCTURESIZE$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'BATTERYAMPERAGEHOURS$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'BATTERYCOUNT$', 'N'],
                   ['PillarPoint$', 'DATEMANUFACTURED$', 'TBC'],
                   ['PillarPoint$', 'FACILITYID$', 'TBC'],
                   ['PillarPoint$', 'FEEDERID$', 'TBC'],
                   ['PillarPoint$', 'NUMBEROFUSEDCIRCUITS$', 'TBC'],
                   ['PillarPoint$', 'SUBTYPECD$', 'N'],
                   ['PillarPoint$', 'TOTALNUMBEROFCIRCUITS$', 'TBC'],
                   ['PillarPoint$', 'TRUENZMGPOS$', 'N'],
                   ['SupportStructure$', 'HIGHESTVOLTAGE$', 'N'],
                   ['SurfaceStructure$', 'ASSETFUNCTION$', 'N'],
                   ['SurfaceStructure$', 'ENCLOSUREMANUFACTURER$', 'N'],
                   ['SurfaceStructure$', 'ENCLOSURETYPE$', 'N'],
                   ['SurfaceStructure$', 'GLOBALID$', 'N'],
                   ['SurfaceStructure$', 'STREETNAME$', 'N'],
                   ['SurfaceStructure$', 'STREETNO$', 'N'],
                   ['SurfaceStructure$', 'SUBURB$', 'N'],
                   ['SurfaceStructure$', 'SYMBOLROTATION$', 'N'],
                   ['SurfaceStructure$', 'TOWN$', 'N'],
                   ['Switch$', 'FACILITYID$', 'N'],
                   ['Switch$', 'FEEDERID$', 'N'],
                   ['Switch$', 'FEEDERID2$', 'N'],
                   ['Switch$', 'GEONETFEEDERCODE$', 'N'],
                   ['Switch$', 'GLOBALID$', 'N'],
                   ['Switch$', 'GROUNDEDINDICATOR$', 'N'],
                   ['Switch$', 'INSTALLATIONDATE$', 'N'],
                   ['Switch$', 'MOUNTING$', 'N'],
                   ['Switch$', 'NORMALPOSITION$', 'N'],
                   ['Switch$', 'NUMPHASES$', 'N'],
                   ['Switch$', 'OPERATINGVOLTAGE$', 'N'],
                   ['Switch$', 'OUTOFORDERINDICATOR$', 'N'],
                   ['Switch$', 'REFERENCE$', 'N'],
                   ['Switch$', 'REMOTECONTROLLED$', 'N'],
                   ['Switch$', 'REMOTEINDICATION$', 'N'],
                   ['Switch$', 'RETICULATION$', 'N'],
                   ['Switch$', 'SITEID$', 'N'],
                   ['Switch$', 'STREETNAME$', 'N'],
                   ['Switch$', 'STREETNO$', 'N'],
                   ['Switch$', 'SUBTYPECD$', 'N'],
                   ['Switch$', 'SUBURB$', 'N'],
                   ['Switch$', 'SYMBOLROTATION$', 'N'],
                   ['Switch$', 'TOWN$', 'N'],
                   ['Switch$', 'WORKORDERID$', 'N'],
                   ['SWITCHUNIT$', 'ARCQUENCHING$', 'N'],
                   ['SWITCHUNIT$', 'C_INTJDEID$', 'N'],
                   ['SWITCHUNIT$', 'COMMENTS$', 'N'],
                   ['SWITCHUNIT$', 'DATEMANUFACTURED$', 'N'],
                   ['SWITCHUNIT$', 'DATEPURCHASED$', 'N'],
                   ['SWITCHUNIT$', 'INSTALLATIONDATE$', 'N'],
                   ['SWITCHUNIT$', 'INSULATIONMEDIUM$', 'N'],
                   ['SWITCHUNIT$', 'LOADBREAKINGCAPACITY$', 'N'],
                   ['SWITCHUNIT$', 'MANUFACTURER$', 'N'],
                   ['SWITCHUNIT$', 'MODEL$', 'N'],
                   ['SWITCHUNIT$', 'NORMALCURRENTRATING$', 'N'],
                   ['SWITCHUNIT$', 'NUMPHASES$', 'N'],
                   ['SWITCHUNIT$', 'OWNER$', 'N'],
                   ['SWITCHUNIT$', 'REFERENCE$', 'N'],
                   ['SWITCHUNIT$', 'SERIALNUMBER$', 'N'],
                   ['SWITCHUNIT$', 'VISUALEARTHINDICATOR$', 'N'],
                   ['SWITCHUNIT$', 'VOLTAGERATING$', 'N'],
                   ['SWITCHUNIT$', 'WORKORDERID$', 'N'],
                   ['UndergroundStructure$', 'C_INTJDEID$', 'N'],
                   ['UndergroundStructure$', 'COMMENTS$', 'N'],
                   ['UndergroundStructure$', 'FACILITYID$', 'N'],
                   ['UndergroundStructure$', 'FEEDERID$', 'N'],
                   ['UndergroundStructure$', 'GLOBALID$', 'N'],
                   ['UndergroundStructure$', 'HIGHESTVOLTAGE$', 'N'],
                   ['UndergroundStructure$', 'INSTALLATIONDATE$', 'N'],
                   ['UndergroundStructure$', 'OUTOFORDERINDICATOR$', 'N'],
                   ['UndergroundStructure$', 'OWNER$', 'N'],
                   ['UndergroundStructure$', 'REFERENCE$', 'N'],
                   ['UndergroundStructure$', 'STREETNAME$', 'N'],
                   ['UndergroundStructure$', 'STREETNO$', 'N'],
                   ['UndergroundStructure$', 'SUBURB$', 'N'],
                   ['UndergroundStructure$', 'SYMBOLROTATION$', 'N'],
                   ['UndergroundStructure$', 'TOWN$', 'N'],
                   ['UndergroundStructure$', 'WORKORDERID$', 'N'],
                   ['Fuse$', 'INSTALLATIONDATE$', 'N'],
                   ['Ground$', 'BELOWGROUNDCONNECTION$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'COOLINGTYPE$', 'TBD'],
                   ['POWERTRANSFORMERUNIT$', 'COOLINGTYPE2$', 'TBD'],
                   ['POWERTRANSFORMERUNIT$', 'COOLINGTYPE3$', 'TBD'],
                   ['POWERTRANSFORMERUNIT$', 'CTBURDENVA$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'CTCLASS$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'CTQUANTITY$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'CTRATIO$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'IMPEDANCE2$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'IMPEDANCE3$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'IMPEDANCEZ0$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'RATEDMVA$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'RATEDMVA2$', 'N'],
                   ['POWERTRANSFORMERUNIT$', 'RATEDMVA3$', 'N'],
                   ['AUXILLARYEQUIPMENT$', 'MANUFACTURER$', 'N'],
                   ['AUXILLARYEQUIPMENT$', 'MODEL$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'BATTERYTYPE$', 'N'],
                   ['SupportStructure$', 'FUNCTION_$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'GENERATORFUELTYPE$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'HOURSOFSUPPLY$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'PARALELLCOUNT$', 'N'],
                   ['COMMSPOWERSUPPLY$', 'PARALELLCOUNT$', 'TBD'],
                   ['COMMSPOWERSUPPLY$', 'SYSTEMVOLTAGE$', 'TBD'],
                   ['SurfaceStructure$', 'TRUENZMGPOS$', 'N'],
                   ['SupportStructure$', 'ABSOLUTE$', 'N'],
                   ['DISTTRANSFUSEUNIT$', 'VOLTAGERATING$', 'N'],
                   ['DISTTRANSFUSEUNIT$', 'WORKORDERID$', 'N'],
                   ['SupportStructure$', 'FEEDERID$', 'TBC'],
                   ['SupportStructure$', 'SHAPE$', ' N'],
                   ['SupportStructure$', 'SUBTYPECD$', 'TBD'],
                   ['SupportStructure$', 'TREATMENTTYPE$', 'N'],
                   ['SupportStructure$', 'TRUENZMG$', 'N'],
                   ['SupportStructure$', 'TYPEOFTOP$', 'N'],
                   ['SupportStructure$', 'USAGETYPE$', 'N']]
    if sap_stat is True:
        for tab_str, att_str, sap_str in bad_doubles:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
                          data['SAP'].str.match(sap_str))]
        bad_null = [['SurfaceStructure$', 'ENCLOSURE$'],
                    ['SurfaceStructure$', 'ENCLOSUREMANUFACTURER$'],
                    ['SurfaceStructure$', 'ENCLOSURETYPE$'],
                    ['Fuse$', 'ACCURACY$'],
                    ['Fuse$', 'ANCILLARYROLE$'],
                    ['Fuse$', 'ASSETFUNCTION$'],
                    ['Fuse$', 'C_INTJDEID$'],
                    ['Fuse$', 'COMMENTS$'],
                    ['Fuse$', 'CREATIONUSER$'],
                    ['Fuse$', 'DATECREATED$'],
                    ['Fuse$', 'DATEMODIFIED$'],
                    ['Fuse$', 'DEVICETYPE$'],
                    ['Fuse$', 'ELECTRICTRACEWEIGHT$'],
                    ['Fuse$', 'ENABLED$'],
                    ['Fuse$', 'FACILITYID$'],
                    ['Fuse$', 'FEEDERID$'],
                    ['Fuse$', 'FEEDERID2$'],
                    ['Fuse$', 'FEEDERINFO$'],
                    ['Fuse$', 'GEONETFEEDERCODE$'],
                    ['Fuse$', 'GEONETFEEDERID$'],
                    ['Fuse$', 'GEONETSUBSTATION$'],
                    ['Fuse$', 'GLOBALID$'],
                    ['Fuse$', 'INSTALLEDBY$'],
                    ['Fuse$', 'LABELTEXT$'],
                    ['Fuse$', 'LASTUSER$'],
                    ['Fuse$', 'MANUFACTURER$'],
                    ['Fuse$', 'MAXCONTINUOUSCURRENT$'],
                    ['Fuse$', 'MAXINTERRUPTINGCURRENT$'],
                    ['Fuse$', 'MAXOPERATINGVOLTAGE$'],
                    ['Fuse$', 'MOUNTING$'],
                    ['Fuse$', 'NOMINALVOLTAGE$'],
                    ['Fuse$', 'NORMALPOSITION$'],
                    ['Fuse$', 'NUMPHASES$'],
                    ['Fuse$', 'OBJECTID$'],
                    ['Fuse$', 'OPERATINGVOLTAGE$'],
                    ['Fuse$', 'OUTOFORDERINDICATOR$'],
                    ['Fuse$', 'OWNER$'],
                    ['Fuse$', 'PARENTID$'],
                    ['Fuse$', 'PHASEDESIGNATION$'],
                    ['Fuse$', 'PREMISE$'],
                    ['Fuse$', 'PRESENTPOSITION$'],
                    ['Fuse$', 'RDB_UFID$'],
                    ['Fuse$', 'REFERENCE$'],
                    ['Fuse$', 'REMOTECONTROLLED$'],
                    ['Fuse$', 'REMOTEINDICATION$'],
                    ['Fuse$', 'RETICULATION$'],
                    ['Fuse$', 'SCADACONTROLMECHANISM$'],
                    ['Fuse$', 'SCADACONTROLTYPE$'],
                    ['Fuse$', 'SCADAPTID$'],
                    ['Fuse$', 'SHAPE$'],
                    ['Fuse$', 'SITEID$'],
                    ['Fuse$', 'STREETNAME$'],
                    ['Fuse$', 'STREETNO$'],
                    ['Fuse$', 'SUBTYPECD$'],
                    ['Fuse$', 'SUBURB$'],
                    ['Fuse$', 'SYMBOLROTATION$'],
                    ['Fuse$', 'TIMESTAMP$'],
                    ['Fuse$', 'TOWN$'],
                    ['Fuse$', 'TYPE$'],
                    ['Fuse$', 'WORKORDERID$'],
                    ['Fuse$', 'ZONE$']]
        for tab_str, att_str in bad_null:
            data = data[~(data['TABLE'].str.match(tab_str) &
                          data['COLUMN'].str.match(att_str) &
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
    data.replace(np.nan, '', inplace=True)
    if ((data['SAP_x'] != data['SAP_y']) &
            (pd.notnull(data['SAP_x']) & pd.notnull(data['SAP_y']))):
        data['Date Changed'] = (str(data['SAP_y']) + ' to ' +
                                str(data['SAP_x']) + ' ' + n_date)
        print(str(data['TABLE']) + ' ' + str(data['COLUMN']) + ' ' +
              data['Date Changed'])
    data.replace('', np.nan, inplace=True)
    return data


def des_table_sql():
    """Created a dataframe of all attributes with a domain lookup."""
    sql_cmd = """SELECT TABLE_, FIELD_NAME, 'Y' AS 'DOMAIN LOOKUP'
    FROM PCOGIS.SDE.DOMAIN_LOOKUP_PC
    WHERE VALUE_ <> ''"""
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


def excel_print_diff(data):
    """This prints out the difference between old and new."""
    n_date = datetime.date.today().strftime("%d-%m-%y")
    excel_writer = pd.ExcelWriter('delta_data_summary-{}.xlsx'.format(n_date),
                                  engine='xlsxwriter')
    data.to_excel(excel_writer, sheet_name='GIS Delta',
                  index=False, freeze_panes=(1, 0))
    num_cols = len(list(data))
    worksheet = excel_writer.sheets['GIS Delta']
    worksheet.autofilter(0, 0, 0, num_cols-1)
    worksheet.set_column(0, 0, 32.11)
    worksheet.set_column(1, 1, 34.89)

    excel_writer.save()


def script_runner():
    """Runs this script."""

    infile, outfile = file_names(LOCATED, FILE_START, FILE_END)

    strp_elec, strp_gas, strp_old, old_sap, com_old_data = file_loading(infile)
    strpmap_elec, strpmap_gas = load_gis_interface_file()

    eg_data = pd.concat([strp_elec, strp_gas], sort=True)
    map_data = pd.concat([strpmap_elec, strpmap_gas], sort=True)
    eg_data = strip_sql(eg_data)
    map_data = old_strip_sql(map_data, False)
    eg_data = eg_data.drop_duplicates(subset=['TABLE', 'COLUMN', 'SAP'])
    map_data = map_data.drop_duplicates(subset=['TABLE', 'COLUMN'])

    eg_data = schema_list(eg_data)

    eg_data = up_merge(eg_data, strp_old)
    eg_data = up_merge(eg_data, map_data)

    new_sap = eg_data[['TABLE', 'COLUMN', 'SAP']]
    # old_sap = strip_sql(old_sap)
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

    com_new_data = eg_data[['TABLE',
                            'COLUMN',
                            'ELEC/GAS',
                            'FLOC/EQUIP',
                            'Master Location',
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
                            'SAP']]
    com_new_data['Old/New'] = 'New'
    com_old_data.replace('', np.nan, inplace=True)
    com_new_data.replace('', np.nan, inplace=True)
    fin_dif = pd.concat([com_old_data, com_new_data]).drop_duplicates(
        subset=['TABLE',
                'COLUMN',
                'ELEC/GAS',
                'FLOC/EQUIP',
                'Master Location',
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
                'SAP'], keep=False)

    # os.rename(LOCATED + infile, DEST_LOCATION + infile)

    excel_print_diff(fin_dif)


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
