# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 13:10:43 2018

@author: KLD300
"""

import pypyodbc as da
import pandas as pd
import numpy as np
import datetime
import os
# from xlsxwriter.utility import xl_range
from xlsxwriter.utility import xl_col_to_name

print('Starting ' + str(datetime.datetime.now()))

##### Comment out SQL servers you do not have permissions for ######
# CONNECTION_LAB = "Driver={SQL Server};SERVER=tstrndsql01.jsds1.test;DATABASE=rawZone;Trusted_Connection=yes"
# CONNECTION_BAM = "Driver={SQL Server};SERVER=PWDWHSQLDW;DATABASE=DW Public Arena;Trusted_Connection=yes"
CONNECTION_GIS = "Driver={SQL Server};SERVER=PWGISSQL01;DATABASE=PCOGIS;Trusted_Connection=yes"
# CONNECTION_LVJDE = "Driver={SQL Server};SERVER=PWJDESQL01;DATABASE=JDE_PRODUCTION;Trusted_Connection=yes"
CONNECTION_TSJDE = "Driver={SQL Server};SERVER=TSTGENSQL01.jsds1.test;DATABASE=JDE_ARL;Trusted_Connection=yes"
# CONNECTION_CWMS = "Driver={SQL Server};SERVER=PWWMSSQL01;DATABASE=PowercoWMS;Trusted_Connection=yes"

def getSqlConnection(conn_string):
    return da.connect(conn_string)

def getSql(sql, conn_string,headers=None):
    cnxn = getSqlConnection(conn_string)
    data = pd.read_sql_query(sql,cnxn)
    if headers is not None:
        data.columns = headers
    return data


def reordCol(old_data):
    new_data = old_data[['TABLE', 'NAME', 'ALIAS', 'TYPE', 'DOMAIN', 'LENGTH',
                         'E/FLOC', 'ELEC/GAS', 'SAP',
                         'SAP Technical Object Description', 'SAP Field Name']]
    return new_data

def stripSQL(data):
    data['TABLE'] = data['TABLE'].where(
        data['TABLE'].str.contains("PCOGIS.SDE.")==False
        ,data['TABLE'].str.slice(start=11) #This flags a SettingWithCopyWarning
        )
    data = data.dropna(subset=['NAME'])
    bad_atts = [" ", "SHAPE_Length", "HVFUSES", "LVFUSES", "SHAPE_Area", "ACTUALLENGTH"
                , "DECOMMISSIONINGDATE", "DECOMMISSIONINGREASON"]
    data = data[data['NAME'].isin(bad_atts)==False]
    data = data[((data['TABLE'].str.contains('SWITCHUNIT')==True) & (
            data['NAME'].str.contains('INTERRUPTINGMEDIUM')==True)) == False]
    data = data[((data['TABLE'].str.contains('DistributionMain')==True) & (
            data['NAME'].str.contains('CROSSINGID')==True))==False]
    data = data[((data['TABLE'].str.contains('DistributionMain')==True) & (
            data['NAME'].str.contains('MOUNTINGTYPE')==True))==False]
    data = data[((data['TABLE'].str.contains('DistributionMain')==True) & (
            data['NAME'].str.contains('MOUNTINGPOSITION')==True))==False]
    data['TABLE'] = data['TABLE'].where(
            data['TABLE'].str.contains("Auxiliary Equipment")==False
            ,data['TABLE'].str.replace("Auxiliary Equipment", "AUXILLARYEQUIPMENT")
            )
    return data

def stripDouble(data):
    data = data[((data['TABLE'].str.contains('Regulator')==True) & (
            data['NAME'].str.contains('SUBTYPECD')==True) & (
            data['SAP'].str.contains('y')==True)) == False]
    data = data[((data['TABLE'].str.contains('RegulatorStation')==True) & (
            data['NAME'].str.contains('EQUIPMENTID')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('APPLICATION')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('ENTRY')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('FACILITYID')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('MANUFACTURER')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('MATERIAL')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('MODEL')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('SurfaceStructure')==True) & (
            data['NAME'].str.contains('STRUCTURESIZE')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('COMMSPOWERSUPPLY')==True) & (
            data['NAME'].str.contains('BATTERYAMPERAGEHOURS')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    data = data[((data['TABLE'].str.contains('COMMSPOWERSUPPLY')==True) & (
            data['NAME'].str.contains('BATTERYCOUNT')==True) & (
            data['SAP'].str.contains('N')==True)) == False]
    return data

def diffColumns(data):
    n_date = datetime.date.today().strftime("%d-%m-%y")
    if (data['SAP_x'] != data['SAP_y']) & (pd.notnull(data['SAP_x']) & pd.notnull(data['SAP_y'])):
        data['Date Changed'] = str(data['SAP_y']) + ' to ' + str(data['SAP_x']) + ' ' + n_date
        print(str(data['TABLE']) + ' ' + str(data['NAME']) + ' ' + data['Date Changed'])
    return data

def nullSQL(data):
    sql_cmd = """SELECT count(*) AS 'Count'
    FROM PCOGIS.SDE.""" + str(data['TABLE']) + """
    WHERE """ + str(data['NAME']) + " IS NULL"
    col = getSql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col

def nonBlankSQL(data):
    sql_cmd = "SELECT count(" + str(data['NAME']) + """) AS 'Count'
    FROM PCOGIS.SDE.""" + str(data['TABLE'])
    col = getSql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col


def objectidSQL(table_name):
    sql_cmd = """SELECT count(OBJECTID) AS 'Count'
    FROM PCOGIS.SDE.""" + str(table_name)
    col = getSql(sql_cmd, CONNECTION_GIS)
    new_col = col["count"].iloc[-1]
    return new_col

def desTableSQL():
    sql_cmd = """SELECT TABLE_, FIELD_NAME, 'Y' AS 'DOMAIN LOOKUP'
    FROM PCOGIS.SDE.DOMAIN_LOOKUP_PC"""
    col = getSql(sql_cmd, CONNECTION_GIS)
    return col


file_start = 'Data_Decisions_Summary-V'
file_end = '.xlsx'
located = 'P:\\NF\\Data Migration\\Data Decisions\\'
dest_location = 'P:\\NF\\Data Migration\\Data Decisions\\Archive\\'

file_list = os.listdir(located)
sum_list = [x for x in file_list if file_start in x and '~$' not in x]
vers_list = [x[len(file_start):-len(file_end)] for x in sum_list]
split_list = [x.split('.') for x in vers_list]
vers = max([int(x[0]) for x in split_list])
rev = max([int(x[1]) for x in split_list if str(vers) in x[0]])

infile = file_start + str(vers) + '.' + str(rev).zfill(2) + file_end
outfile = file_start + str(vers) + '.' + str(rev+1).zfill(2) + file_end

print("Loading the three excel files.")
# In this section load in the three excel files
elec_data = pd.read_excel(r"Electricity GIS-SAP Attribute Mapping.xlsx",
                          sheet_name="GIS Attributes")
gas_data = pd.read_excel("Gas_Network_Model_1.7.xlsx",
                         sheet_name="ArcFM Model - Features & Object",
                         skiprows=1)
old_eg_data = pd.read_excel(located + infile, sheet_name="GIS Data")

# Removes unwanted columns.
new_elec = elec_data[['GIS Table', 'GIS Column Name', 'ALIAS ', 'TYPE',
                      'DOMAIN', 'LENGTH', 'E/FLOC', 'SAP required']]
new_elec = new_elec.rename(columns={'GIS Table': 'TABLE',
                                    'GIS Column Name': 'NAME',
                                    'ALIAS ': 'ALIAS', 'SAP required': 'SAP'})
new_elec['ELEC/GAS'] = 'E'
new_elec['SAP Technical Object Description'] = np.nan
new_elec['SAP Field Name'] = np.nan
new_elec = reordCol(new_elec)


new_gas = gas_data[['OBJECTCLASSNAME', 'NAME2', 'FIELDALIAS', 'DOMAIN',
                    'FLOC - EQUIP', 'Migrated GIS to SAP ',
                    'SAP Technical Object Description',
                    'SAP Field Name']]
new_gas = new_gas.rename(columns={'OBJECTCLASSNAME': 'TABLE', 'NAME2': 'NAME',
                                  'FIELDALIAS': 'ALIAS',
                                  'FLOC - EQUIP': 'E/FLOC',
                                  'Migrated GIS to SAP ': 'SAP'})

# Adds new columns
new_gas['ELEC/GAS'] = 'G'
new_gas['TYPE'] = np.nan
new_gas['DOMAIN'] = np.nan
new_gas['LENGTH'] = np.nan
new_gas = reordCol(new_gas)

# new_old1 = old_eg_data[['sde.table','NAME', 'Incorrect Data']]
new_old1 = old_eg_data[['TABLE', 'NAME', 'Incorrect Data']]
# new_old1 = new_old1.rename(columns={'sde.table':'TABLE'})

# new_old2 = old_eg_data[['sde.table','NAME', 'Notes']]
new_old2 = old_eg_data[['TABLE', 'NAME', 'DR#', 'Notes']]
# new_old2 = new_old2.rename(columns={'sde.table':'TABLE'})


# Elec and gas concatonated array.
eg_data = pd.concat([new_elec, new_gas])

# Removes all instances of PCOGIS.SDE.
eg_data = stripSQL(eg_data)


domLook = desTableSQL()
domLook = domLook.rename(columns={'table_': 'TABLE', 'field_name': 'NAME',
                                  'domain lookup': 'DOMAIN LOOKUP'})
eg_data = pd.merge(eg_data, domLook, how='left', left_on=['TABLE', 'NAME'],
                   right_on=['TABLE', 'NAME'])
eg_data['DOMAIN LOOKUP'] = eg_data['DOMAIN LOOKUP'].fillna('N')
eg_data = eg_data.drop_duplicates(subset=['TABLE', 'NAME', 'SAP'])

new_sap = eg_data[['TABLE', 'NAME', 'SAP']]
new_sap = new_sap.drop_duplicates(subset=['TABLE', 'NAME', 'SAP'])
new_sap = stripDouble(new_sap)
old_sap = old_eg_data[['TABLE', 'NAME', 'SAP', 'Date Changed']]
old_sap = old_sap.drop_duplicates(subset=['TABLE', 'NAME', 'SAP'])
old_sap = stripDouble(old_sap)
dif_sap = pd.merge(new_sap, old_sap, how='left', left_on=['TABLE', 'NAME'],
                   right_on=['TABLE', 'NAME'])
dif_sap = dif_sap.apply(diffColumns, axis=1)
new_dif_sap = dif_sap[['TABLE', 'NAME', 'Date Changed']]

new_dif_sap = new_dif_sap.drop_duplicates(subset=['TABLE', 'NAME',
                                                  'Date Changed'])
eg_data = pd.merge(eg_data, new_dif_sap, how='left', left_on=['TABLE', 'NAME'],
                   right_on=['TABLE', 'NAME'])

# Create a schema with all table and column attributes
all_tabs_atts = """
SELECT C.Table_Catalog DB,C.Table_Schema, C.Table_Name, Column_Name, Data_Type, character_maximum_length, numeric_precision, datetime_precision
    FROM Information_Schema.Columns C JOIN Information_Schema.Tables T
    ON C.table_name = T.table_name
    WHERE Table_Type = 'BASE TABLE'
"""
# SELECT * --C.Table_Catalog DB,C.Table_Schema, C.Table_Name, Column_Name, Data_Type
all_schema = getSql(all_tabs_atts, CONNECTION_GIS)
all_schema['GIS - Limit/Precision'] = all_schema['character_maximum_length'].fillna(all_schema['numeric_precision']).fillna(all_schema['datetime_precision'])
stp_schema = all_schema[all_schema['table_schema'].str.contains('jde')==False]
new_all_schema = stp_schema[['table_name', 'column_name',
                             'data_type', 'GIS - Limit/Precision']]
new_all_schema = new_all_schema.rename(columns={'table_name': 'TABLE_up',
                                                'column_name': 'NAME_up',
                                                'data_type': 'GIS Type'})
new_all_schema['TABLE_up'] = new_all_schema['TABLE_up'].str.upper()
new_all_schema['NAME_up'] = new_all_schema['NAME_up'].str.upper()
eg_data['TABLE_up'] = eg_data['TABLE'].str.upper()
eg_data['NAME_up'] = eg_data['NAME'].str.upper()

eg_data = pd.merge(eg_data, new_all_schema, how='left',
                   left_on=['TABLE_up', 'NAME_up'],
                   right_on=['TABLE_up', 'NAME_up'])

eg_data = eg_data[['TABLE', 'NAME', 'ALIAS', 'TYPE', 'DOMAIN', 'LENGTH',
                   'E/FLOC', 'GIS Type', 'GIS - Limit/Precision',
                   'DOMAIN LOOKUP', 'ELEC/GAS', 'SAP', 'Date Changed',
                   'SAP Technical Object Description', 'SAP Field Name']]

print('Querying the number of nulls per attribute. As of '
      + str(datetime.datetime.now()))
eg_data['NULL'] = eg_data.apply(nullSQL, axis=1)
print('Querying the number of non-nulls per attribute. As of '
      + str(datetime.datetime.now()))
eg_data['Not-NULL'] = eg_data.apply(nonBlankSQL, axis=1)

new_old1 = stripSQL(new_old1)
new_old2 = stripSQL(new_old2)



# Acts like an SQL left join
new_old1 = new_old1.drop_duplicates(subset=['TABLE', 'NAME', 'Incorrect Data'])
fin_data = pd.merge(eg_data, new_old1, how='left', left_on=['TABLE', 'NAME'], right_on=['TABLE', 'NAME'])

print('Querying the total number of objects per table. As of ' + str(datetime.datetime.now()))
fin_data['Total'] = fin_data['TABLE'].apply(objectidSQL)

fin_data['% Not-NULL'] = np.nan
fin_data['% Complete'] = np.nan

#Acts like an SQL left join
new_old2 = new_old2.drop_duplicates(subset=['TABLE', 'NAME', 'DR#', 'Notes'])
fin_data = pd.merge(fin_data, new_old2, how='left', left_on=['TABLE', 'NAME'], right_on=['TABLE', 'NAME'])

fin_data = fin_data.drop_duplicates(subset=['TABLE','NAME','ALIAS','TYPE','DOMAIN','LENGTH',
                                 'E/FLOC','GIS Type','ELEC/GAS','SAP','DR#', 'Notes'])

fin_data = fin_data.sort_values(by=['TABLE', 'NAME'])


#Creates and adds data to excel file
excel_writer = pd.ExcelWriter(located + outfile, engine='xlsxwriter')
fin_data.to_excel(excel_writer, sheet_name='GIS Data', index=False  ,freeze_panes=(1,0))

names_dict = dict((v,k) for k,v in dict(enumerate(list(fin_data))).items())
num_cols = len(list(fin_data))
num_rows = len(fin_data)

workbook = excel_writer.book
worksheet = excel_writer.sheets['GIS Data']

worksheet.autofilter(0, 0, 0, num_cols-1)
worksheet.filter_column_list(names_dict['SAP'], ['Y'])

"""
#worksheet.freeze_panes(1, 0)

#notnull_range = xl_range(1,names_dict['Not-NULL'],num_rows,names_dict['Not-NULL'])
#incor_range = xl_range(1,names_dict['Incorrect Data'],num_rows,names_dict['Incorrect Data'])
#total_range = xl_range(1,names_dict['Total'],num_rows,names_dict['Total'])

#perc_nn_str = '{=' + notnull_range + '/' + total_range + '}'
#worksheet.write_array_formula(1,names_dict['% Not-NULL'],num_rows,names_dict['% Not-NULL'], perc_nn_str)

#perc_comp_str = '{=(' + notnull_range + '-' + incor_range + ')/' + total_range + '}'
#worksheet.write_array_formula(1,names_dict['% Complete'],num_rows,names_dict['% Complete'], perc_comp_str)
"""

notnull_let = xl_col_to_name(names_dict['Not-NULL'])
incor_let = xl_col_to_name(names_dict['Incorrect Data'])
total_let = xl_col_to_name(names_dict['Total'])

for i in range(1, num_rows):
    nn_form_str = '=' + notnull_let + str(i+1) + '/' + total_let + str(i+1)
    comp_form_str = '=(' + notnull_let + str(i+1) + '-' + incor_let + str(i+1) + ')/' + total_let + str(i+1)
    worksheet.write_formula(i, names_dict['% Not-NULL'], nn_form_str)
    worksheet.write_formula(i, names_dict['% Complete'], comp_form_str)


perc_format = workbook.add_format({'num_format': '0%'})
bg_red = workbook.add_format({'bg_color': '#FF8080'})


worksheet.set_column(names_dict['% Not-NULL'], names_dict['% Complete'], None,
                     perc_format)

worksheet.conditional_format(1, names_dict['% Complete'],
                             num_rows, names_dict['% Complete'],
                             {'type': 'cell',
                              'criteria': '<=',
                              'value': 0.5,
                              'format': bg_red})

excel_writer.save()

print('Ending ' + str(datetime.datetime.now()))
print(
      """--------------------------
      Share Workbook
--------------------------"""
      )

os.rename(located + infile, dest_location + infile)
