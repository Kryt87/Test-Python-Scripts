# -*- coding: utf-8 -*-
"""
Created on Mon Jul  2 10:24:42 2018

@author: KLD300
"""

import pandas as pd

E_EXCEL = 'Elec FLOC Structure and Details -MASTER.xlsx'
E_HI = 'Technical Object Hierarchy'
E_DE = 'Technical Object Details'
E_IN = 'SAP - GIS Integration Details'


def na_floc_filler(data):
    """This fills in the gaps in floc columns."""
    f2_list = list(data['Floc L2\nAsset Class'].dropna())
    data['Floc L2\nAsset Class'] = data[
        'Floc L2\nAsset Class'].fillna(method='ffill')

    for f2_val in f2_list:
        temp_f2 = data[data['Floc L2\nAsset Class'] == f2_val]
        temp_f2['Floc L3\nGroup'] = temp_f2[
            'Floc L3\nGroup'].fillna(method='ffill')
        data['Floc L3\nGroup'] = data[
            'Floc L3\nGroup'].fillna(temp_f2['Floc L3\nGroup'])
        f3_list = list(data['Floc L3\nGroup'].dropna())

        for f3_val in f3_list:
            temp_f3 = temp_f2[temp_f2['Floc L3\nGroup'] == f3_val]
            temp_f3['Floc L4\nFloc instance'] = temp_f3[
                'Floc L4\nFloc instance'].fillna(method='ffill')
            data['Floc L4\nFloc instance'] = data[
                'Floc L4\nFloc instance'].fillna(temp_f3[
                    'Floc L4\nFloc instance'])

    return data


def file_loading():
    """Loads excel sheets into dataframes."""
    e_hi = pd.read_excel(E_EXCEL, sheet_name=E_HI, skiprows=1).iloc[8:]
    e_de = pd.read_excel(E_EXCEL, sheet_name=E_DE, skiprows=1).iloc[1:]
    e_in = pd.read_excel(E_EXCEL, sheet_name=E_IN).dropna(
        subset=['Object Type'])

    e_hi.reset_index(drop=True, inplace=True)
    e_hi['Order'] = e_hi.index.values + 1

    e_hi = na_floc_filler(e_hi)

    e_hi = e_hi[['Order',
                 'Floc L2\nAsset Class',
                 'Floc L3\nGroup',
                 'Floc L4\nFloc instance',
                 'Equipment',
                 'Sub Equipment',
                 'Technical Object',
                 'Object Type',
                 'Object Type Description']]

    e_de = e_de[['Technical Object',
                 'Object Type',
                 'Equipment Category'
                 + ' (R = Rotable M = Maintenance Equipment)\n',
                 'Financial Asset',
                 'Notes']]

    e_in = e_in[['Technical Object',
                 'Object Type',
                 'GIS Feature / Object Class',
                 'GIS Class Name',
                 'SAP filter',
                 'GIS Filter Required',
                 'GIS Filter',
                 'Geometry type',
                 'Notes']]

    return e_hi, e_de, e_in


def data_merger(data1, data2, data3):
    """Merges dataframes."""
    data1_2 = pd.merge(data1, data2, how='outer',
                       left_on=['Technical Object', 'Object Type'],
                       right_on=['Technical Object', 'Object Type'])

    all_data = pd.merge(data1_2, data3, how='outer',
                        left_on=['Technical Object', 'Object Type'],
                        right_on=['Technical Object', 'Object Type'])

    return all_data


def fin_dataframe_structure(data):
    """Reorders and renames the final table."""
    data = data.rename(columns={'Floc L2\nAsset Class': 'FLOC2 - Asset Class',
                                'Floc L3\nGroup': 'FLOC3 - Group',
                                'Floc L4\nFloc instance': 'FLOC4 - Instance',
                                'Equipment Category (R = Rotable M = Mainten'
                                + 'ance Equipment)\n': 'Rotable/Maintenance',
                                'Notes_x': 'Object Notes',
                                'Notes_y': 'Integration Notes'})
    data = data[['Order',
                 'Technical Object',
                 'FLOC2 - Asset Class',
                 'FLOC3 - Group',
                 'FLOC4 - Instance',
                 'Equipment',
                 'Sub Equipment',
                 'Object Type',
                 'Object Type Description',
                 'GIS Class Name',
                 'GIS Filter Required',
                 'GIS Filter',
                 'SAP filter',
                 'Object Notes',
                 'Integration Notes',
                 'GIS Feature / Object Class',
                 'Geometry type',
                 'Rotable/Maintenance',
                 'Financial Asset']]
    data = data.sort_values(by=['Order'])
    return data


def excel_exporter(data1):
    """Exports to a new excel file."""
    excel_writer = pd.ExcelWriter('FLOC-Equipment_Summary.xlsx',
                                  engine='xlsxwriter')
    data1.to_excel(excel_writer, sheet_name='Elec FLOC Data',
                   index=False, freeze_panes=(1, 0))

    num_cols = len(list(data1))
    worksheet1 = excel_writer.sheets['Elec FLOC Data']
    worksheet1.autofilter(0, 0, 0, num_cols-1)

    names_dict = dict((v, k) for k, v in dict(enumerate(list(data1))).items())

    col_lengths = [['Order', 7.22],
                   ['Technical Object', 16.44],
                   ['FLOC2 - Asset Class', 18.33],
                   ['FLOC3 - Group', 30.89],
                   ['FLOC4 - Instance', 31.11],
                   ['Equipment', 31.11],
                   ['Sub Equipment', 16.78],
                   ['Object Type', 13.56],
                   ['Object Type Description', 30.89],
                   ['GIS Class Name', 22.22],
                   ['GIS Filter Required', 17.89],
                   ['GIS Filter', 32.56],
                   ['SAP filter', 89.78],
                   ['GIS Feature / Object Class', 5.89],
                   ['Rotable/Maintenance', 8.11],
                   ['Financial Asset', 14.78]]

    for col_name, col_val in col_lengths:
        colnum = names_dict[col_name]
        worksheet1.set_column(colnum, colnum, col_val)

    excel_writer.save()


def floc_cat_runner():
    """Runs this script."""
    e_hi, e_de, e_in = file_loading()
    e_all = data_merger(e_hi, e_de, e_in)
    fin_e_all = fin_dataframe_structure(e_all)
    excel_exporter(fin_e_all)


floc_cat_runner()
