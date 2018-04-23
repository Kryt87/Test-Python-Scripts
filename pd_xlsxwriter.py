# -*- coding: utf-8 -*-
"""
Created on Sat Apr 21 14:02:38 2018

@author: KurtDrew
"""

import pandas as pd

import os
#from xlsxwriter.utility import xl_range
from xlsxwriter.utility import xl_col_to_name #New!


file_start = 'Data_Decisions_Summary-V'
file_end = '.xlsx'

file_list = os.listdir()
sum_list = [x for x in file_list if file_start in x and '~$' not in x] # NEW!
vers_list = [x[len(file_start):-len(file_end)] for x in sum_list]
split_list = [x.split('.') for x in vers_list]
vers = max([int(x[0]) for x in split_list])
rev = max([int(x[1]) for x in split_list if str(vers) in x[0]])

infile = file_start + str(vers) + '.' + str(rev).zfill(2) + file_end
outfile = file_start + str(vers) + '.' + str(rev+1).zfill(2) + file_end






fin_data = pd.read_excel(infile, sheet_name="All Data")


fin_data = fin_data.sort_values(by=['TABLE', 'NAME']) #New!



names_dict = dict((v,k) for k,v in dict(enumerate(list(fin_data))).items())
num_cols = len(list(fin_data))
num_rows = len(fin_data)







excel_writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
fin_data.to_excel(excel_writer, sheet_name='All Data', index=False ,freeze_panes=(1,0))


workbook  = excel_writer.book
worksheet = excel_writer.sheets['All Data']

worksheet.autofilter(0,0,0,num_cols-1)
worksheet.filter_column_list(names_dict['SAP'], ['Y'])
#worksheet.freeze_panes(1, 0)


#notnull_range = xl_range(1,names_dict['Not-NULL'],num_rows,names_dict['Not-NULL'])
#incor_range = xl_range(1,names_dict['Incorrect Data'],num_rows,names_dict['Incorrect Data'])
#total_range = xl_range(1,names_dict['OBJECTID Total'],num_rows,names_dict['OBJECTID Total'])

#perc_nn_str = '{=' + notnull_range + '/' + total_range + '}'
#worksheet.write_array_formula(1,names_dict['% Not-NULL'],num_rows,names_dict['% Not-NULL'], perc_nn_str)

#perc_comp_str = '{=(' + notnull_range + '-' + incor_range + ')/' + total_range + '}'
#worksheet.write_array_formula(1,names_dict['% Complete'],num_rows,names_dict['% Complete'], perc_comp_str)
##########NEW From here!##########
notnull_let = xl_col_to_name(names_dict['Not-NULL'])
incor_let = xl_col_to_name(names_dict['Incorrect Data'])
total_let = xl_col_to_name(names_dict['OBJECTID Total'])

for i in range(1, num_rows):
    nn_form_str = '=' + notnull_let + str(i+1) + '/' + total_let + str(i+1)
    comp_form_str = '=(' + notnull_let + str(i+1) + '-' + incor_let + str(i+1) + ')/' + total_let + str(i+1)
    worksheet.write_formula(i,names_dict['% Not-NULL'],nn_form_str)
    worksheet.write_formula(i,names_dict['% Complete'],comp_form_str)


perc_format = workbook.add_format({'num_format': '0%'})
bg_red = workbook.add_format({'bg_color': '#FF8080'})

worksheet.set_column(names_dict['% Not-NULL'],names_dict['% Complete'],None,perc_format)

worksheet.conditional_format(1,names_dict['% Complete'],num_rows,names_dict['% Complete'],{'type':'cell',
                             'criteria': '<=',
                             'value': 0.5,
                             'format':bg_red})

excel_writer.save()