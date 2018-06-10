# -*- coding: utf-8 -*-
"""
Created on Wed May 30 09:04:08 2018
@author: KLD300
"""


import datetime
import urllib
import pandas as pd
from bs4 import BeautifulSoup


JDETABLES_URL = r'http://jdetables.com/'
START_HREF = r'?schema=812'


def get_soup(url):
    """Use BeautifulSoup to extract html from a url."""
    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html, "lxml")
    return soup


def get_table_header(table):
    """Returns the table headers."""
    th_tags = []
    col_nam = []
    for row in table.find_all('tr'):
        th_tag = row.find_all('th')
        if th_tag:
            if not th_tags:
                th_tags = th_tag
            # elif th_tag != th_tags:
                # print("Error! There is another set of headors!")
                # print(th_tag)
    for th_tag in th_tags:
        header = th_tag.get_text()
        if header:
            col_nam.append(header)
        else:
            col_nam.append('Id')
    return col_nam


def get_table_rows(table):
    """Strips out useful rows."""
    td_tags = []
    td_txt = []
    hlink_boo = False
    col_nam = get_table_header(table)
    num_col = len(col_nam)
    for row in table.find_all('tr'):
        td_tag = row.find_all('td')
        if len(td_tag) == num_col:
            td_tags.append(td_tag)
            hlink = row.find('a')
            col_txt = []
            for td_val in td_tag:
                col_txt.append(td_val.get_text())
            if hlink:
                col_txt.append(hlink.attrs['href'])
                hlink_boo = True
            td_txt.append(col_txt)
    if hlink_boo:
        col_nam.append('href')
    return col_nam, td_txt


def create_df(table):
    """Uses BeautifulSoup to create a dataframe."""
    cols, rows = get_table_rows(table)

    df_table = pd.DataFrame(columns=cols, data=rows)
    return df_table


def get_tables(soup):
    """Gets all table information from BeautifulSoup."""
    dfs = []
    for table in soup.find_all('table'):
        # print('----------------New Table----------------')
        dfs.append(create_df(table))
    return dfs


def jde_crawler(data):
    """Crawls through all hrefs for dataframes."""
    df_list = []
    if data['href']:
        soup = get_soup(JDETABLES_URL + data['href'])
        t_dfs = get_tables(soup)
        df_temp = t_dfs[4]
        df_temp['Table'] = data['Table']
        df_list.append(df_temp)
        # print('------------------------------')
        print(data['Id'] + ' ' + data['Table'])
        # print('------------------------------')
    return df_list


def get_jde_df():
    """Creates a master dataframe for all JDE tables and fields."""
    bs_jde = get_soup(JDETABLES_URL + START_HREF)
    jde_tables = get_tables(bs_jde)

    table_data = jde_tables[3]

    # col_data = table_data.apply(jde_crawler, axis=1)
    list_data = table_data.apply(jde_crawler, axis=1)
    field_data = list_data[0][0]
    for num in range(1, len(list_data)):
        field_data = pd.concat([field_data, list_data[num][0]],
                               ignore_index=True)

    fin_data = pd.merge(table_data, field_data, how='left', left_on=['Table'],
                        right_on=['Table'])
    return fin_data


def excel_printer(data, outfile_name):
    """Creates an excel file from a dataframe."""
    excel_writer = pd.ExcelWriter(outfile_name, engine='xlsxwriter')
    data.to_excel(excel_writer, sheet_name='JDE Data',
                  index=False, freeze_panes=(1, 0))
    excel_writer.save()


def run_script():
    """Run this webscraper."""
    fin_data = get_jde_df()
    out = 'JDE_names.xlsx'
    excel_printer(fin_data, out)


START_TIME = str(datetime.datetime.now())
print('Starting ' + START_TIME)
run_script()
print('Starting ' + START_TIME)
END_TIME = str(datetime.datetime.now())
print('Ending ' + END_TIME)
