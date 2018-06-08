# -*- coding: utf-8 -*-
"""
Created on Wed May 30 09:04:08 2018

@author: KLD300
"""
import urllib
import pandas as pd
from bs4 import BeautifulSoup

JDETABLES_URL = r'http://jdetables.com/?schema=812'


def get_soup(url):
    """Use BeautifulSoup to extract html from a url."""
    html = urllib.request.urlopen(url)
    bs = BeautifulSoup(html, "lxml")
    return bs


def get_table_header(table):
    """Returns the table headers."""
    th_tags = []
    col_nam = []
    for row in table.find_all('tr'):
        th_tag = row.find_all('th')
        if th_tag:
            if not th_tags:
                th_tags = th_tag
            elif th_tag != th_tags:
                print("Error! There is another set of headors!")
                print(th_tag)
    for th in th_tags:
        header = th.get_text()
        if header:
            col_nam.append(header)
        else:
            col_nam.append('Id')
    return col_nam


def get_table_rows(table, num_row):
    """Strips out useful"""
    td_tags = []
    for row in table.find_all('tr'):
        td_tag = row.find_all('td')
        if len(td_tag) == num_row:
            td_tags.append(td_tag)
    return td_tags


def create_df(table):
    """Uses BeautifulSoup to create a dataframe."""

    num_row = 0
    col_nam = get_table_header(table)
    num_col = len(col_nam)
    all_rows = get_table_rows(table, num_row)


    if len(col_nam) > 0 and len(col_nam) != num_col:
        raise Exception("Column names don't match the number of columns.")

    cols = col_nam if len(col_nam) > 0 else range(0, num_col)
    df = pd.DataFrame(columns=cols,  index=range(0, num_row))
    print(df)


def get_tables(soup):
    """Gets all table information from BeautifulSoup."""
    for table in soup.find_all('table'):
        print('----------------New Table----------------')
        create_df(table)


bs_jde = get_soup(JDETABLES_URL)

get_tables(bs_jde)
