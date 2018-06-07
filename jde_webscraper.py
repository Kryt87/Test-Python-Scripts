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


def create_df(table):
    """Uses BeautifulSoup to create a dataframe."""

    num_col = 0
    num_row = 0
    col_nam = []

    for row in table.find_all('tr'):
        td_tags = row.find_all('td')
        if len(td_tags) > 0:
            num_row += 1
            if num_col == 0:
                num_col = len(td_tags)

        th_tags = row.find_all('th')
        if len(th_tags) > 0 and len(col_nam) == 0:
            for th in th_tags:
                col_nam.append(th.get_text())

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
