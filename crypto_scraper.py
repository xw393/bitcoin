# -*- coding: utf-8 -*-
# @Author: infinitecycle
# @Date:   2017-09-17 21:08:45
# @Last Modified by:   infinitecycle
# @Last Modified time: 2018-04-01 00:26:31

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import datetime as dt
import arrow
import sys, os

path = "/home/infinitecycle/Desktop/GoogleDrive/strats_test/"
sys.path.append(os.path.abspath(path))
from utilities import connectDB, createEngine

def convert_date(date_str):
    try:
        std_date = dt.datetime.strftime(arrow.get(date_str, 'MMM DD, YYYY').date(), '%Y-%m-%d')
    except:
        std_date = date_str

    return(std_date)

def parse_price_table(coin_name, url):
    try:
        url_resp = requests.get(url)
        if url_resp.status_code == 200:
            print(">>> Get response ({coin_name}) from website successfully.".format(coin_name=coin_name))

            url_text = BeautifulSoup(url_resp.text, 'lxml')
            trade_table = url_text.findAll('tr')
            col_name = [item.contents[0].lower().replace(' ', '_') for item in trade_table[0].findAll('th')]

            data = [[item.contents[0] for item in trade_table[i].findAll('td')] for i in range(1, len(trade_table))]
            data_list = [dict(zip(col_name, data[i])) for i in range(len(data))]
            data_df = pd.DataFrame(data_list)

            # ORganize the table structure and set data format.
            data_df['coin_name'] = coin_name.upper()
            data_df['date'] = data_df.date.map(lambda x: convert_date(x))

            return(data_df)
        else:
            print(">>> URL ({coin_name}) status code is {status_code}\n".format(status_code=url_resp.status_code, coin_name=coin_name))
            return(url_resp.status_code)
    except Exception as err:
        print(">>> Catch the error: {error}\n".format(error=err))
        return(0)


def get_last_trade_date(coin_name, trade_date_df):
    """ Get the latest trade date of a coin from database.

    Param:
    ------
    coin_name: a string
    trade_date_df: a DataFrame, which contains coin_name and trade_date columns (latest trade date.)

    Return:
    start_date: a date string, one day after the last trade date of the specified coin in the database.
    """
    coin_max_trade_date = trade_date_df.trade_date[trade_date_df.coin_name.str.lower() == coin_name.lower()].item()
    start_date = (arrow.get(coin_max_trade_date, 'YYYY-MM-DD').date() + dt.timedelta(days=1)).strftime('%Y%m%d')
    return(start_date)

def insert_row(row):
    insert_sql = """
    insert into dev.crypto_price (coin_name, date, open, close, high, low, volume, market_cap)
    values ( '{coin_name}', '{date}', '{open}', '{close}', '{high}', '{low}', '{volume}', '{market_cap}');
    """.format(coin_name=row.coin_name,
               date=row.date,
               open=row.open,
               close=row.close,
               high=row.high,
               low=row.low,
               volume=row.volume,
               market_cap=row.market_cap)
    try:
        cursor.execute(insert_sql)
        print(">>> Done.")
    except Exception as err:
        print(">>> Catch the error: {error}".format(error=err))

def get_coin_price(coin_name, freq='daily'):
    """get coin price from website.

    Params:
    -------
    coin_name: a string, the crypto currency name.
    freq: a string, could only be either 'daily' or 'all'.
        'daily': download the latest price information.
        'all': download the whole historical price information

    Return:
    -------
    The information will be inserted into database directly.
    """
    if freq == 'daily':
        start_date = get_last_trade_date(coin_name, trade_date_df=max_trade_date)
        end_date = dt.datetime.strftime(dt.datetime.today(), '%Y%m%d')
        url = "https://coinmarketcap.com/currencies/{coin_name}/historical-data/?start={start_date}&end={end_date}".format(coin_name=coin_name, start_date=start_date, end_date=end_date)

        try:
            coin_price_df = parse_price_table(coin_name, url)
            if isinstance(coin_price_df, pd.DataFrame):
                print(">>> Get the price data for ({coin_name}) successfully.".format(coin_name=coin_name))
                # engine = createEngine('server')
                # coin_price_df.to_sql(con=engine, schema='dev', name='crypto_price', if_exists='append', index=False)
                # engine.dispose()

                # Ensure the close price will not be null.
                coin_price_df = coin_price_df[~coin_price_df.close.isnull()]
                coin_price_df.apply(lambda row: insert_row(row), axis = 1)
                print(">>> Insert data successfully\n")
                # return(coin_price_df)
        except Exception as err_parse:
            print(">>> Catch the error: {error}".format(error=err_parse))

    elif freq == 'all':
        start_date = '20130428'
        end_date = dt.datetime.strftime(dt.datetime.today(), '%Y%m%d')
        url = "https://coinmarketcap.com/currencies/{coin_name}/historical-data/?start={start_date}&end={end_date}".format(coin_name=coin_name, start_date=start_date, end_date=end_date)

        try:
            coin_price_df = parse_price_table(coin_name, url)
            if isinstance(coin_price_df, pd.DataFrame):
                print(">>> Get the price data for ({coin_name}) successfully.".format(coin_name=coin_name))
                # engine=createEngine('server')
                engine=createEngine('local')
                coin_price_df.to_sql(con=engine, schema='dev', name='crypto_price', if_exists='append', index=False)
                engine.dispose()
                print(">>> Insert data successfully\n")
                # return(coin_price_df)
        except Exception as err_parse:
            print(">>> Catch the error: {error}".format(error=err_parse))
    else:
        raise ValueError(">>> freq must be either 'daily' or 'all'")

if __name__ == '__main__':
    # coin_name = 'bitcoin'

    # coin_list = ['bitcoin', 'ethereum', 'bitcoin-cash',
    #          'ripple', 'litecoin', 'dash', 'nem', 'monero',
    #          'iota', 'ethereum-classic']
    coin_rank_200 = pd.read_csv(path+'bitcoin/coin_rank_200.csv')
    coin_list = coin_rank_200.coin_name.tolist()

    freq = sys.argv[1]

    if freq == 'daily':
        max_trade_date_sql = """
            SELECT coin_name,
                   max(date)::varchar(10) trade_date
            FROM dev.crypto_price
            GROUP BY coin_name
        """

        # con_prod = connectDB('server')
        con_prod = connectDB('local')
        con_prod.autocommit = True
        cursor = con_prod.cursor()

        max_trade_date = pd.read_sql(con=con_prod, sql=max_trade_date_sql)
        if isinstance(max_trade_date, pd.DataFrame):
            print(">>> Get max trade date successfully.")

            for coin_name in coin_list:
                get_coin_price(coin_name=coin_name, freq=freq)
        else:
            print(">>> Cannot get max trade date.")

        con_prod.close()
    elif freq == 'all':
        for coin_name in coin_list:
            get_coin_price(coin_name=coin_name, freq=freq)
    else:
        raise ValueError("'freq' could only be 'daily' or 'all'.")

    print(">>> Done.")