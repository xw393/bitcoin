# -*- coding: utf-8 -*-
# @Author: XiaoWang
# @Date:   2018-01-19 01:01:35
# @Last Modified by:   infinitecycle
# @Last Modified time: 2018-04-01 00:27:39


import pandas as pd
import time
import arrow
import datetime as dt
import sys, os
from logbook import FileHandler, StreamHandler, Logger


# path on Linux-server
path = '/home/infinitecycle/Desktop/GoogleDrive/strats_test/bitcoin/'

# path on Local (Mac)
# path = '/Users/XiaoWang/Desktop/GoogleDrive/strats_test/bitcoin/'
sys.path.append(os.path.abspath(path))
from utilities import connectDB, createEngine



def make_logger(name, file_name, path):
    log_file_addr = path + file_name
    new_logger = Logger(name)
    new_logger.handlers.append(StreamHandler(sys.stdout, bubble=True))
    new_logger.handlers.append(FileHandler(log_file_addr, bubble=True, mode='w'))

    return new_logger

def get_est_time(date_str):
    """
    Convert a UTC timestamp string into EST timestamp

    Params:
        date_str: timestamp string, like '2018-01-18 05:03:00 UTC'
    Returns:
        est_time: timestamp with time zone as EST.
    """
    utc_time = arrow.get(date_str, 'YYYY-MM-DD hh:mm:ss')
    est_time = utc_time - dt.timedelta(hours = 5)
    est_time = est_time.strftime(format='%Y-%m-%d %H:%M:%S')
    return est_time


if __name__ == '__main__':
    # make a logger to keep record.
    file_name = 'logger_btc_vol'
    log_path = path + 'btc_vol/'
    logger_btc_vol = make_logger('BTC Volume', file_name, log_path)

    url = 'https://data.bitcoinity.org/export_data.csv?c=e&data_type=volume&r=minute&t=b&timespan=1h'

    # Top - 9 exchanges list.
    xchange_list = ['bit-x', 'bitfinex', 'bitflyer',
                    'bitstamp', 'coinbase', 'gemini', 'hitbtc',
                    'kraken', 'itbit','others']

    logger_btc_vol.info("Start to parse data from website.")
    try:
        btc_trading_vol = pd.read_csv(url)
    except pd.errors.ParserError:
        # If there is a ParseError, sleep 5 seconds and parse it again.
        time.sleep(5)
        btc_trading_vol = pd.read_csv(url)

    if isinstance(btc_trading_vol, pd.DataFrame) and btc_trading_vol.shape[0] > 1:

        btc_trading_vol = btc_trading_vol[:-20] # remove most recent 20 data points.
        btc_trading_vol = btc_trading_vol.fillna(0) # Fill missing data with 0.
        btc_trading_vol.loc[:, 'Time'] = btc_trading_vol.Time.map(lambda x: get_est_time(x))

        current_xchange = btc_trading_vol.columns.tolist()

        # Find small exchanges.
        other_xchange = [xchange for xchange in current_xchange
                                    if xchange not in xchange_list and xchange != 'Time']

        if len(other_xchange) > 0:
            # add all the other volume into 'others' category.
            btc_trading_vol['others'] = btc_trading_vol[other_xchange + ['others']].sum(axis = 1)
            # drop the combined columns (other exchanges)
            btc_trading_vol = btc_trading_vol.drop(other_xchange, axis = 1)

        logger_btc_vol.info('Start loading data into the database.')

        # engine = createEngine('server')
        engine = createEngine('local')
        btc_trading_vol.to_sql(con=engine, name='btc_trading_vol_temp', schema='dev',
                               if_exists='append', index=False)
        engine.dispose()
        logger_btc_vol.info('Data is inserted.')
    else:
        logger_btc_vol.info("Unable to acquire data.")


    # con_prod = connectDB('server')
    con_prod = connectDB('local')
    con_prod.autocommit = True
    cursor = con_prod.cursor()

    insert_sql = """
        /*
        Only keep data points of recent two days in the temp table.
        */
        DELETE
        FROM dev.btc_trading_vol_temp
        WHERE "Time"::date < CURRENT_DATE - interval '2 days';

        /*
        Insert most recent data into final table.
        */

        WITH t0 AS (
        SELECT "Time"::TIMESTAMP WITHOUT time ZONE AS time_point,
            "bit-x" bitx,
            "bitfinex" bitfinex,
            "bitflyer" bitflyer,
            "bitstamp" bitstamp,
            "coinbase" coinbase,
            "gemini" gemini,
            "hitbtc" hitbtc,
            "itbit" itbit,
            "kraken" kraken,
            "others" others
        FROM dev.btc_trading_vol_temp
        )

        INSERT INTO dev.btc_trading_vol
        SELECT *
        FROM t0
        WHERE time_point NOT IN
            (SELECT DISTINCT time_point
             FROM dev.btc_trading_vol);
        """

    try:
        cursor.execute(insert_sql)
        logger_btc_vol.info("Updating BTC trading volume data completed.")
    except Exception as err:
        logger_btc_vol.info(">>> Catch the error {error}".format(error = repr(err)))

    con_prod.close()

