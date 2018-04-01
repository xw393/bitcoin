# -*- coding: utf-8 -*-
# @Author: infinitecycle
# @Date:   2018-01-23 11:28:48
# @Last Modified by:   infinitecycle
# @Last Modified time: 2018-04-01 00:28:20


import pandas as pd
import arrow
import datetime as dt
import sys, os


# path on Linux-server
path = '/home/infinitecycle/Desktop/GoogleDrive/strats_test/bitcoin/'

# path on Local (Mac)
# path = '/Users/XiaoWang/Desktop/GoogleDrive/strats_test/bitcoin/'
sys.path.append(os.path.abspath(path))
from utilities import connectDB, createEngine

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
    # con_prod = connectDB('server')
    con_prod = connectDB('local')
    con_prod.autocommit = True
    cursor = con_prod.cursor()

    btc_last24_sql = """
    SELECT *
    FROM dev.btc_trading_vol
    WHERE time_point >= ((CURRENT_TIMESTAMP - interval '24 hours') AT TIME ZONE 'EST')
    """

    btc_last24_data = pd.read_sql(con=con_prod, sql=btc_last24_sql)

    col_with_missing_val = btc_last24_data.columns[btc_last24_data.isnull().any()].tolist()

    if len(col_with_missing_val) > 0:
        print("# get last 24-hour's data to fill missing value.")
        url = 'https://data.bitcoinity.org/export_data.csv?c=e&data_type=volume&r=minute&t=b&timespan=24h'
        btc_trading_vol = pd.read_csv(url)

        btc_trading_vol = btc_trading_vol.fillna(0) # Fill missing data with 0.
        btc_trading_vol.loc[:, 'Time'] = btc_trading_vol.Time.map(lambda x: get_est_time(x))

        try:
            print("Insert data into temp2 table.")
            # engine = createEngine('server')
            engine = createEngine('local')
            btc_trading_vol.to_sql(con=engine, name = 'btc_trading_vol_temp2',
                                   schema = 'dev', if_exists = 'replace', index = False)

            for item in col_with_missing_val:
                update_sql = """
                    UPDATE dev.btc_trading_vol a
                    SET {col_final} = b."{col_raw}"
                    FROM dev.btc_trading_vol_temp2 b
                    WHERE a.{col_final} IS NULL AND
                    a.time_point::TEXT = b."Time"
                    """.format(col_final = item.replace('-', ''),
                               col_raw = item)
                try:
                    cursor.execute(update_sql)
                    print("Filling missing value for {xchange} completed.".format(xchange = item))
                except Exception as err:
                    print("Catch the error: {error}".format(error=repr(err)))
        except Exception as err2:
            print("Catch the error: {error}".format(error=repr(err2)))
    else:
        print("There is no missing value to be filled.")
