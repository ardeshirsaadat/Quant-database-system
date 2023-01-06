import os
import json
import aiohttp
import requests
import datetime
import calendar
import numpy as np
import pandas as pd
import urllib
import urllib.parse

from dateutil.relativedelta import relativedelta

import wrappers.aiohttp_wrapper as aiohttp_wrapper

def get_fundamental_data(eod_client, ticker, exchange="US"):
    return eod_client.get_fundamental_equity("{}.{}".format(ticker, exchange))
       
def get_ohlcv(ticker="", exchange="US", period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    url = "https://eodhistoricaldata.com/api/eod/{}.{}".format(ticker, exchange)
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    resp = requests.get(url, params=params)
    df = pd.DataFrame(resp.json())
    df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
    if len(df) > 0:
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df

async def asyn_get_ohlcv(ticker="", exchange="US", period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    url = "https://eodhistoricaldata.com/api/eod/{}.{}".format(ticker, exchange)
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url=url, params=params) as resp:
            df = pd.DataFrame((await resp.json()))
            df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
            if len(df) > 0:
                df["datetime"] = pd.to_datetime(df["datetime"])
            return df

async def asyn_batch_get_ohlcv(tickers, exchanges, period_end=datetime.datetime.today(), period_start=None, period_days=3650):
    urls = []
    params = {
        "api_token": os.getenv('EOD_KEY'), 
        "fmt": "json",
        "from": period_start.strftime('%Y-%m-%d') if period_start else (period_end - datetime.timedelta(days=period_days)).strftime('%Y-%m-%d'),
        "to": period_end.strftime('%Y-%m-%d')
    }
    results = []
    for ticker, exchange in zip(tickers, exchanges):
        url = "https://eodhistoricaldata.com/api/eod/{}.{}?".format(ticker, exchange)
        urls.append(url + urllib.parse.urlencode(params))
    results = await aiohttp_wrapper.async_aiohttp_get_all(urls)
    dfs = []
    for result in results:
        if result is None:
            dfs.append(result)
            continue
        df = pd.DataFrame(result)
        df.rename(columns={"date": "datetime", "adjusted_close": "adj_close"}, inplace=True)
        if len(df) > 0:
            df["datetime"] = pd.to_datetime(df["datetime"])
        dfs.append(df)
    return dfs
