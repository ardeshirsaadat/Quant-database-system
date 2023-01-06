import os
import sys
import json
import datetime
import asyncio

import pandas as pd
import oandapyV20 as Oanda

from fredapi import Fred
from eod import EodHistoricalData

import db_logs
import securities.fx as fx
import securities.misc as misc
import securities.macro as macro
import securities.crypto as crypto
import securities.baskets as baskets
import securities.options as options
import securities.equities as equities
import securities.commodities as commodities
import securities.fixed_income as fixed_income
import db.db_service as db_service

class DataMaster:

    def __init__(self, config_file_path="config.json"):
        with open(config_file_path, "r") as f:
            config = json.load(f)
            os.environ['EOD_KEY'] = config["eod_key"]
            os.environ['FRED_KEY'] = config["fred_key"]
            os.environ['MONGO_DB'] = config["mongo_db"]
            os.environ['MONGO_USER'] = config["mongo_user"]
            os.environ['MONGO_PW'] = config["mongo_pw"]
            os.environ['MONGO_CLUSTER'] = config["mongo_cluster"]
            os.environ['OAN_ID'] = config["oan_acc_id"]
            os.environ['OAN_TOKEN'] = config["oan_token"]
            os.environ['OAN_ENV'] = config["oan_env"]

        self.eod_client = EodHistoricalData(os.getenv('EOD_KEY'))
        self.fred_client = Fred(api_key=os.getenv("FRED_KEY"))
        self.oanda_client = Oanda.API(access_token=os.environ['OAN_TOKEN'], environment=os.environ['OAN_ENV'])
        self.data_clients = {
            "eod_client": self.eod_client,
            "fred_client": self.fred_client,
            "oanda_client": self.oanda_client
        }
        self.db_service = db_service.DbService()
        self.fx = fx.FX(data_clients=self.data_clients, db_service=self.db_service)
        self.misc = misc.Miscellaneous(data_clients=self.data_clients, db_service=self.db_service)
        self.macro = macro.Macro(data_clients=self.data_clients, db_service=self.db_service)
        self.crypto = crypto.Crypto(data_clients=self.data_clients, db_service=self.db_service)
        self.commodities = commodities.Commodities(data_clients=self.data_clients, db_service=self.db_service)
        self.baskets = baskets.Baskets(data_clients=self.data_clients, db_service=self.db_service)
        self.equities = equities.Equities(data_clients=self.data_clients, db_service=self.db_service)
        self.fixed_income = fixed_income.FixedIncome(data_clients=self.data_clients, db_service=self.db_service)
        self.options = options.Options(data_clients=self.data_clients, db_service=self.db_service)

    def get_fx_service(self):
        return self.fx

    def get_equity_service(self):
        return self.equities
    
    def get_fi_service(self):
        return self.fixed_income
    
    def get_misc_service(self):
        return self.misc
    
    def get_commodity_service(self):
        return self.commodities

    def get_option_service(self):
        return self.options

    def get_crypto_service(self):
        return self.crypto
    
    def get_basket_service(self):
        return self.baskets
    
    def get_macro_service(self):
        return self.macro

    def get_db_service(self):
        return self.db_service

async def batch_insert_ohlcv(loop, df):
    API_LIMIT = 300
    print(len(df))
    for i in range(0, len(df), API_LIMIT):
        print(i , "/" , len(df))
        temp_df = df.loc[i : i + API_LIMIT - 1]
        ohlcv = await data_master.get_equity_service().asyn_batch_get_ohlcv(
            tickers=list(temp_df["Code"]), 
            exchanges=list(["US" for _ in range(len(temp_df))]), 
            period_days=3000, 
            read_db=True, 
            insert_db=True
        )
    return True

async def batch_get_fundamentals(loop, df, exchange, exchange_filter):
    df = df.loc[df["Exchange"] == exchange_filter].dropna()
    df = df.loc[df["Type"] == "Commonf Stock"].reset_index(drop=True)
    tasks = []
    fundamentals = await data_master.get_equity_service().asyn_batch_get_ticker_generals(
        tickers=list(df["Code"]), 
        exchanges=["US" for _ in range(len(df))], 
        read_db=True, 
        insert_db=True
    )
    return fundamentals


if __name__ == "__main__":
    a = datetime.datetime.now() 
    data_master = DataMaster()
    df = data_master.get_misc_service().get_exchange_tickers("US")
    df = df.loc[(df["Exchange"] == "NYSE") | (df["Exchange"] == "NASDAQ")]
    df = df.loc[df["Type"] == "Common Stock"].dropna().reset_index(drop=True).head(300)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(batch_insert_ohlcv(loop, df))

    print(datetime.datetime.now() - a)
    