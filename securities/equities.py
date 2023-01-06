import os
import json
import time
import aiohttp
import asyncio
import requests
import datetime
import urllib
import numpy as np
import pandas as pd

import db_logs

from collections import defaultdict
from dateutil.relativedelta import relativedelta

import wrappers.eod_wrapper as eod_wrapper
import wrappers.aiohttp_wrapper as aiohttp_wrapper

class Equities():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.db_service = db_service
    
    """
    Master Utilities
    """
    def get_sec_tickers(self):
        url = 'https://www.sec.gov/files/company_tickers.json'
        resp = requests.get(url=url, params=None)
        data = resp.json()
        df = pd.DataFrame(data).transpose()
        return df

    def get_us_securities(self):
        resp = self.eod_client.get_exchange_symbols(exchange='US')
        df = pd.DataFrame(resp)
        return df

    """
    Fundamentals
    """
    def get_fundamentals_dump(self, ticker, exchange, read_db=True, insert_db=True, expire_db=10):
        return eod_wrapper.get_fundamental_data(eod_client=self.eod_client, ticker=ticker, exchange=exchange)
         
    """
    Fundamentals::General Information
    """
    def get_ticker_generals(self, ticker, exchange, read_db=True, insert_db=True, expire_db=24*12):
        docdata = None
        try:
            doc_identifier = {
                "type": "ticker_generals",
                "ticker": ticker,
                "exchange": exchange,
                "source": "eodhistoricaldata"
            }
            if read_db and expire_db > 0:
                exists, expired, docdata = self.db_service.read_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", doc_identifier=doc_identifier, metalogs=ticker, expire_db=expire_db)
            if not read_db or expire_db <= 0 or not exists or expired:
                url = "https://eodhistoricaldata.com/api/fundamentals/{}.{}".format(ticker, exchange)
                params = {"api_token": os.getenv('EOD_KEY'), "filter":"General"}
                resp = requests.get(url, params=params)
                docdata = resp.json()
                if insert_db:
                    self.db_service.insert_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", docdata=docdata, doc_identifier=doc_identifier, metalogs=ticker)
        except Exception:
            db_logs.DBLogs().critical("get_ticker_generals FAILED {}".format(ticker))
        return docdata

    async def asyn_get_ticker_generals(self, ticker, exchange, read_db=True, insert_db=True, expire_db=24*12, tries=0):
        docdata = None
        try:
            doc_identifier = {
                "type": "ticker_generals",
                "ticker": ticker,
                "exchange": exchange,
                "source": "eodhistoricaldata"
            }
            if read_db and expire_db > 0:
                exists, expired, docdata = await self.db_service.asyn_read_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", doc_identifier=doc_identifier, metalogs=ticker, expire_db=expire_db)
            if not read_db or expire_db <= 0 or not exists or expired:
                url = "https://eodhistoricaldata.com/api/fundamentals/{}.{}".format(ticker, exchange)
                params = {"api_token": os.getenv('EOD_KEY'), "filter":"General"}
                async with aiohttp.ClientSession() as session:
                    async with session.get(url=url, params=params) as resp:
                        docdata = await resp.json()
                if insert_db:
                    await self.db_service.asyn_insert_docs(dtype="equity", dformat="fundamentals", dfreq="irregular", docdata=docdata, doc_identifier=doc_identifier, metalogs=ticker)
        except Exception:
            if tries < 5:
                db_logs.DBLogs().warning("asyn_get_ticker_generals RETRY {} {}".format(tries, ticker))
                return await self.asyn_get_ticker_generals(ticker=ticker, exchange=exchange, read_db=read_db, insert_db=insert_db, expire_db=expire_db, tries=tries+1)
            db_logs.DBLogs().critical("asyn_get_ticker_generals FAILED {}".format(ticker))
        return docdata
    
    async def asyn_batch_get_ticker_generals(self, tickers, exchanges, read_db=True, insert_db=True, expire_db=24*12):
        doc_identifiers = [{
            "type": "ticker_generals",
            "ticker": ticker,
            "exchange": exchange,
            "source": "eodhistoricaldata"
        } for ticker, exchange in zip(tickers, exchanges)]

        if read_db and expire_db > 0:
            existss, expireds, docdatas = await self.db_service.asyn_batch_read_docs(
                dtype="equity", dformat="fundamentals", dfreq="irregular", 
                doc_identifiers=doc_identifiers, metalogs=tickers, expire_db=expire_db
            )
        
        request_tickers, request_urls, request_identifiers = [], [], []
        for i in range(len(tickers)):
            if not read_db or expire_db <= 0 or not existss[i] or expireds[i]:
                url = "https://eodhistoricaldata.com/api/fundamentals/{}.{}?".format(tickers[i], exchanges[i])
                params = {"api_token": os.getenv('EOD_KEY'), "filter":"General"}
                request_tickers.append(tickers[i])
                request_urls.append(url + urllib.parse.urlencode(params))
                request_identifiers.append(doc_identifiers[i])
        
        request_results = await aiohttp_wrapper.async_aiohttp_get_all(request_urls)
        ticker_generals = []
        j = 0
        for i in range(len(tickers)):
            if not read_db or expire_db <= 0 or not existss[i] or expireds[i]:
                if request_results[j] is None:
                    db_logs.DBLogs().critical("asyn_batch_get_ticker_generals FAILED {}".format(request_tickers[j]))            
                ticker_generals.append(request_results[j])
                j += 1
            else:
                ticker_generals.append(docdatas[i])
        
        if insert_db:
            insert_tickers, insert_docdatas, insert_doc_identifiers = [], [], []
            for i in range(len(request_tickers)):
                if request_results[i] is not None:
                    insert_tickers.append(request_tickers[i])
                    insert_docdatas.append(request_results[i])
                    insert_doc_identifiers.append(request_identifiers[i])
            await self.db_service.asyn_batch_insert_docs(
                dtype="equity", dformat="fundamentals", dfreq="irregular", 
                docdatas=insert_docdatas, doc_identifiers=insert_doc_identifiers, metalogs=insert_tickers)
        
        return ticker_generals

    def get_ticker_classification(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return {
            "sector": ticker_fundamentals["Sector"],
            "industry": ticker_fundamentals["Industry"],
            "gicsect": ticker_fundamentals["GicSector"],
            "gicgrp": ticker_fundamentals["GicGroup"],
            "gicind": ticker_fundamentals["GicIndustry"],
            "gicsubind": ticker_fundamentals["GicSubIndustry"]
        }


    def get_ticker_exchange(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["Exchange"]

    def get_ticker_countryiso(self, ticker, exchange):
        ticker_fundamentals = self.get_ticker_generals(ticker=ticker, exchange=exchange)
        return ticker_fundamentals["CountryISO"]

    def get_identification_codes(self, ticker="AAPL", exchange="US"):
        ticker_fundamentals = defaultdict(str)
        ticker_fundamentals.update(self.get_ticker_generals(ticker=ticker, exchange=exchange))
        return {
            "isin": ticker_fundamentals["ISIN"],
            "cusip": ticker_fundamentals["CUSIP"],
            "cik": ticker_fundamentals["CIK"]
        }

    async def asyn_get_identification_codes(self, ticker="AAPL", exchange="US"):
        ticker_fundamentals = defaultdict(str)
        ticker_fundamentals.update(await self.asyn_get_ticker_generals(ticker=ticker, exchange=exchange))
        return {
            "isin": ticker_fundamentals["ISIN"],
            "cusip": ticker_fundamentals["CUSIP"],
            "cik": ticker_fundamentals["CIK"]
        }
    
    async def asyn_batch_get_identification_codes(self, tickers=[], exchanges=[]):
        batch_generals = await self.asyn_batch_get_ticker_generals(tickers=tickers, exchanges=exchanges)
        batch_codes = []
        for general in batch_generals:
            ticker_fundamentals = defaultdict(str)
            ticker_fundamentals.update(general)
            batch_codes.append({
                "isin": ticker_fundamentals["ISIN"],
                "cusip": ticker_fundamentals["CUSIP"],
                "cik": ticker_fundamentals["CIK"]
            })
        return batch_codes

   
    """
    Price::OHLCV and Others
    """
    def get_ohlcv(self, ticker, exchange, period_end=datetime.datetime.today(), period_start=None, period_days=3650, read_db=True, insert_db=True):
        series_df = None
        try:
            id_codes = self.get_identification_codes(ticker=ticker, exchange=exchange)
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                isin=id_codes["isin"],
                ticker=ticker,
                exchange=exchange,
                source="eodhistoricaldata"
            )
            if read_db:
                period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
                exists, series_df = self.db_service.read_timeseries(
                    dtype="equity", dformat="spot", dfreq="1d", 
                    series_metadata=series_metadata, series_identifier=series_identifier, metalogs=ticker,
                    period_start=period_start,
                    period_end=period_end
                )
            if not read_db or not exists:
                series_df = eod_wrapper.get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
                if not insert_db:
                    db_logs.DBLogs().info("successful get_ohlcv but not inserted {}".format(ticker))
                elif insert_db and len(series_df) > 0:
                    self.db_service.insert_timeseries_df(dtype="equity", dformat="spot", dfreq="1d", 
                                    df=series_df, series_identifier=series_identifier, series_metadata=series_metadata, metalogs=ticker)
                    db_logs.DBLogs().info("successful get_ohlcv with db write {}".format(ticker))
                elif insert_db and len(series_df) == 0:
                    db_logs.DBLogs().info("successful get_ohlcv but skipped with len-0 insert {}".format(ticker))
                else:
                    pass
        except Exception:
            db_logs.DBLogs().critical("get_ohlcv FAILED {}".format(ticker))
        return series_df

    async def asyn_get_ohlcv(self, ticker, exchange, period_end=datetime.datetime.today(), period_start=None, period_days=3650, read_db=True, insert_db=True, tries=0):
        series_df = None
        try:
            id_codes = await self.asyn_get_identification_codes(ticker=ticker, exchange=exchange)
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                isin=id_codes["isin"], 
                ticker=ticker, 
                exchange=exchange, 
                source="eodhistoricaldata")
            if read_db:
                period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
                exists, series_df = await self.db_service.asyn_read_timeseries(
                    dtype="equity", dformat="spot", dfreq="1d", 
                    series_metadata=series_metadata, series_identifier=series_identifier, metalogs=ticker,
                    period_start=period_start,
                    period_end=period_end
                )
            if not read_db or not exists:
                series_df = await eod_wrapper.asyn_get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days)
                if not insert_db:
                    db_logs.DBLogs().info("successful asyn_get_ohlcv but not inserted {}".format(ticker))
                elif insert_db and len(series_df) > 0:
                    await self.db_service.asyn_insert_timeseries_df(dtype="equity", dformat="spot", dfreq="1d", 
                                    df=series_df, series_identifier=series_identifier, series_metadata=series_metadata, metalogs=ticker)
                    db_logs.DBLogs().info("successful asyn_get_ohlcv with db write {}".format(ticker))
                elif insert_db and len(series_df) == 0:
                    db_logs.DBLogs().info("successful asyn_get_ohlcv but skipped with len-0 insert {}".format(ticker))
                else:
                    pass
        except Exception:
            if tries < 3:
                db_logs.DBLogs().warning("asyn_get_ohlcv RETRY {} {}".format(tries, ticker))    
                return await self.asyn_get_ohlcv(ticker=ticker, exchange=exchange, period_end=period_end, period_start=period_start, period_days=period_days, read_db=read_db, insert_db=insert_db, tries=tries+1)
            db_logs.DBLogs().critical("asyn_get_ohlcv FAILED {}".format(ticker))
        return series_df

    async def asyn_batch_get_ohlcv(self, tickers, exchanges, 
        period_end=datetime.datetime.today(), period_start=None, period_days=3650, 
        read_db=True, insert_db=True):
        
        id_codess = await self.asyn_batch_get_identification_codes(tickers=tickers, exchanges=exchanges)
        series_metadatas, series_identifiers = [], []
   
        for i in range(len(tickers)):
            id_codes = id_codess[i]
            series_metadata, series_identifier = self._get_series_identifiers_and_metadata(
                isin=id_codes["isin"], 
                ticker=tickers[i], 
                exchange=exchanges[i], 
                source="eodhistoricaldata")
            series_metadatas.append(series_metadata)
            series_identifiers.append(series_identifier)

        if read_db:
            period_start = period_start if period_start else period_end - datetime.timedelta(days=period_days)
            existss, series_dfs = await self.db_service.asyn_batch_read_timeseries(
                dtype="equity", dformat="spot", dfreq="1d", 
                series_metadatas=series_metadatas, series_identifiers=series_identifiers, metalogs=tickers,
                period_start=period_start,
                period_end=period_end
            )
        
        request_tickers, request_exchanges, request_metadatas, request_identifiers = [], [], [], []
        for i in range(len(tickers)):
            if not read_db or not existss[i]:
                request_tickers.append(tickers[i])
                request_exchanges.append(exchanges[i])
                request_metadatas.append(series_metadatas[i])
                request_identifiers.append(series_identifiers[i])

        request_results = await eod_wrapper.asyn_batch_get_ohlcv(
            tickers=request_tickers, 
            exchanges=request_exchanges, 
            period_end=period_end, 
            period_start=period_start, 
            period_days=period_days
        )

        ohlcvs = []
        j = 0
        for i in range(len(tickers)):
            if not read_db or not existss[i]:
                if request_results[j] is None:
                    db_logs.DBLogs().critical("asyn_batch_get_ohlcv FAILED {}".format(request_tickers[j]))
                ohlcvs.append(request_results[j])
                j += 1
            else:
                ohlcvs.append(series_dfs[i])

        if insert_db:
            insert_tickers, insert_ohlcvs, insert_series_metadatas, insert_series_identifiers = [], [], [], []
            for i in range(len(request_tickers)):
                if request_results[i] is None:
                    pass
                elif request_results[i] is not None and len(request_results[i]) == 0:
                    db_logs.DBLogs().info("successful asyn_get_ohlcv but skipped with len-0 insert {}".format(request_tickers[i]))
                elif request_results[i] is not None and len(request_results[i]) > 0:
                    insert_tickers.append(request_tickers[i])
                    insert_ohlcvs.append(request_results[i])
                    insert_series_metadatas.append(request_metadatas[i])
                    insert_series_identifiers.append(request_identifiers[i])
            
            await self.db_service.asyn_batch_insert_timeseries_df(dtype="equity", dformat="spot", dfreq="1d", 
                    dfs=insert_ohlcvs, series_identifiers=insert_series_identifiers, series_metadatas=insert_series_metadatas, metalogs=insert_tickers)

        return ohlcvs
