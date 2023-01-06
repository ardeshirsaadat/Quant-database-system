import os
import json
import motor
import pymongo
import pandas as pd
import datetime
import asyncio
import db_logs
import threading
import motor.motor_asyncio
import multiprocessing as mp

from pymongo import UpdateOne
from collections import defaultdict

class DbService():

    def __init__(self, db_config={}):
        self.db_config = db_config
        self.mongo_cluster = pymongo.MongoClient(os.getenv("MONGO_CLUSTER"))        
        self.mongo_db = self.mongo_cluster[os.getenv("MONGO_DB")]
        self.asyn_mongo_cluster = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_CLUSTER"))
        self.asyn_mongo_db = self.asyn_mongo_cluster[os.getenv("MONGO_DB")]
    
    def _get_coll_name(self, dtype, dformat, dfreq):
        return "{}_{}_{}".format(dtype, dformat, dfreq)

    def _get_collection(self, dtype, dformat, dfreq):
        return self.mongo_db[self._get_coll_name(dtype, dformat, dfreq)]
    
    async def _asyn_get_collection(self, dtype, dformat, dfreq):
        return self.asyn_mongo_db[self._get_coll_name(dtype, dformat, dfreq)]

    def _get_collection_meta(self, dtype, dformat, dfreq):
        return self.mongo_db["{}-meta".format(self._get_coll_name(dtype, dformat, dfreq))]

    async def _asyn_get_collection_meta(self, dtype, dformat, dfreq):
        return self.asyn_mongo_db["{}-meta".format(self._get_coll_name(dtype, dformat, dfreq))]

    def _ensure_coll(self, dtype, dformat, dfreq, coll_type="timeseries", granularity="hours"):
        filter = {"name": {"$regex": r"^(?!system\.)"}}
        names = self.mongo_db.list_collection_names(filter=filter)   
        exists = self._get_coll_name(dtype=dtype, dformat=dformat, dfreq=dfreq) in names
        if not exists and (coll_type == "timeseries"):
            self.mongo_db.create_collection(
                "{}_{}_{}".format(dtype, dformat, dfreq), 
                timeseries={ 'timeField': 'datetime', 'metaField': 'metadata', 'granularity': granularity },
                check_exists=True
            )
            self.mongo_db.drop_collection("{}_{}_{}-meta".format(dtype, dformat, dfreq))
            self.mongo_db.create_collection(
                "{}_{}_{}-meta".format(dtype, dformat, dfreq)
            )
        if not exists and coll_type == "regular":
            self.mongo_db.create_collection(
                "{}_{}_{}".format(dtype, dformat, dfreq), 
                check_exists=True
            )
        return True

    def _check_contiguous_series(self, record_start, record_end, new_start, new_end):
        return new_start <= record_end and record_start <= new_end

    def insert_timeseries_df(self, dtype="equity", dformat="spot", dfreq="1d", 
                            df=None, series_metadata={}, series_identifier={}, metalogs=""):
        
        if len(df) == 0:
            db_logs.DBLogs().error("insert_timeseries_df got len 0 df {}".format(metalogs))
            return False
        df = df.loc[df["datetime"] >=  datetime.datetime(1970, 2, 1)]

        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        records = [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in df.iterrows()]
        series_start = records[0]["datetime"]
        series_end = records[-1]["datetime"]
        meta_series_count = self._get_collection_meta(dtype, dformat, dfreq).count_documents(series_identifier)
        
        if meta_series_count == 0:
            doc = {**series_identifier, **{"time_start": series_start, "time_end": series_end, "last_updated": datetime.datetime.utcnow()}}
            self._get_collection(dtype, dformat, dfreq).insert_many(records, ordered=False)
            self._get_collection_meta(dtype, dformat, dfreq).insert_one(doc)
        elif meta_series_count == 1:
            meta_record = self._get_collection_meta(dtype, dformat, dfreq).find_one(series_identifier)
            meta_start = meta_record["time_start"]
            meta_end = meta_record["time_end"]
            contiguous_series = self._check_contiguous_series(record_start=meta_start, record_end=meta_end, new_start=series_start, new_end=series_end)
            if not contiguous_series:
                db_logs.DBLogs().critical("discontiguous series in insert_timeseries_df {}".format(metalogs))
                return False
            new_head = df.loc[df["datetime"] < meta_start]
            new_tail = df.loc[df["datetime"] > meta_end]
            if len(new_head) + len(new_tail) > 0:
                head_records =  [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in new_head.iterrows()]
                tail_records =  [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in new_tail.iterrows()]
                self._get_collection(dtype, dformat, dfreq).insert_many(head_records + tail_records, ordered=False)
                self._get_collection_meta(dtype, dformat, dfreq).update_one(
                    series_identifier,
                    {"$set": {
                        "time_start": min(series_start, meta_start),
                        "time_end": max(series_end, meta_end),
                        "last_updated": datetime.datetime.utcnow(),
                    }}
                )

        else:
            db_logs.DBLogs().critical("meta series corruption, series count gt 1 insert_timeseries_df {}".format(metalogs))
            exit()
          
        db_logs.DBLogs().info("successful insert_timeseries_df {}".format(metalogs))
        return True
    
    async def asyn_insert_timeseries_df(self, dtype="equity", dformat="spot", dfreq="1d", 
                            df=None, series_metadata={}, series_identifier={}, metalogs=""):
        
        if len(df) == 0:
            db_logs.DBLogs().error("asyn_insert_timeseries_df got len 0 df {}".format(metalogs))
            return False
        df = df.loc[df["datetime"] >=  datetime.datetime(1970, 2, 1)]

        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        records = [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in df.iterrows()]
        series_start = records[0]["datetime"]
        series_end = records[-1]["datetime"]
        meta_series_count = await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).count_documents(series_identifier)
        
        if meta_series_count == 0:
            doc = {**series_identifier, **{"time_start": series_start, "time_end": series_end, "last_updated": datetime.datetime.utcnow()}}
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).insert_many(records, ordered=False)
            await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).insert_one(doc)
        elif meta_series_count == 1:
            meta_record = await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).find_one(series_identifier)
            meta_start = meta_record["time_start"]
            meta_end = meta_record["time_end"]
            contiguous_series = self._check_contiguous_series(record_start=meta_start, record_end=meta_end, new_start=series_start, new_end=series_end)
            if not contiguous_series:
                db_logs.DBLogs().critical("discontiguous series in asyn_insert_timeseries_df {}".format(metalogs))
                return False
            new_head = df.loc[df["datetime"] < meta_start]
            new_tail = df.loc[df["datetime"] > meta_end]
            if len(new_head) + len(new_tail) > 0:
                head_records =  [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in new_head.iterrows()]
                tail_records =  [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in new_tail.iterrows()]
                await (await self._asyn_get_collection(dtype, dformat, dfreq)).insert_many(head_records + tail_records, ordered=False)
                await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).update_one(
                    series_identifier,
                    {"$set": {
                        "time_start": min(series_start, meta_start),
                        "time_end": max(series_end, meta_end),
                        "last_updated": datetime.datetime.utcnow(),
                    }}
                )

        else:
            db_logs.DBLogs().critical("meta series corruption, series count gt 1 asyn_insert_timeseries_df {}".format(metalogs))
            exit()

        db_logs.DBLogs().info("successful asyn_insert_timeseries_df {}".format(metalogs))
        return True

    @staticmethod
    def unroll_df(args):
        series_metadata = args["sm"]
        df = args["df"]
        return [{**row.dropna().to_dict(), **{"metadata": series_metadata}} for index,row in df.iterrows()]

    async def asyn_batch_insert_timeseries_df(self, dtype="equity", dformat="spot", dfreq="1d", 
                            dfs=[], series_metadatas=[], series_identifiers=[], metalogs=[]):

        dfs = [df.loc[df["datetime"] >= datetime.datetime(1970, 2, 1)] for df in dfs]
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        recordss = []
        series_starts = []
        series_ends = []

        with mp.Manager() as manager:
            with mp.Pool(mp.cpu_count()) as pool:
                recordss = pool.map(DbService.unroll_df, [{"i": i, "sm" : series_metadatas[i], "df": dfs[i]} for i in range(len(dfs))])
        
        series_starts = [records[0]["datetime"] for records in recordss]
        series_ends = [records[-1]["datetime"] for records in recordss]

        docs = []
        if series_identifiers:
            async for i in (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).find({"$or" : series_identifiers}):
                docs.append(i)        
        
        meta_records = []
        for series_identifier in series_identifiers:
            matched = []
            for doc in docs:
                if series_identifier.items() <= doc.items():
                    matched.append(doc)
            meta_records.append(matched)

        new_inserts_seriess = []
        new_inserts_series_metas = []
        new_updates_series_metas = []
        update_identifers, new_heads, new_tails, time_starts, time_ends = [], [], [], [], []
        for i in range(len(series_identifiers)):
            matched = meta_records[i]
            if len(matched) == 0:
                doc = {
                    **series_identifiers[i], 
                    **{
                        "time_start": series_starts[i], 
                        "time_end": series_ends[i], 
                        "last_updated": datetime.datetime.utcnow()
                    }
                }
                new_inserts_seriess.extend(recordss[i])
                new_inserts_series_metas.append(doc)
            elif len(matched) == 1:
                meta_record = matched[0]
                meta_start = meta_record["time_start"]
                meta_end = meta_record["time_end"]
                contiguous_series = self._check_contiguous_series(
                    record_start=meta_start, 
                    record_end=meta_end, 
                    new_start=series_starts[i], 
                    new_end=series_ends[i]
                )
                if not contiguous_series:
                    db_logs.DBLogs().critical("discontiguous series in asyn_batch_insert_timeseries_df {}".format(metalogs[i]))
                    continue
                new_head = dfs[i].loc[dfs[i]["datetime"] < meta_start]
                new_tail = dfs[i].loc[dfs[i]["datetime"] > meta_end]
                if len(new_head) + len(new_tail) > 0:
                    new_heads.append(new_head)
                    new_tails.append(new_tail)
                    time_starts.append(min(series_starts[i], meta_start))
                    time_ends.append(max(series_ends[i], meta_end))
                    update_identifers.append(series_identifiers[i])
            else:
                db_logs.DBLogs().critical("meta series corruption, series count gt 1 asyn_batch_insert_timeseries_df {}".format(metalogs[i]))
        
        with mp.Manager() as manager:
            with mp.Pool(mp.cpu_count()) as pool:
                unrolled_heads = pool.map(DbService.unroll_df, [{"i": i, "sm" : update_identifers[i], "df": new_heads[i]} for i in range(len(new_heads))])
                unrolled_tails = pool.map(DbService.unroll_df, [{"i": i, "sm" : update_identifers[i], "df": new_tails[i]} for i in range(len(new_tails))])
                
                for head_records in unrolled_heads:
                    new_inserts_seriess.extend(head_records)
                for tail_records in unrolled_tails:
                    new_inserts_seriess.extend(tail_records)
                for i in range(len(new_heads)):
                    new_updates_series_metas.append(UpdateOne(
                        update_identifers[i],
                        {"$set": {
                            "time_start": time_starts[i],
                            "time_end": time_ends[i],
                            "last_updated": datetime.datetime.utcnow(),
                        }}
                    ))
   
        if new_inserts_seriess:
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).insert_many(new_inserts_seriess, ordered=False)      
        if new_inserts_series_metas:
            await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).insert_many(new_inserts_series_metas, ordered=False)
        if new_updates_series_metas:
            await (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).bulk_write(new_updates_series_metas, ordered=False)
       
        db_logs.DBLogs().info("successful asyn_batch_insert_timeseries_df {}".format(metalogs))
        return True

    def read_timeseries(self, dtype="equity", dformat="spot", dfreq="1d", period_start=None, period_end=None, series_metadata={}, series_identifier={}, metalogs=""):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        period_start = max(period_start, datetime.datetime(1970, 2, 1))
        period_end = max(period_end, datetime.datetime(1970, 2, 1))
        docs = list(self._get_collection_meta(dtype, dformat, dfreq).find(series_identifier))
        if len(docs) == 0:
            exists, series_df = False, pd.DataFrame()
            db_logs.DBLogs().info("successful len 0 read_timeseries {}".format(metalogs))
            return exists, series_df
        if len(docs) == 1:
            record_start = docs[0]["time_start"]
            record_end = docs[0]["time_end"]
            if dfreq == "1d":
                period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
                period_end = period_end.replace(hour=0, minute=0, second=0, microsecond=0)
            series_filter = {"datetime" : {"$gte" : period_start, "$lte" :  period_end }}
            records = list(self._get_collection(dtype, dformat, dfreq).find(
                {
                    **series_filter,
                    **{"metadata.{}".format(k) : v for k,v in series_metadata.items()}
                }
            ))
            if period_start < record_start or record_end < period_end:
                exists, series_df = False, pd.DataFrame(records)
                db_logs.DBLogs().info("successful len 1 missing read_timeseries {}".format(metalogs))
                return exists, series_df
            
            exists, series_df = True, pd.DataFrame(records)
            db_logs.DBLogs().info("successful len 1 full read_timeseries {}".format(metalogs))
            return exists, series_df
        if len(docs) > 1:
            db_logs.DBLogs().critical("read_timeseries got count gt 1 {}".format(metalogs))
            exit()

    async def asyn_read_timeseries(self, dtype="equity", dformat="spot", dfreq="1d", period_start=None, period_end=None, series_metadata={}, series_identifier={}, metalogs=""):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        period_start = max(period_start, datetime.datetime(1970, 2, 1))
        period_end = max(period_end, datetime.datetime(1970, 2, 1))
        docs = []
        async for i in (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).find(series_identifier):
            docs.append(i)
        if len(docs) == 0:
            exists, series_df = False, pd.DataFrame()
            db_logs.DBLogs().info("successful len 0 asyn_read_timeseries {}".format(metalogs))
            return exists, series_df
        if len(docs) == 1:
            record_start = docs[0]["time_start"]
            record_end = docs[0]["time_end"]
            if dfreq == "1d":
                period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
                period_end = period_end.replace(hour=0, minute=0, second=0, microsecond=0)
            series_filter = {"datetime" : {"$gte" : period_start , "$lte" : period_end }}
            records = []
            async for i in (await self._asyn_get_collection(dtype, dformat, dfreq)).find(
                {
                    **series_filter,
                    **{"metadata.{}".format(k) : v for k,v in series_metadata.items()}
                }):
                records.append(i)
            if period_start < record_start or record_end < period_end:
                exists, series_df = False, pd.DataFrame(records)
                db_logs.DBLogs().info("successful len 1 missing asyn_read_timeseries {}".format(metalogs))
                return exists, series_df
            
            exists, series_df = True, pd.DataFrame(records)
            db_logs.DBLogs().info("successful len 1 full asyn_read_timeseries {}".format(metalogs))
            return exists, series_df
        if len(docs) > 1:
            db_logs.DBLogs().critical("asyn_read_timeseries got count gt 1 {}".format(metalogs))
            exit()

    async def asyn_batch_read_timeseries(self, 
        dtype="equity", dformat="spot", dfreq="1d", 
        period_start=None, period_end=None, 
        series_metadatas=[], series_identifiers=[], metalogs=[]):
        
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="timeseries")
        period_start = max(period_start, datetime.datetime(1970, 2, 1))
        period_end = max(period_end, datetime.datetime(1970, 2, 1))
        docs = []
        if series_identifiers:
            async for i in (await self._asyn_get_collection_meta(dtype, dformat, dfreq)).find({"$or" : series_identifiers}):
                docs.append(i)

        series_records = []
        for series_identifier in series_identifiers:
            matched = []
            for doc in docs:
                if series_identifier.items() <= doc.items():
                    matched.append(doc)
            series_records.append(matched)

        if dfreq == "1d":
            period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end = period_end.replace(hour=0, minute=0, second=0, microsecond=0)

        series_filter = {"datetime" : {"$gte" : period_start , "$lte" : period_end }}

        existss = [None for _ in range(len(series_identifiers))]
        series_dfs = [None for _ in range(len(series_identifiers))]
        def poll_record(i):
            matched = series_records[i]
            if len(matched) == 0:
                existss[i] = False
                series_dfs[i] = pd.DataFrame()

            if len(matched) == 1:
                record_start = matched[0]["time_start"]
                record_end = matched[0]["time_end"]
                records = list(self._get_collection(dtype, dformat, dfreq).find(
                    {
                        **series_filter,
                        **{"metadata.{}".format(k) : v for k,v in series_metadatas[i].items()}
                    }
                ))
                if period_start < record_start or record_end < period_end:
                    existss[i] = False 
                    series_dfs[i] = pd.DataFrame(records)
                else:
                    existss[i] = True
                    series_dfs[i] = pd.DataFrame(records) 
            if len(matched) > 1:
                db_logs.DBLogs().critical("asyn_batch_read_timeseries got count gt 1 {}".format(metalogs[i]))
                existss[i] = None
                series_dfs[i] = None

        threads = []
        for i in range(len(series_identifiers)):
            threads.append(
                threading.Thread(target=poll_record, args=(i,))
            )
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
     
        return existss, series_dfs
   
    def insert_docs(self, dtype="equity", dformat="fundamentals", dfreq="irregular", 
                    docdata={}, doc_identifier={}, metalogs=""):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="regular")
        doc_count = self._get_collection(dtype, dformat, dfreq).count_documents(doc_identifier)
        if doc_count == 0:
            doc = {**doc_identifier, **{"data" : docdata}, **{"last_updated": datetime.datetime.utcnow()}}
            self._get_collection(dtype, dformat, dfreq).insert_one(doc)
        elif doc_count == 1:
            self._get_collection(dtype, dformat, dfreq).update_one(
                doc_identifier, 
                {"$set" : {"data" : docdata, "last_updated" : datetime.datetime.utcnow()} }, 
                upsert=True
            )
        else:
            db_logs.DBLogs().critical("insert_docs got count gt 1 {}".format(metalogs))
            exit()
        db_logs.DBLogs().info("successful insert_docs {}".format(metalogs))
        return True

    async def asyn_insert_docs(self, dtype="equity", dformat="fundamentals", dfreq="irregular", 
                            docdata={}, doc_identifier={}, metalogs=""):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="regular")
        doc_count = await (await self._asyn_get_collection(dtype, dformat, dfreq)).count_documents(doc_identifier)
        if doc_count == 0:
            doc = {**doc_identifier, **{"data" : docdata}, **{"last_updated": datetime.datetime.utcnow()}}
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).insert_one(doc)
        elif doc_count == 1:
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).update_one(
                doc_identifier, 
                {"$set" : {"data" : docdata, "last_updated" : datetime.datetime.utcnow()}},
                upsert=True
            )
        else:
            db_logs.DBLogs().critical("asyn_insert_docs got count gt 1 {}".format(metalogs))
            exit()
        db_logs.DBLogs().info("successful asyn_insert_docs {}".format(metalogs))
        return True

    async def asyn_batch_insert_docs(self, dtype="equity", dformat="fundamentals", dfreq="irregular", 
                            docdatas=[], doc_identifiers=[], metalogs=[]):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="regular")
        
        docs = []
        if doc_identifiers:
            async for i in (await self._asyn_get_collection(dtype, dformat, dfreq)).find({"$or" : doc_identifiers}):
                docs.append(i)
            
        doc_records = []
        for doc_identifier in doc_identifiers:
            matched = []
            for doc in docs:
                if doc_identifier.items() <= doc.items():
                    matched.append(doc)
            doc_records.append(matched)
        
        new_inserts = []
        new_updates = []
        for i in range(len(doc_identifiers)):
            matched = doc_records[i]
            if len(matched) == 0:
                doc = {
                    **doc_identifiers[i], 
                    **{"data" : docdatas[i]}, 
                    **{"last_updated": datetime.datetime.utcnow()}
                }
                new_inserts.append(doc)
            elif len(matched) == 1:
                new_updates.append(UpdateOne(
                    doc_identifiers[i],
                    {"$set" : {
                        "data" : docdatas[i], 
                        "last_updated" : datetime.datetime.utcnow()}},
                    upsert=True
                ))
            else:
                db_logs.DBLogs().critical("asyn_batch_insert_docs got count gt 1 {}".format(metalogs[i]))
        
        if new_inserts:
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).insert_many(new_inserts, ordered=False)      
        if new_updates:
            await (await self._asyn_get_collection(dtype, dformat, dfreq)).bulk_write(new_updates, ordered=False)
        db_logs.DBLogs().info("successful asyn_insert_docs {}".format(metalogs))
        return True

    def read_docs(self, dtype="equity", dformat="fundamentals", dfreq="irregular", 
                doc_identifier={}, metalogs="", expire_db=24*5):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="regular")
        docs = list(self._get_collection(dtype, dformat, dfreq).find(doc_identifier))
        if len(docs) == 0:
            exists, expired, docdata = False, True, {}
            db_logs.DBLogs().info("successful len 0 read_docs {}".format(metalogs))
            return exists, expired, docdata
        if len(docs) == 1:
            last_updated = docs[0]["last_updated"]
            time_delta_hours = (datetime.datetime.utcnow() - last_updated) / datetime.timedelta(hours=1)
            if time_delta_hours < expire_db:
                exists, expired, docdata = True, False, docs[0]["data"] 
            else:
                exists, expired, docdata = True, True, docs[0]["data"]
            db_logs.DBLogs().info("successful len 1 read_docs {}".format(metalogs))
            return exists, expired, docdata
        if len(docs) > 1:
            db_logs.DBLogs().critical("read_docs got count gt 1 {}".format(metalogs))
            exit()

    async def asyn_read_docs(self, dtype="equity", dformat="fundamentals", dfreq="irregular", 
                            doc_identifier={}, metalogs="", expire_db=10):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="regular")
        docs = []
        async for i in (await self._asyn_get_collection(dtype, dformat, dfreq)).find(doc_identifier):
            docs.append(i)
        if len(docs) == 0:
            exists, expired, docdata = False, True, {}
            db_logs.DBLogs().info("successful len 0 asyn_read_docs {}".format(metalogs))
            return exists, expired, docdata
        if len(docs) == 1:
            last_updated = docs[0]["last_updated"]
            time_delta_hours = (datetime.datetime.utcnow() - last_updated) / datetime.timedelta(hours=1)
            if time_delta_hours < expire_db:
                exists, expired, docdata = True, False, docs[0]["data"] 
            else:
                exists, expired, docdata = True, True, docs[0]["data"]
            db_logs.DBLogs().info("successful len 1 asyn_read_docs {}".format(metalogs))
            return exists, expired, docdata
        if len(docs) > 1:
            db_logs.DBLogs().critical("asyn_read_docs got count gt 1 {}".format(metalogs))
            exit()

    async def asyn_batch_read_docs(self, dtype="equity", dformat="fundamentals", dfreq="irregular", 
                            doc_identifiers=[], metalogs=[], expire_db=24*5):
        self._ensure_coll(dtype=dtype, dformat=dformat, dfreq=dfreq, coll_type="regular")
        docs = []
        if doc_identifiers:
            async for i in (await self._asyn_get_collection(dtype, dformat, dfreq)).find({"$or": doc_identifiers}):
                docs.append(i)

        doc_records = []
        for doc_identifier in doc_identifiers:
            matched = []
            for doc in docs:
                if doc_identifier.items() <= doc.items():
                    matched.append(doc)
            doc_records.append(matched)         
             
        existss = []
        expireds = []
        docdatas = []

        for i in range(len(doc_identifiers)):
            matched = doc_records[i]
            if len(matched) == 0:
                existss.append(False)
                expireds.append(True)
                docdatas.append({})
            
            if len(matched) == 1:
                last_updated = matched[0]["last_updated"]
                time_delta_hours = (datetime.datetime.utcnow() - last_updated) / datetime.timedelta(hours=1)
                if time_delta_hours < expire_db:
                    existss.append(True)
                    expireds.append(False) 
                    docdatas.append(matched[0]["data"]) 
                else:
                    existss.append(True)
                    expireds.append(True)
                    docdatas.append(matched[0]["data"])
                db_logs.DBLogs().info("successful len 1 asyn_read_docs {}".format(metalogs[i]))
            
            if len(matched) > 1:
                db_logs.DBLogs().critical("asyn_batch_read_docs got count gt 1 {}".format(metalogs[i]))
                existss.append(None)
                expireds.append(None)
                docdatas.append(None)
        
        return existss, expireds, docdatas
