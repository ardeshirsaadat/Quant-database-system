import data_master as DataMaster
# Improve server side optimization via indexing
data_master = DataMaster.DataMaster()
            
resp = data_master.get_db_service()._get_collection(dtype="equity", dformat="spot", dfreq="1d").create_index([
        ("datetime", 1),
        ("metadata.isin", 1),
        ("metadata.ticker", 1),
        ("metadata.exchange", 1),
        ("metadata.source", 1),
        ]
)
                        