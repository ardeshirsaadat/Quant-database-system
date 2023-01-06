import os
import re
import json
import requests
import datetime
import pandas as pd

from cif import cif
from dbnomics import fetch_series, fetch_series_by_api_link
# To be completed
class Macro():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.fred_client = data_clients["fred_client"]

