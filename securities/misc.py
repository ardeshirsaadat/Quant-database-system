import os
import json
import requests
import datetime
import pandas as pd

from sec_edgar_api import EdgarClient
# To be completed
class Miscellaneous():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.edgar_client = EdgarClient("edgar_user_agent")
        
