import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

import oandapyV20
import oandapyV20.endpoints.instruments as instruments

from dateutil.relativedelta import relativedelta

import wrappers.eod_wrapper as eod_wrapper
# To be completed
class FX():
        
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]
        self.oanda_client = data_clients["oanda_client"]

