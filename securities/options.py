import os
import json
import requests
import datetime
import calendar
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
# To be completed
class Options():
        
    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

