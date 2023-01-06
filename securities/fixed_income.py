import os
import json
import requests
import datetime
import calendar
import websockets
import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta
from sockets.eod_sockclient import EodSocketClient

import wrappers.eod_wrapper as eod_wrapper
# To be completed
class FixedIncome():

    def __init__(self, data_clients={}, db_service=None):
        self.data_clients = data_clients
        self.eod_client = data_clients["eod_client"]

