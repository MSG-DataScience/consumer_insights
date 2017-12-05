# -*- coding: utf-8 -*-
"""
Created on Wed Nov 15 12:37:26 2017

@author: haot
"""

import re
import time
import requests
import base64
import json
import pprint
import pandas as pd
import numpy as np
import datetime
from sklearn import preprocessing
import sqlalchemy
import statsmodels.api as sm
from itertools import product


wifi=pd.read_csv('wifi_data.csv')
#wifi=wifi[wifi.Authenticated==True]
#wifi=wifi[pd.notnull(wifi['Facebook ID'])]
wifi1=wifi[pd.notnull(wifi['Email'])]
#a=float(len(wifi1.index))/len(wifi.index)