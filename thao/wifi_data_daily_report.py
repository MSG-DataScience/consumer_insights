# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 09:36:15 2017

@author: haot
"""
import pandas as pd
from datetime import date, timedelta


#https://s3.amazonaws.com/ampthink-msg/device-reports/device-report-2017-09-08.csv
df=pd.DataFrame()
yesterday = date.today() - timedelta(1)
for x in pd.date_range('09-08-2017', yesterday):
    x=x.date()
    name='https://s3.amazonaws.com/ampthink-msg/device-reports/device-report-%s.csv'%x
    data=pd.read_csv(name)
    df=pd.concat([df,data]).drop_duplicates().reset_index(drop=True)
df.to_csv('C:/Users/haot/Documents/GitHub/consumer_insights/thao/wifi_data.csv')