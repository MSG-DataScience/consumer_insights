# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 13:11:25 2017

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
sch=pd.read_csv('C:/Users/haot/schedule.csv')
df['Date']=pd.to_datetime(df['Date']).dt.date
sch['tm_event_date']=pd.to_datetime(sch['tm_event_date']).dt.date
df2=pd.merge(df,sch,how='left',left_on='Date',right_on='tm_event_date')
final=df2[['Email','Date','ads_source']]
final.columns=['Email','Date','Team']
final.to_csv('G:/Engagement Marketing/wifi_gate_emails.csv',index=False)