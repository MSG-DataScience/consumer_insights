# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 11:17:28 2017

@author: haot
"""


import time
import requests
import base64
import json
import pprint
import pandas as pd
import numpy as np
import datetime
import sqlalchemy
from datetime import date, timedelta
engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")
query='''
SELECT attraction_name,inet_event_id,event_date
FROM msgbiadb.ads_main.d_event_master
where venue_name='MADISON SQUARE GARDEN'
ORDER BY event_date
'''
other= pd.read_sql(query, engine)
df=pd.DataFrame()
yesterday = date.today() - timedelta(1)
for x in pd.date_range(yesterday, yesterday):
    x=x.date()
    name='https://s3.amazonaws.com/ampthink-msg/device-reports/device-report-%s.csv'%x
    data=pd.read_csv(name)
    df=pd.concat([df,data]).drop_duplicates().reset_index(drop=True)

df = df[pd.notnull(df['Suite'])]
df = df[pd.notnull(df['Email'])]
df=df[['Date','Email','Suite']]
df=df.drop_duplicates()
df = df[df['Suite'] > 1] 
df = df[df['Suite'] < 58] 
suite=pd.read_csv('C:/Users/haot/Documents/GitHub/consumer_insights/thao/suite.csv')
email=pd.read_csv('C:/Users/haot/Documents/GitHub/consumer_insights/thao/am_email.csv')
suite['Suite']=suite['Suite'].str.extract('(\d+)').astype(int)
suite=suite.drop_duplicates().reset_index(drop=True)
suite=suite.drop(suite.index[:2])
df1=pd.merge(df,suite,how='left',on='Suite')
df2=pd.merge(df1,email,how='left',on='name')
df2['Date']=pd.to_datetime(df2['Date'])
df3=pd.merge(df2,other,how='left',left_on='Date',right_on='event_date')
df3=df3.drop(['Date','event_date'],axis=1)
df3.columns=['email_address','suite_id','suite_account_rep_name','suite_account_rep_email','attraction_name','event_inventory_id']
df3['name_first']='Fan'
df3['name_last']=np.nan
df3=df3[['event_inventory_id','name_first','name_last','email_address','suite_id','attraction_name','suite_account_rep_name','suite_account_rep_email']]
df3['suite_id'] = 'LSL' + df3['suite_id'].astype(int).astype(str)
def to_integer(dt_time):   
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day
a=to_integer(yesterday)
df3.to_csv('msg_invitation_file_suites_%s.csv'%a,index=False)


import ftplib
session = ftplib.FTP('ftp1.medallia.com','msg','Ms$5trf^&G*&')
session.cwd('/MSG_to_Medallia/Invites')
file = open('msg_invitation_file_suites_%s.csv'%a,'rb')                  # file to send
session.storbinary('STOR uat_msg_invitation_file_suites_%s.csv'%a, file)     # send the file
file.close()                                    # close file and FTP
session.quit()
