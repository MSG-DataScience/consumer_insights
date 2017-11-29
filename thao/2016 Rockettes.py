# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 13:03:17 2017

@author: haot
"""

from datetime import date, timedelta
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
from sklearn.metrics import r2_score
from datetime import datetime

engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")
query1 = '''
with table1
as
(
select 
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as cust_sum,
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as max_date,
full_date,
tm_acct_id,
tm_event_name,
tm_price_code_desc,
tm_section_name, 
tm_event_date,
zip,
country,
tm_row_name, 
tm_seat_num, 
tickets_sold,
tickets_total_revenue, 
tickets_add_datetime, 
ticket_transaction_date,
ticket_type_price_level,
tm_comp_name,
tm_season_name,
mpd_indy_rank,
mpd_game_num,
tm_event_name_long,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2015 RCCS 1st Half','2015 RCCS 2nd Half','2016 RCCS 1st Half','2016 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
and acct_type_desc != 'Tix Pro Seller'
AND house_seat_flag='N'
and tm_acct_id not in(-1,-2)
)
select tm_acct_id,zip,country,tm_season_name,full_date
from table1
'''

#full_date<    DATEADD(DAY,0,GETDATE())

data1= pd.read_sql(query1, engine)

data1['full_date']=pd.to_datetime(data1['full_date']).dt.date
data1['tm_season_name'] = data1['tm_season_name'].str[0:4]
location=pd.read_csv('location.csv')
data1['zip']=data1['zip'].apply(lambda x: str(x.encode('utf-8')))
location['zip']=location['zip'].apply(lambda x: str(x))
data1['zip']=data1['zip'].apply(lambda x : x[1:] if x.startswith('0') else x)

data1['zip'] = data1['zip'].str[0:5]
data1=pd.merge(data1,location,how='left',on='zip')
data1['digit']=data1['zip'].apply(lambda x: str.isdigit(x))
def f(row):
    if (row['value']==0 and row['country'] == 'United States') or (row['value']==0 and row['country'] == 'US') or (row['value']==0 and row['country'] == 'USA') or (row['value']==0 and row['country'] == '') or (row['zip']=='' and row['country'] == '') or (row['zip']=='' and row['country'] == 'United States') or (row['zip']=='' and row['country'] == 'USA') or (row['zip']=='' and row['country'] == 'US'):
        val = 0
    elif row['country'] == 'United States' or row['country'] == 'US' or row['country'] == 'USA' or (row['country'] == '' and row['digit']==True)  :
        val = 1
    else:
        val = 2
    return val

data1['C'] = data1.apply(f, axis=1)

data2=data1.loc[data1['tm_season_name']=='2016']
data3=data1.loc[data1['tm_season_name']=='2015']
data2=data2.groupby('tm_acct_id').count().reset_index()
repeat=pd.merge(data3,data2,how='inner',on=['tm_acct_id'])
repeat_n_75=repeat[repeat['C_x']>0]
repeat_y_75=repeat[repeat['C_x']==0]
all_n_75=data3[data3['C']>0]
all_y_75=data3[data3['C']==0]

'''
data4=data1.loc[data1['full_date']>=pastweek]
data5=data1.loc[data1['full_date']<pastweek]

data5=data5.loc[data5['tm_season_name'].isin(['2016','2017'])]
data5=data5.groupby('tm_acct_id').count().reset_index()
week2016=pd.merge(data4,data5,how='inner',on=['tm_acct_id'])

season_repeat_1_n_75=year2016[year2016['C_x']>0]
week_repeat_1_n_75=week2016[week2016['C_x']>0]
season_repeat_1_y_75=year2016[year2016['C_x']==0]
week_repeat_1_y_75=week2016[week2016['C_x']==0]

data6=data1.loc[data1['tm_season_name']=='2015']
data6=data6.groupby('tm_acct_id').count().reset_index()
year2015=pd.merge(data2,data6,how='inner',on=['tm_acct_id'])




week2015=pd.merge(data4,data6,how='inner',on=['tm_acct_id'])
season_repeat_2_n_75=year2015[year2015['C_x']>0]
week_repeat_2_n_75=week2015[week2015['C_x']>0]
season_repeat_2_y_75=year2015[year2015['C_x']==0]
week_repeat_2_y_75=week2015[week2015['C_x']==0]

year2016group=year2016.groupby('tm_acct_id').count().reset_index()
year20152016=pd.merge(year2015,year2016group,how='inner',on=['tm_acct_id'])
week2016group=week2016.groupby('tm_acct_id').count().reset_index()
week20152016=pd.merge(week2015,week2016group,how='inner',on=['tm_acct_id'])

year20152016_n_75=year20152016[year20152016['C_x_x']>0]
week20152016_n_75=week20152016[week20152016['C_x_x']>0]
year20152016_y_75=year20152016[year20152016['C_x_x']==0]
week20152016_y_75=week20152016[week20152016['C_x_x']==0]

season_n_75=data2[data2['C']>0]
week_n_75=data4[data4['C']>0]
season_y_75=data2[data2['C']==0]
week_y_75=data4[data4['C']==0]
'''