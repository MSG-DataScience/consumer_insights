# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 14:05:29 2017

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

engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")
query = '''
with table1
as
(
select
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as cust_sum,
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as max_date,
tm_event_date,
tickets_sold,
tickets_total_revenue,
tickets_add_datetime,
ticket_transaction_date,
tm_comp_name,
tm_event_name_long,
ticket_type_price_level,
ticket_sell_location_name,
acct_type_desc,
ticket_group_flag,
house_seat_flag,
full_date,
zip,
country

from ads_main.t_ticket_sales_event_seat
where 1=1
and tm_season_name IN ('2017-18 New York Knicks'
) 
--and tm_event_name In('ENK1008E','ENK1010E','ENK1015E','ENK1029E','ENK1102E','ENK1106M','ENK1109E','ENK1114E','ENK1116E','ENK1120M')
)
select
tm_event_date,full_date,zip,country
from table1
where tickets_sold>0
and cust_sum >0
and max_date=tickets_add_datetime
and ticket_group_flag='N'
and tm_comp_name = 'Not Comp'
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk'
and acct_type_desc!= 'Sponsor'
and house_seat_flag='N'
and full_date<'2017-11-3'
order by full_date
'''
data = pd.read_sql(query, engine)
location=pd.read_csv('location.csv')
data['zip']=data['zip'].apply(lambda x: str(x.encode('utf-8')))
location['zip']=location['zip'].apply(lambda x: str(x))
filter = data["zip"] != ""
data1 = data[filter]
data1= data1[pd.notnull(data['zip'])]
data1['zip'] = data1['zip'].str[0:5]
data2=pd.merge(data1,location,how='left',on='zip')
data2['digit']=data2['zip'].apply(lambda x: str.isdigit(x))
def f(row):
    if (row['value']==0 and row['country'] == 'United States') or (row['value']==0 and row['country'] == 'US') or (row['value']==0 and row['country'] == 'USA') or (row['value']==0 and row['country'] == '') :
        val = 0
    elif row['country'] == 'United States' or row['country'] == 'US' or row['country'] == 'USA' or (row['country'] == '' and row['digit']==True)  :
        val = 1
    else:
        val = 2
    return val

data2['C'] = data2.apply(f, axis=1)
'''
data2['tm_event_date']=pd.to_datetime(data2['tm_event_date']).dt.date
data2['full_date']=pd.to_datetime(data2['full_date']).dt.date
data2['a']=data2['tm_event_date']-data2['full_date']
data2['a']=data2['a'].apply(lambda x: x.total_seconds())
data2=data2[data2.a<100000]
'''
print(len(data2[data2['C']==0].index))
print(len(data2[data2['C']==1].index))
print(len(data2[data2['C']==2].index))