# -*- coding: utf-8 -*-
"""
Created on Thu Nov 09 12:30:23 2017

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
country,
tm_event_time
from ads_main.t_ticket_sales_event_seat
where 1=1
and tm_season_name IN ('2016-17 New York Knicks') 
and tm_event_name not in('ENKPRE')
)
select *
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
and tm_event_date<'2017-11-13'
and tm_event_time>'16:00:00'
order by full_date
'''
data = pd.read_sql(query, engine)

data['tm_event_date']=pd.to_datetime(data['tm_event_date']).dt.date
data['full_date']=pd.to_datetime(data['full_date']).dt.date
data['a']=data['tm_event_date']-data['full_date']
data['a']=data['a'].apply(lambda x: x.total_seconds())
data=data[data.a==0]
data['tickets_add_datetime']=pd.to_datetime(data['tickets_add_datetime']).dt.hour

a=data.groupby(['tickets_add_datetime']).count()