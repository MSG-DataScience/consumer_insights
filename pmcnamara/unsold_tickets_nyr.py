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
import statsmodels.api as sm

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

# DATA IMPORT #

query = '''
with table1
as
(
select 
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as cust_sum,
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as max_date,
tm_acct_id,
high_volume_buyer_flag,
tm_event_name,
tm_price_code_desc,
tm_section_name, 
tm_event_date,
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
zip,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat S
WHERE tm_acct_id NOT IN (-1,-2)
AND tm_comp_name IS NOT NULL
AND tm_season_name IN (
'2016-17 New York Rangers',
'2017-18 New York Rangers')
--and ticket_type_price_level IN (/*'Half Plan'*/'New Fulls', 'Individuals' ) 
--and tm_event_name='ENK0412E'
--and tm_section_name = 1
--and tm_row_name= 14
--and tm_seat_num = 6
)
select 
*
from table1
where tickets_sold>0
and cust_sum >0
and max_date=tickets_add_datetime
'''

data = pd.read_sql(query, engine)
ESR0918E
ESR0920E
ESR0925E


#data = data[data['ticket_type_price_level'] == 'Individuals']
preseasons = ['ENR0927E','ENR0929E','ENR1006E','ENRPRE','ESR0918E','ESR0920E','ESR0925E']
#presale = list(pd.date_range(datetime.date(2016,9,20), datetime.date(2016,9,26)).date) + list(pd.date_range(datetime.date(2016,9,7), datetime.date(2016,9,11)).date)
data = data[~data['tm_event_name'].isin(preseasons)]

#data[(data['tm_season_name'] != '2015-16 New York Knicks') & (data['ticket_type_price_level'] == 'Individuals')].groupby(['high_volume_buyer_flag','sale_date']).sum()['cust_sum'].reset_index().pivot(columns='high_volume_buyer_flag', index = 'sale_date').fillna(0).to_csv('indy_sales.csv')
# CONVERTING TO DATE OBJECTS #
data['tm_event_date'] = data['tm_event_date'].dt.date
data['event_month'] = pd.DatetimeIndex(data['tm_event_date']).month
data['event_year'] = pd.DatetimeIndex(data['tm_event_date']).year
data['sale_date'] = data['tickets_add_datetime'].dt.date
data['days_out'] = (data['tm_event_date'] - data['sale_date']).dt.days
data['day_name'] = pd.to_datetime(data['tm_event_date']).dt.weekday_name

data[(data['ticket_type_price_level'] == 'Individuals')].groupby(['high_volume_buyer_flag','sale_date']).sum()['tickets_sold'].reset_index().pivot(columns='high_volume_buyer_flag', index = 'sale_date').fillna(0).to_csv('indy_sales_nyr.csv')