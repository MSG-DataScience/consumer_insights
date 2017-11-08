# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 13:27:58 2017

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
from sklearn import preprocessing
import sqlalchemy
import statsmodels.api as sm
from itertools import product

engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

query1='''WITH table1 as(SELECT *
FROM ads_main.f_ticket_exchange_event_seat a
left join ads_main.d_event_plan b on a.event_plan_id=b.event_plan_id
where a.event_plan_id in ( SELECT event_plan_id from ads_main.d_event_plan WHERE tm_event_name in('ESN1030E')))
 SELECT * from table1 LEFT JOIN ads_main.d_manifest_seat c on table1.manifest_seat_id=c.manifest_seat_id'''
 
query2='''WITH curr_yr_games as (
      select
             distinct a.tm_season_name, a.tm_event_name, a.tm_event_name_long, a.tm_event_date, a.tm_event_time,
                          a.event_plan_id, a.mpd_indy_rank, a.mpd_game_num, a.mpd_opponent
         from ads_main.d_event_plan   a
               where a.tm_season_name in ('2017-18 New York Knicks' )
         and a.tm_event_name  in ('ESN1030E')
         and a.tm_plan_event_id = -1
    
         order by a.tm_season_name, a.tm_event_date, a.tm_event_time
)
select
    curr_yr_games.tm_season_name,
    curr_yr_games.tm_event_name,
    curr_yr_games.tm_event_name_long,
    curr_yr_games.tm_event_date,
    prd.full_date report_as_of_date,
      section_name,
  row_name,
      avg(abs(seats_avail_full_price)) avg_price,
       sum(a.seats_avail_count) seats_available
from  ads_main.d_date prd
      join curr_yr_games
        on 1 = 1
              and prd.full_date <= (curr_yr_games.tm_event_date + 1)
       join ads_main.f_avail_event_seats a
       on a.event_plan_id = curr_yr_games.event_plan_id
          and a.seats_avail_report_day_id <= prd.day_id
       left join ads_main.d_seat_class seat_class
       on seat_class.seat_class_dim_id = a.seat_class_dim_id
    left join ads_main.d_host_dist_status dist_status
         on dist_status.host_dist_status_dim_id = a.host_dist_status_dim_id
where prd.full_date between to_date('01-sep-2017','dd-mon-yyyy') and current_date
group by
    curr_yr_games.tm_season_name,
    curr_yr_games.tm_event_name,
    curr_yr_games.tm_event_name_long,
    curr_yr_games.tm_event_date,
    prd.full_date,
  section_name,
  row_name
    order by tm_event_name,section_name,row_name

'''
data1 = pd.read_sql(query1, engine)
data2 = pd.read_sql(query2, engine)
data4=pd.merge(data1,data2,how='left',left_on=['tm_event_name','tm_section_name','tm_row_name'],right_on=['tm_event_name','section_name','row_name'])
data5=data4[['report_as_of_date','section_name','row_name','avg_price','tickets_posting_price','ticket_add_datetime']]
data5 = data5[np.isfinite(data5['tickets_posting_price'])]
data5['ticket_add_datetime']=pd.to_datetime(data5['ticket_add_datetime'])
data5 = data5[data5['tickets_posting_price']<5000]
data5 = data5[data5['tickets_posting_price']>50]
data5['ticket_add_datetime']=pd.to_datetime(data5['ticket_add_datetime']).dt.date
data5['a']=data5['ticket_add_datetime']-data5['report_as_of_date']
data5['a']=data5['a'].apply(lambda x: x.total_seconds())

data5=data5[data5.a<0]
y=data5[data5.avg_price.gt(data5.tickets_posting_price)].groupby('report_as_of_date').size().reset_index()
z=data5.groupby('report_as_of_date').size().reset_index()
'''
select * from table3
left join table2 on 1=1
    and table3.tm_event_name=table2.tm_event_name
    and table3.section_name= table3.section_name
    and table3.row_name= table3.row_name
ORDER BY table3.tm_event_name,report_as_of_date,section_name,row_name,avg_price
'''
