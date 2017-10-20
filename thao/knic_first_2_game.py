# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 14:29:09 2017

@author: haot
"""
from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import numpy as np
import datetime
import sqlalchemy
import matplotlib.pylab as plt
import sqlalchemy
import xgboost as xgb
from sklearn.externals import joblib

engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

renewals_query = '''
with TABLE_1
  AS (
SELECT tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
FROM msgbiadb.ads_main.t_ticket_sales_event_seat
WHERE tm_event_name IN ('ENK1029E')  and tm_comp_name='Not Comp' and ticket_group_flag='N' and ticket_type_price_level = 'Individuals' and acct_type_desc!='Trade Desk' and acct_type_desc!= 'Sponsor'
GROUP BY tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
having count(*)=1
),
  Table_2 AS (
  SELECT *
  FROM msgbiadb.ads_main.t_ticket_sales_event_seat
  WHERE tm_event_name IN ('ENK1029E')  and tm_comp_name='Not Comp' and ticket_group_flag='N' and  ticket_type_price_level = 'Individuals' and acct_type_desc!='Trade Desk' and acct_type_desc!= 'Sponsor')

      SELECT Table_2.*
      FROM TABLE_1
        LEFT JOIN Table_2
          ON
             TABLE_1.tm_section_name = Table_2.tm_section_name
             AND TABLE_1.tm_row_name = Table_2.tm_row_name
             AND TABLE_1.tm_seat_num = Table_2.tm_seat_num
             AND TABLE_1.tickets_add_datetime = Table_2.tickets_add_datetime
             AND TABLE_1.tm_event_name =Table_2.tm_event_name

'''
data = pd.read_sql(renewals_query, engine)
data.to_csv('1.csv')
# CLEAN SALES DATA #
