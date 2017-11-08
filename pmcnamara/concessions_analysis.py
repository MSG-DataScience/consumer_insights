import pandas as pd
import numpy as np
import datetime
import sqlalchemy

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

concessions_data_query = '''
SELECT DISTINCT ads_fmb_sales_product_detail_id, D.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_3, fmb_category_lvl_4
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_2 = 'Concessions'
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.ads_fmb_sales_hdr_id = D.ads_fmb_sales_hdr_id AND H.tendered_datetime >= '2015-07-13'
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id
'''
data = pd.read_sql(concessions_data_query, engine)

data['date'] = data['tendered_datetime'].dt.date
data['time'] = data['tendered_datetime'].dt.time
data['event_month'] = pd.DatetimeIndex(data['tendered_datetime']).month
data['event_year'] = pd.DatetimeIndex(data['tendered_datetime']).year
data['day_name'] = pd.to_datetime(data['tendered_datetime']).dt.weekday_name

data['line_item_profit'] = data['gross'] - data['cost']