import pandas as pd
import numpy as np
import datetime
import sqlalchemy
import matplotlib.pyplot as plt
import seaborn as sns

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

# importing ticket scans #
ticket_scans_query = '''
SELECT tm_season_name, tm_event_date, tm_event_time, tm_event_name, scan_time, COUNT(*) FROM (
SELECT DISTINCT e.tm_event_date::DATE, e.tm_event_time, e.tm_event_name, scan_time, c.tm_acct_id, e.tm_season_name, tm_section_name, tm_row_name, tm_seat_num
FROM ads_main.f_attendance_event_seat a
JOIN ads_main.d_event_plan e on e.event_plan_id=a.event_plan_id AND e.tm_org_name IN ('RANGERS','Knicks') AND scan_time != ''
JOIN ads_main.d_customer_account c on c.customer_account_id=a.customer_account_id and c.ads_source=c.ads_source and tm_acct_id NOT IN ('-1','-2'))
GROUP BY tm_season_name, tm_event_date, tm_event_time, tm_event_name, scan_time;
'''
ticket_scans = pd.read_sql(ticket_scans_query, engine)

# importing F&B/merch sales #
# NEED TO AGGREAGATE BY MINUTE, CATEGORY AND CHASE SQUARE VS. NON-CHASE SQUARE #
food_bev_merch_query = '''
SELECT DISTINCT H.tendered_datetime::DATE AS event_date, E.ads_source, ads_fmb_sales_product_detail_id, H.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_2, fmb_category_lvl_3, fmb_category_lvl_4, tendered_terminal_dim_id AS terminal
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_event_master E ON E.event_plan_id= D.event_plan_id AND E.ads_source IN ('RANGERS','KNICKS')
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_2 IN ('Concessions','Stores') AND store_name = 'Madison Square Garden' AND product_quantity > 0
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.transaction_data_id = D.transaction_data_id
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id AND D.void_reason_dim_id = 0;
'''
food_bev_merch_sales = pd.read_sql(food_bev_merch_query, engine)