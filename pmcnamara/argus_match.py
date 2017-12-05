import pandas as pd
import numpy as np
import datetime
import sqlalchemy

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

original = pd.read_csv('/Users/mcnamarp/Downloads/knicks_games_years.csv')
argus_scores_indy = pd.read_csv('/Users/mcnamarp/Downloads/ALL_Knicks_MODEL_RESULT.csv')
argus_scores_plan = pd.read_csv('/Users/mcnamarp/Downloads/Knicks_Multi_Games_MODEL_RESULT.csv')

hashed_emails_query = '''
SELECT DISTINCT tm_acct_id, ads_main.f_sha256(upper(email_address)), email_address
FROM ads_main.d_customer_account
WHERE email_address != ''
'''
crm_emails = pd.read_sql(hashed_emails_query, engine)

existing_planholders_query = '''
SELECT DISTINCT tm_acct_id, email_address
FROM ads_main.t_ticket_sales_event_seat
WHERE tm_season_name = '2017-18 New York Knicks' AND tm_plan_total_events IS NOT NULL
'''
existing_plans = pd.read_sql(existing_planholders_query, engine)

data_i = pd.merge(crm_emails, argus_scores_indy, left_on = ['f_sha256'], right_on = ['hashed_email'])
data_i = data_i.drop(['f_sha256','hashed_email'], axis = 1).drop_duplicates()
data_i = data_i[~data_i['email_address'].isin(existing_plans['email_address'])]
data_i = data_i[~data_i['tm_acct_id'].isin(existing_plans['tm_acct_id'])]

data_p = pd.merge(crm_emails, argus_scores_plan, left_on = ['f_sha256'], right_on = ['hashed_email'])
data_p = data_p.drop(['f_sha256','hashed_email','Rank'], axis = 1).drop_duplicates()
data_p = data_p[~data_p['email_address'].isin(existing_plans['email_address'])]
data_p = data_p[~data_p['tm_acct_id'].isin(existing_plans['tm_acct_id'])]

data_i[['email_address','tm_acct_id','Score_Knicks_Fans']].drop_duplicates().to_csv('/Users/mcnamarp/Downloads/knicks_indy_scores.csv', index = False)

data_p[['email_address','tm_acct_id','Score_Knicks_Fans']].drop_duplicates().to_csv('/Users/mcnamarp/Downloads/knicks_multi_scores.csv', index = False)

# WESTCHESTER DATA EXPORT #
existing_wc_planholders_query = '''
SELECT DISTINCT email_address
FROM ads_main.t_ticket_sales_event_seat
WHERE tm_season_name = '2017-18 Westchester Knicks' AND tm_plan_total_events IS NOT NULL
'''
existing_wc_plans = pd.read_sql(existing_wc_planholders_query, engine)

wc_zips = pd.read_csv('/Users/mcnamarp/Downloads/westchester_zips.csv', dtype = 'str')

westchester_accounts_query = '''SELECT DISTINCT email_address FROM ads_main.d_customer_account WHERE zip IN (''' + str(list(wc_zips['zip'])) + ')'
westchester_accounts_query = westchester_accounts_query.replace('[','').replace(']','')
westchester_emails = pd.read_sql(westchester_accounts_query, engine)
westchester_emails = westchester_emails[~westchester_emails['email_address'].isin(existing_wc_plans['email_address'])]
westchester_accounts = pd.merge(data_i, westchester_emails, on = 'email_address')
westchester_accounts['email_address'].drop_duplicates().to_csv('wck_argus.txt', index = False)