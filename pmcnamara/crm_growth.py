import pandas as pd
import numpy as np
import datetime
import sqlalchemy

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

existing_emails_query = '''
SELECT DISTINCT email_address FROM ads_main.d_customer_account
WHERE ads_active_flag = 'Y' AND name_type = 'I'
'''

current_emails = pd.read_sql(existing_emails_query, engine)

gate_data = pd.read_csv('/Users/mcnamarp/Downloads/wifi_data.csv').drop(['Unnamed: 0'], axis = 1)

len(set(gate_data['Email'])) - len(set(gate_data['Email']) & set(current_emails['email_address']))