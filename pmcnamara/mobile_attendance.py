import pandas as pd
import sqlalchemy
import redshift_sqlalchemy

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@msgbiadb-prod.cqp6htpq4zp6.us-east-1.rds.amazonaws.com:5432/msgbiadb")
mobile_attendance_query = '''
SELECT DISTINCT customer_account_id, ads_source, event_plan_id, attendance_mobile_flag
FROM ads_main.f_attendance_event_seat
WHERE customer_account_id IS NOT NULL AND ads_source IN ('KNICKS','RANGERS')
'''
mobile_attendance = pd.read_sql(mobile_attendance_query, engine)

customer_account_query = '''
SELECT DISTINCT tm_acct_id, ads_source, customer_account_id, email_address
FROM ads_main.d_customer_account
WHERE ads_source IN ('KNICKS','RANGERS') AND tm_acct_id NOT IN (-1,-2) 
'''
customer_accounts = pd.read_sql(customer_account_query, engine)

ticket_type_query = '''
SELECT DISTINCT tm_acct_id, ads_source, ticket_product_description, tm_event_date::DATE,
CASE WHEN ticket_product_description = 'Renewals' THEN 5 WHEN ticket_product_description = 'New Fulls' THEN 4 WHEN ticket_product_description = 'Partials (FSEs)' THEN 3 WHEN ticket_product_description = 'Individuals' THEN 2 WHEN ticket_product_description IN ('Group Tickets','Lounges') THEN 1 ELSE 0 END AS description_level
FROM ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2016-17 New York Rangers','2016-17 New York Knicks') AND tm_acct_id NOT IN (-1,-2) 
'''
ticket_data = pd.read_sql(ticket_type_query, engine)
ticket_data.ix[ticket_data['ticket_product_description'] == 'Lounges', 'ticket_product_description'] = 'Group Tickets'
labels = ticket_data.groupby('tm_acct_id').max()[['ticket_product_description','description_level']].reset_index().rename(columns = {'ticket_product_description':'category'}).drop(['description_level'], axis = 1)
ticket_data = pd.merge(ticket_data, labels, on = ['tm_acct_id'], how = 'left').drop(['ticket_product_description','description_level'], axis = 1)

mobile_data = pd.merge(customer_accounts, ticket_data, on = ['tm_acct_id','ads_source'])
mobile_data = pd.merge(mobile_data, mobile_attendance, on = ['customer_account_id','ads_source'])
mobile_data['email_address'] = mobile_data['email_address'].astype(str)

# MOBILE USAGE BY TICKET GROUP #
mobile_by_group = mobile_data.groupby(['customer_account_id','ads_source','category']).max()['attendance_mobile_flag'].reset_index()
mobile_by_group[mobile_by_group['attendance_mobile_flag'] == 1].groupby(['ads_source','category']).count()[['attendance_mobile_flag']].join(mobile_by_group.groupby(['ads_source','category']).count()[['customer_account_id']])
first_mobile_scan = mobile_data[mobile_data['attendance_mobile_flag'] == 1].groupby(['customer_account_id','ads_source']).min()['date'].reset_index()

# NEW MOBILE SCANS BY GAME #
daily_new_scans = first_mobile_scan.groupby(['ads_source','date']).count().rename(columns = {'customer_account_id':'new'}).reset_index()

# ROLLING SUM MOBILE SCANS BY DAY #
rolling_sum = daily_new_scans.groupby(['ads_source']).cumsum().rename(columns = {'new':'cumulative'})
rolling_sum['ads_source'] = daily_new_scans['ads_source']
rolling_sum['date'] = daily_new_scans['date']
mobile_scan_counts = pd.merge(daily_new_scans, rolling_sum, on = ['ads_source','date'])

# COMBINING WITH DEMOGRAPHICS DATA #
demo_indexes = pd.read_csv('/Users/mcnamarp/Downloads/Customer Infobase_170425.txt', usecols = ['EMAIL','INDIVIDUAL_ID'])
demo_indexes['EMAIL'] = demo_indexes['EMAIL'].str.lower()
demo_data = pd.read_csv('/Users/mcnamarp/Downloads/acxiom_customerinfobase_mh.txt').drop(['PERSONIX_LIFESTAGE','PERSONIX_CLUSTER_GROUP','PERSONIX_TIER','DISCRET_INCOME_SCORE','RELIGION','ETHNICITY','INCOME_HIGHRANGES'], axis = 1)
demo_data = pd.merge(demo_data, demo_indexes, on = ['INDIVIDUAL_ID'])

data = mobile_data[mobile_data['attendance_mobile_flag'] == 1][['email_address','category','ads_source']].drop_duplicates()
data = pd.merge(data, demo_data, left_on = ['email_address'], right_on = ['EMAIL']).drop(['EMAIL'], axis = 1)


tickets_by_group_query = '''
SELECT ads_source, substring(ads_source_file from position('201' in ads_source_file) for 8) AS date, COUNT(*) 
FROM ads_main.d_customer_account
WHERE email_address IS NOT NULL
GROUP BY ads_source, substring(ads_source_file from position('201' in ads_source_file) for 8)
'''
tickets_by_group = pd.read_sql (tickets_by_group_query, engine, parse_dates = ['date'])

mobile_data_query = '''
SELECT e.tm_event_date::DATE, a.attendance_mobile_flag, a.manifest_seat_id, c.tm_acct_id, c.email_address, coalesce(pc.ticket_type_price_level,'Individuals') AS ticket_type, e.ads_source
FROM ads_main.f_attendance_event_seat a
join ads_main.d_event_plan e on e.event_plan_id=a.event_plan_id AND e.tm_season_name IN ('2016-17 New York Rangers','2016-17 New York Knicks')
join ads_main.d_customer_account c on c.customer_account_id=a.customer_account_id and c.ads_source=c.ads_source
left join ads_main.d_price_code pc on pc.price_code_id=a.price_code_id
'''
mobile_data2 = pd.read_sql(mobile_data_query, engine, parse_dates = ['tm_event_date'])
mobile_data2.ix[mobile_data2['ticket_type'] == 'Lounges', 'ticket_type'] = 'Groups'
mobile_data2.ix[mobile_data2['ticket_type'] == 'Pick Plan', 'ticket_type'] = 'Mini Plan'
mobile_data2.groupby(['ads_source','tm_acct_id']).max()['attendance_mobile_flag'].mean()
mobile_data2.groupby(['ads_source','tm_acct_id','ticket_type']).max()['attendance_mobile_flag'].reset_index().groupby(['ads_source','ticket_type']).mean()

mobile_data2[['tm_acct_id','ads_source','ticket_type']].drop_duplicates().groupby('ticket_type').count()

test = pd.merge(ticket_data[['tm_acct_id','ads_source','ticket_product_description','tm_price_code']].drop_duplicates(), mobile_data2[['tm_acct_id','ads_source','ticket_type','tm_price_code']].drop_duplicates(), on = ['tm_acct_id','ads_source','tm_price_code'], how = 'left')
test[pd.isnull(test['ticket_type_price_level'])]['tm_price_code'].value_counts()
test2 = pd.merge(ticket_data[['tm_acct_id','ads_source','ticket_product_description','tm_price_code']].drop_duplicates(), mobile_data2[['tm_acct_id','ads_source','ticket_type','tm_price_code']].drop_duplicates(), on = ['tm_acct_id','ads_source','tm_price_code'], how = 'right')
test2[pd.isnull(test2['ticket_product_description'])]['tm_price_code'].value_counts()
test3 = pd.merge(ticket_data[['tm_acct_id','ads_source','ticket_product_description','tm_price_code']].drop_duplicates(), mobile_data2[['tm_acct_id','ads_source','ticket_type','tm_price_code']].drop_duplicates(), on = ['tm_acct_id','ads_source','tm_price_code'])
