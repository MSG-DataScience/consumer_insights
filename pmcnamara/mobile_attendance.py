import pandas as pd
import sqlalchemy
import redshift_sqlalchemy
import statsmodels.api as sm
import matplotlib.pyplot as plt
import numpy as np

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@msgbiadb-prod.cqp6htpq4zp6.us-east-1.rds.amazonaws.com:5432/msgbiadb")

mobile_data_query = '''
SELECT e.tm_event_date::DATE, e.tm_event_time, e.tm_event_name, a.attendance_mobile_flag, scan_time, c.tm_acct_id, e.tm_season_name 
FROM ads_main.f_attendance_event_seat a
JOIN ads_main.d_event_plan e on e.event_plan_id=a.event_plan_id AND e.tm_org_name IN ('RANGERS','Knicks')
JOIN ads_main.d_customer_account c on c.customer_account_id=a.customer_account_id and c.ads_source=c.ads_source and tm_acct_id NOT IN ('-1','-2')
LIMIT 500;
'''
mobile_data2 = pd.read_sql(mobile_data_query, engine, parse_dates = ['tm_event_date'])

# CLEAN MOBILE SCAN DATA #
mobile_data2['email'] = mobile_data2['email'].str.lower()
mobile_data2.ix[mobile_data2['ticket_type'] == 'Lounges', 'ticket_type'] = 'Groups'
mobile_data2.ix[mobile_data2['ticket_type'] == 'Pick Plan', 'ticket_type'] = 'Partials'
mobile_data2.ix[mobile_data2['ticket_type'] == 'Half Plan', 'ticket_type'] = 'Partials'
mobile_data2.ix[mobile_data2['ticket_type'] == 'Mini Plan', 'ticket_type'] = 'Partials'

# HIGH-LEVEL STATS #
mobile_data2.groupby(['ads_source','tm_acct_id']).max()['attendance_mobile_flag'].mean()
mobile_data2[mobile_data2['attendance_mobile_flag'] == 1][['ads_source','tm_acct_id','ticket_type','attendance_mobile_flag']].drop_duplicates().groupby(['ads_source','ticket_type']).count()[['attendance_mobile_flag']].JOIN(mobile_data2[['tm_acct_id','ads_source','ticket_type']].drop_duplicates().groupby(['ads_source','ticket_type']).count())
mobile_data2.groupby(['ads_source','tm_acct_id','ticket_type']).max()['attendance_mobile_flag'].reset_index().drop('tm_acct_id',axis = 1).groupby(['ads_source','ticket_type']).mean().round(2)
acct_rates = mobile_data2.groupby(['ads_source','tm_acct_id','tm_event_date']).max()['attendance_mobile_flag'].reset_index().groupby(['ads_source','tm_acct_id']).mean()
first_mobile_scan = mobile_data2[mobile_data2['attendance_mobile_flag'] == 1].groupby(['ads_source','tm_acct_id']).min()['tm_event_date'].reset_index().rename(columns = {'tm_event_date':'first_scan'})

# ANALYZING RATES FOR PEOPLE WHO DO AND DO NOT USE THE MOBILE TICKET SCAN, AS WELL AS BEFORE AND AFTER #
pre_post_rates = pd.merge(mobile_data2, first_mobile_scan, on = ['ads_source','tm_acct_id'], how = 'left')
pre_post_rates[pd.isnull(pre_post_rates['first_scan'])][['tm_acct_id','ads_source','tm_event_date']].drop_duplicates().groupby(['ads_source','tm_acct_id']).count().describe()
pre_post_rates = pre_post_rates.groupby(['ads_source','tm_acct_id','ticket_type','tm_event_date','first_scan']).max()['attendance_mobile_flag'].reset_index()
#pre_post_rates = pre_post_rates[pre_post_rates['tm_event_date'] > pre_post_rates['first_scan']]
pre_post_rates.dropna(subset = ['first_scan'])[['tm_acct_id','ads_source','tm_event_date','attendance_mobile_flag']].drop_duplicates().mean()
pre_post_rates.dropna(subset = ['first_scan'])[['tm_acct_id','ads_source','tm_event_date']].drop_duplicates().groupby(['ads_source','tm_acct_id']).count().describe()
pre_post_rates[pd.isnull(pre_post_rates['first_scan'])][['tm_acct_id','ads_source','tm_event_date']].drop_duplicates().groupby(['ads_source','tm_acct_id']).count().describe()
# multiple games vs single game #
games_attended = mobile_data2[['tm_acct_id','ads_source','tm_event_date','email']].drop_duplicates().groupby(['ads_source','tm_acct_id','email']).count().rename(columns = {'tm_event_date':'games_attended'})

# COMBINING WITH DEMOGRAPHICS DATA #
demo_indexes = pd.read_csv('/Users/mcnamarp/Downloads/Customer Infobase_170425.txt', usecols = ['EMAIL','INDIVIDUAL_ID'])
demo_indexes['EMAIL'] = demo_indexes['EMAIL'].str.lower()
demo_data = pd.read_csv('/Users/mcnamarp/Downloads/acxiom_customerinfobase_mh.txt').drop(['PERSONIX_LIFESTAGE','PERSONIX_CLUSTER_GROUP','PERSONIX_TIER','DISCRET_INCOME_SCORE','RELIGION','ETHNICITY','INCOME_HIGHRANGES','OCCUPATION'], axis = 1)
demo_data.dropna(subset = ['DISCRET_INCOME_PERCENTILE','AGE','GENDER','EDUCATION','MARITAL_STATUS','PRESENCE_OF_CHILDREN','NETWORTH','INCOME_LOWRANGES'], inplace = True)
demo_data = pd.merge(demo_data, demo_indexes, on = ['INDIVIDUAL_ID']).set_index('INDIVIDUAL_ID').rename(columns = {'EMAIL':'email','INCOME_LOWRANGES':'INCOME','PRESENCE_OF_CHILDREN':'CHILDREN'})
demo_data.replace({'NETWORTH':{'Less than or equal to $0':0,'$1 - $4,999':1,'$5,000 - $9,999':2,'$10,000 - $24,999':4,'$25,000 - $49,999':8,'$50,000 - $99,999':16,'$100,000 - $249,999':32,'$250,000 - $499,999':64,'$500,000 - $999,999':128,'$1,000,000 - $1,999,999':256,'$2,000,000 +':512}}, inplace = True)

data = mobile_data2.groupby(['email','ads_source']).max()['attendance_mobile_flag'].reset_index()
data = pd.merge(data, demo_data, on = ['email'])
data = pd.merge(data, games_attended.reset_index(), on = ['ads_source','email'])

# GROUP BY MEANS #
pd.DataFrame(data.groupby('EDUCATION').count()['email']).JOIN(data.groupby('EDUCATION').mean()['attendance_mobile_flag'].round(2))
pd.DataFrame(data.groupby('CHILDREN').count()['email']).JOIN(data.groupby('CHILDREN').mean()['attendance_mobile_flag'].round(2))
pd.DataFrame(data.groupby('GENDER').count()['email']).JOIN(data.groupby('GENDER').mean()['attendance_mobile_flag'].round(2))
pd.DataFrame(data.groupby('MARITAL_STATUS').count()['email']).JOIN(data.groupby('MARITAL_STATUS').mean()['attendance_mobile_flag'].round(2))
pd.DataFrame(data.groupby(['GENDER','MARITAL_STATUS']).count()['email']).JOIN(data.groupby(['GENDER','MARITAL_STATUS']).mean()['attendance_mobile_flag'].round(2))
pd.DataFrame(data.groupby('INCOME').count()['email']).JOIN(data.groupby('INCOME').mean()['attendance_mobile_flag'].round(2))

# USING LOGISTIC REGRESSION TO GET ODDS RATIOS FOR DIFFERENT CHARACTERISTICS #
education_dummies = pd.get_dummies(data['EDUCATION'])
networth_dummies = pd.get_dummies(data['NETWORTH'])
income_dummies = pd.get_dummies(data['INCOME'])
children_dummies = pd.get_dummies(data['CHILDREN'])
gender_dummies = pd.get_dummies(data['GENDER'])
marital_dummies = pd.get_dummies(data['MARITAL_STATUS'])
#team_dummies = pd.get_dummies(data['ads_source'])
dummies = children_dummies.JOIN(marital_dummies).JOIN(gender_dummies).rename(columns = {'YES':'CHILDREN'}).JOIN(income_dummies)
dummies.drop(['NO','MALE','MARRIED'], axis = 1, inplace = True)

model_data = data.drop(['EDUCATION','INCOME','CHILDREN','GENDER','MARITAL_STATUS','ads_source','NETWORTH','tm_acct_id'], axis = 1).JOIN(dummies)
logit = sm.Logit(model_data['attendance_mobile_flag'], model_data.drop(['attendance_mobile_flag','email'], axis = 1))
result = logit.fit()
result.summary()
np.exp(result.params)

#############################################################################
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
mobile_by_group[mobile_by_group['attendance_mobile_flag'] == 1].groupby(['ads_source','category']).count()[['attendance_mobile_flag']].JOIN(mobile_by_group.groupby(['ads_source','category']).count()[['customer_account_id']])
first_mobile_scan = mobile_data[mobile_data['attendance_mobile_flag'] == 1].groupby(['customer_account_id','ads_source']).min()['date'].reset_index()

# NEW MOBILE SCANS BY GAME #
daily_new_scans = first_mobile_scan.groupby(['ads_source','date']).count().rename(columns = {'customer_account_id':'new'}).reset_index()

# ROLLING SUM MOBILE SCANS BY DAY #
rolling_sum = daily_new_scans.groupby(['ads_source']).cumsum().rename(columns = {'new':'cumulative'})
rolling_sum['ads_source'] = daily_new_scans['ads_source']
rolling_sum['date'] = daily_new_scans['date']
mobile_scan_counts = pd.merge(daily_new_scans, rolling_sum, on = ['ads_source','date'])

test = pd.merge(ticket_data[['tm_acct_id','ads_source','ticket_product_description','tm_price_code']].drop_duplicates(), mobile_data2[['tm_acct_id','ads_source','ticket_type','tm_price_code']].drop_duplicates(), on = ['tm_acct_id','ads_source','tm_price_code'], how = 'left')
test[pd.isnull(test['ticket_type_price_level'])]['tm_price_code'].value_counts()
test2 = pd.merge(ticket_data[['tm_acct_id','ads_source','ticket_product_description','tm_price_code']].drop_duplicates(), mobile_data2[['tm_acct_id','ads_source','ticket_type','tm_price_code']].drop_duplicates(), on = ['tm_acct_id','ads_source','tm_price_code'], how = 'right')
test2[pd.isnull(test2['ticket_product_description'])]['tm_price_code'].value_counts()
test3 = pd.merge(ticket_data[['tm_acct_id','ads_source','ticket_product_description','tm_price_code']].drop_duplicates(), mobile_data2[['tm_acct_id','ads_source','ticket_type','tm_price_code']].drop_duplicates(), on = ['tm_acct_id','ads_source','tm_price_code'])

tickets_by_group_query = '''
SELECT ads_source, substring(ads_source_file from position('201' in ads_source_file) for 8) AS date, COUNT(*) 
FROM ads_main.d_customer_account
WHERE email_address IS NOT NULL
GROUP BY ads_source, substring(ads_source_file from position('201' in ads_source_file) for 8)
'''
tickets_by_group = pd.read_sql (tickets_by_group_query, engine, parse_dates = ['date'])