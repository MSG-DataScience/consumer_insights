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
from ads_main.t_ticket_sales_event_seat
where 1=1
and tm_season_name IN (
'2015-16 New York Knicks',
'2016-17 New York Knicks')
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

#data = data[data['ticket_type_price_level'] == 'Individuals']
preseasons = ['ENK1008E','ENK1010E','ENKPRE','ENK1015E','ESN1007E','ESN1012E','ESN1016E','ESNPRE']
data = data[~data['tm_event_name'].isin(preseasons)]


# CONVERTING TO DATE OBJECTS #
data['tm_event_date'] = data['tm_event_date'].dt.date
data['event_month'] = pd.DatetimeIndex(data['tm_event_date']).month
data['event_year'] = pd.DatetimeIndex(data['tm_event_date']).year
data['sale_date'] = data['tickets_add_datetime'].dt.date
data['days_out'] = (data['tm_event_date'] - data['sale_date']).dt.days
data['day_name'] = pd.to_datetime(data['tm_event_date']).dt.weekday_name

# AVG TICKET SALES PER EVENT BY MONTH # 
#data.groupby(['event_year','event_month','tm_event_name']).sum()['cust_sum'].reset_index().groupby(['event_year','event_month']).mean().astype(int)
total_seats = data.groupby(['tm_section_name','tm_row_name','tm_seat_num']).count().reset_index()
seats_15 = data[data['tm_season_name'] == '2015-16 New York Knicks'].groupby(['tm_price_code_desc','tm_section_name','tm_row_name','tm_seat_num']).sum().rename(columns = {'cust_sum':'seats_15'})['seats_15'].reset_index()
seats_16 = data[data['tm_season_name'] == '2016-17 New York Knicks'].groupby(['tm_price_code_desc','tm_section_name','tm_row_name','tm_seat_num']).sum().rename(columns = {'cust_sum':'seats_16'})['seats_16'].reset_index()
total_seats = pd.merge(total_seats, seats_15, on = ['tm_section_name','tm_row_name','tm_seat_num'], how = 'left')
total_seats = pd.merge(total_seats, seats_16, on = ['tm_section_name','tm_row_name','tm_seat_num'], how = 'left')
'''
# COMPS ANALYSIS #
comps = data[data['tm_comp_name'] != 'Not Comp']
comps = comps[~comps['tm_comp_name'].isin(['MADISON CLUB','NBA','Media','LEAGUE','PLAYERS'])]
comps.groupby(['tm_season_name','tm_comp_name']).count()['tm_acct_id'].reset_index().set_index('tm_season_name').join(data.groupby('tm_season_name').count()['cust_sum'])
(comps[comps['tm_season_name'] == '2016-17 New York Knicks'].groupby('days_out_cat').count()['nbr_bought']/len(comps[comps['tm_season_name'] == '2016-17 New York Knicks'])).round(2)
(comps[comps['tm_season_name'] == '2015-16 New York Knicks'].groupby('days_out_cat').count()['nbr_bought']/len(comps[comps['tm_season_name'] == '2015-16 New York Knicks'])).round(2)
'''
# BUILDING FEATURES #
data['event_month'] = data['event_month'].replace({10:'0',11:'1',12:'2',1:'3',2:'4',3:'5',4:'6'}).astype(int)

# Time Categories #
data['days_out_cat'] = '1Planners'
data.ix[(data['days_out'] > 7) & (data['days_out'] < 29), 'days_out_cat'] = '2Early'
data.ix[(data['days_out'] > 1) & (data['days_out'] < 8), 'days_out_cat'] = '3Week-Ahead'
data.ix[data['days_out'] ==0, 'days_out_cat'] = '4Last-Minute'

# Inventory by Time Category #
inventory = data.groupby(['tm_event_name','sale_date']).sum()['tickets_sold'].reset_index()
inventory['running_sum'] = data.groupby(['tm_event_name','sale_date']).sum()['tickets_sold'].reset_index().groupby('tm_event_name')['tickets_sold'].cumsum().shift(1).fillna(0)
inventory['inventory'] = 18778 - inventory['running_sum']
tickets_sold = data[data['ticket_type_price_level'] == 'Individuals'].groupby(['tm_event_name','sale_date']).sum()['tickets_sold'].reset_index()
tickets_sold = pd.merge(inventory.drop(['tickets_sold','running_sum'], axis = 1), tickets_sold, on = ['tm_event_name','sale_date'], how = 'left').fillna(0)

# Creating Dummies #
day_dummies = pd.get_dummies(data['day_name'])
grade_dummies = pd.get_dummies(data['mpd_indy_rank']).rename(columns={'':'NA'})
days_cat_dummies = pd.get_dummies(data['days_out_cat'])
data = data.join(day_dummies).join(grade_dummies).join(days_cat_dummies)

# promo dummies #
early_games = ['ENK1029E','ENK1102E','ENK1106M','ENK1109E','ENK1114E','ENK1116E','ENK1120M','ENK1122E','ENK1125E','ENK1128E','ENK1202E','ENK1204E']
preseason_displays = pd.date_range(datetime.date(2016, 9, 26), datetime.date(2016, 10, 2))
preseason_displays = pd.to_datetime(preseason_displays).date
fb_promos = list(pd.date_range(datetime.date(2016, 10, 27), datetime.date(2016, 11, 6))) + list(pd.date_range(datetime.date(2016, 11, 11), datetime.date(2016, 11, 16)))
fb_promos = pd.to_datetime(fb_promos).date
fb_game_promos = list(pd.date_range(datetime.date(2016, 11, 4), datetime.date(2016, 11, 6))) + list(pd.date_range(datetime.date(2016, 11, 7), datetime.date(2016, 11, 9))) + list(pd.date_range(datetime.date(2017, 3, 13), datetime.date(2017, 3, 16))) + list(pd.date_range(datetime.date(2017, 2, 6), datetime.date(2017, 2, 10))) + list(pd.date_range(datetime.date(2017, 2, 24), datetime.date(2017, 2, 27)))
fb_game_promos = pd.to_datetime(fb_game_promos).date
data['ps_display'] = 0
data.ix[data['sale_date'].isin(preseason_displays), 'ps_display'] = 1
data['fb_promos'] = 0
data.ix[data['sale_date'].isin(fb_promos), 'fb_promos'] = 1
data['fb_game_promos'] = 0
data.ix[data['sale_date'].isin(fb_game_promos), 'fb_game_promos'] = 1

# game-specific interaction dummies #
data['fb_game_specific'] = 0
jazz_days = pd.to_datetime(list(pd.date_range(datetime.date(2016, 11, 4), datetime.date(2016, 11, 6)))).date
data.ix[(data['tm_event_name'] == 'ENK1106M') & (data['sale_date'].isin(jazz_days)), 'fb_game_specific'] = 1
nets1_days = pd.to_datetime(list(pd.date_range(datetime.date(2016, 11, 7), datetime.date(2016, 11, 9)))).date
data.ix[(data['tm_event_name'] == 'ENK1109E') & (data['sale_date'].isin(nets1_days)), 'fb_game_specific'] = 1
nets2_days = pd.to_datetime(list(pd.date_range(datetime.date(2017, 3, 13), datetime.date(2017, 3, 16)))).date
data.ix[(data['tm_event_name'] == 'ENK0316E') & (data['sale_date'].isin(nets2_days)), 'fb_game_specific'] = 1
lakers_days = datetime.date(2017, 2, 6)
data.ix[(data['tm_event_name'] == 'ENK0206E') & (data['sale_date'] == lakers_days), 'fb_game_specific'] = 1
clippers_days = [datetime.date(2017, 2, 7),datetime.date(2017, 2, 8)]
data.ix[(data['tm_event_name'] == 'ENK0208E') & (data['sale_date'].isin(clippers_days)), 'fb_game_specific'] = 1
nuggets_days = [datetime.date(2017, 2, 9),datetime.date(2017, 2, 10)]
data.ix[(data['tm_event_name'] == 'ENK0210E') & (data['sale_date'].isin(nuggets_days)), 'fb_game_specific'] = 1
raptors_days = pd.to_datetime(list(pd.date_range(datetime.date(2017, 2, 24), datetime.date(2017, 2, 27)))).date
data.ix[(data['tm_event_name'] == 'ENK0227E') & (data['sale_date'].isin(raptors_days)), 'fb_game_specific'] = 1

# REGRESSION DATA FRAME #
keeps = ['tm_event_name','sale_date','event_month','days_out','ticket_type_price_level','ps_display','fb_promos','fb_game_promos','fb_game_specific','ticket_sell_location_name'] + list(day_dummies.columns) + list(grade_dummies.columns) + list(days_cat_dummies.columns)
reg_data = data[keeps].drop_duplicates()
reg_data = pd.merge(reg_data, tickets_sold, on = ['tm_event_name','sale_date'], how = 'right')
shifted_onsale = pd.date_range(datetime.date(2016, 4, 1), datetime.date(2016, 8, 24)).date
reg_data = reg_data[~reg_data['sale_date'].isin(shifted_onsale)]
reg_data = reg_data[reg_data['ticket_type_price_level'] == 'Individuals'].drop(['ticket_type_price_level'], axis = 1).drop_duplicates()
reg_data.set_index(['tm_event_name','sale_date'], inplace = True)
reg_data['1P*I'] = reg_data['1Planners']*reg_data['inventory']
reg_data['2E*I'] = reg_data['2Early']*reg_data['inventory']
reg_data['3W*I'] = reg_data['3Week-Ahead']*reg_data['inventory']
reg_data['4L*I'] = reg_data['4Last-Minute']*reg_data['inventory']

reg_data_bo = reg_data[reg_data['ticket_sell_location_name'] == 'Box Office'].drop(['ticket_sell_location_name'], axis = 1)
reg_data_ol = reg_data[reg_data['ticket_sell_location_name'] == 'Internet'].drop(['ticket_sell_location_name'], axis = 1)

# RUN REGRESSION #
result = sm.OLS(reg_data['tickets_sold'], reg_data.drop(['tickets_sold','ticket_sell_location_name'], axis = 1)).fit()
result.summary()
result_bo = sm.OLS(reg_data_bo['tickets_sold'], reg_data_bo.drop(['tickets_sold'], axis = 1)).fit()
result_bo.summary()
result_ol = sm.OLS(reg_data_ol['tickets_sold'], reg_data_ol.drop(['tickets_sold'], axis = 1)).fit()
result_ol.summary()


x = data[data['tm_season_name'] == '2016-17 New York Knicks'].groupby(['tm_event_name','tm_event_name_long','tm_event_date','ticket_type_price_level']).sum()['cust_sum'].reset_index(level=3).join(data.groupby(['tm_event_name','tm_event_name_long','tm_event_date']).count()['tickets_sold'])
x['prop'] = (x['cust_sum'] / x['tickets_sold']).round(3)
type_avgs = x.groupby('ticket_type_price_level').mean()['prop'].reset_index().rename(columns = {'prop':'type_avg'}).round(3)
x = pd.merge(x.reset_index(), type_avgs, on = ['ticket_type_price_level'], how = 'left')
x['diff'] = (x['prop'] - x['type_avg']).round(3)
#x[x['tm_event_name'] == 'ENK1106M']

data[(data['tm_event_name_long'] == 'NYK vs. Detroit Pistons')].groupby(['tm_event_date','ticket_type_price_level','days_out_cat']).mean()['tickets_total_revenue'].astype(int)


y = data[data['tm_season_name'] == '2016-17 New York Knicks'].groupby(['tm_event_name','tm_event_name_long','tm_event_date','ticket_type_price_level','days_out_cat']).mean()['tickets_total_revenue'].reset_index()
y = pd.merge(y, data.groupby(['tm_event_name','days_out_cat']).mean()['tickets_total_revenue'].reset_index().rename(columns = {'tickets_total_revenue':'game_avg'}), on = ['tm_event_name','days_out_cat'])
y['diff'] = (y['tickets_total_revenue'] / y['game_avg']).round(2)

data.ix[data['ticket_type_price_level'].isin(['Mini Plan','New Fulls','Renewals','Pick Plan','Lounges','Half Plan']), 'ticket_type_price_level'] = 'Plans & Lounges'
data[data['tm_event_name'].isin(promos)].groupby(['ticket_type_price_level','days_out_cat']).sum()['tickets_sold']/data[data['tm_event_name'].isin(promos)]['tickets_sold'].sum()
data[~data['tm_event_name'].isin(promos)].groupby(['ticket_type_price_level','days_out_cat']).sum()['tickets_sold']/data[~data['tm_event_name'].isin(promos)]['tickets_sold'].sum()