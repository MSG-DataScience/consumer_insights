import time
import pandas as pd
import numpy as np
import datetime
from sklearn import preprocessing
import sqlalchemy
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
tm_section_name, 
tm_event_date,
tm_row_name, 
tm_seat_num,
tickets_sold,
tickets_total_revenue, 
tickets_add_datetime, 
tm_season_name,
mpd_indy_rank,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
where tm_season_name IN ('2016-17 New York Knicks','2017-18 New York Knicks')
AND ticket_type_price_level = 'Individuals'
AND tm_comp_name  = 'Not Comp'
AND SUBSTRING(company_name,1,11) != 'Eventellect'
)
select 
*
from table1
where tickets_sold>0
and cust_sum >0
and max_date=tickets_add_datetime
'''
data = pd.read_sql(query, engine)
data_ = data.copy()
#pd.read_csv('/Users/mcnamarp/Downloads/with_table1_as___select__SUM_tickets_sol.csv')
#data = data[data['ticket_type_price_level'] == 'Individuals']
preseasons = ['ENK1008E','ENK1010E','ENKPRE','ENK1015E','ESN1007E','ESN1012E','ESN1016E','ESNPRE','KTEST','ESN1013E'you
]
data = data[~data['tm_event_name'].isin(preseasons)]

# CONVERTING TO DATE OBJECTS #
data['tm_event_date'] = pd.to_datetime(data['tm_event_date']).dt.date
data['sale_date'] = pd.to_datetime(data['tickets_add_datetime']).dt.date
data = data[data['sale_date'] <= data['tm_event_date']]

# TICKET SALES PER EVENT BY DATE # 
total_seats = data.groupby(['tm_section_name','tm_row_name','tm_seat_num']).count().reset_index()
seats_15 = data[data['tm_season_name'] == '2015-16 New York Knicks'].groupby(['tm_section_name','tm_row_name','tm_seat_num']).sum().rename(columns = {'cust_sum':'seats_15'})['seats_15'].reset_index()
seats_16 = data[data['tm_season_name'] == '2016-17 New York Knicks'].groupby(['tm_section_name','tm_row_name','tm_seat_num']).sum().rename(columns = {'cust_sum':'seats_16'})['seats_16'].reset_index()
total_seats = pd.merge(total_seats, seats_15, on = ['tm_section_name','tm_row_name','tm_seat_num'], how = 'left')
total_seats = pd.merge(total_seats, seats_16, on = ['tm_section_name','tm_row_name','tm_seat_num'], how = 'left')

inventory_query = '''
SELECT tm_event_name, report_as_of_date AS sale_date, seats_available AS inventory FROM (
with
   curr_yr_games as (
      select
             distinct a.tm_season_name, a.tm_event_name, a.tm_event_name_long, a.tm_event_date, a.tm_event_time,
                          a.event_plan_id, a.mpd_indy_rank, a.mpd_game_num, a.mpd_opponent
         from ads_main.d_event_plan   a        
               where a.tm_season_name in ('2016-17 New York Knicks','2017-18 New York Knicks')
         and a.tm_event_name not in ('ENK1008E','ENK1010E','ENKPRE','ENK1015E','ESN1007E','ESN1012E','ESN1016E','ESNPRE')
         and a.tm_plan_event_id = -1
         order by a.tm_season_name, a.tm_event_date, a.tm_event_time
)
select
    curr_yr_games.tm_season_name,
    curr_yr_games.tm_event_name,
    curr_yr_games.tm_event_name_long,
    curr_yr_games.tm_event_date,
       curr_yr_games.mpd_indy_rank, curr_yr_games.mpd_game_num, curr_yr_games.mpd_opponent,
       --------
    prd.full_date report_as_of_date,
       --dist_status.host_dist_status, dist_status.host_dist_name,
       --seat_class.tm_seat_class_code, seat_class.tm_seat_class_name,
    --
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
where prd.full_date between to_date('20-apr-2016','dd-mon-yyyy') and current_date
group by
    curr_yr_games.tm_season_name,
    curr_yr_games.tm_event_name,
    curr_yr_games.tm_event_name_long,
    curr_yr_games.tm_event_date,
       curr_yr_games.mpd_indy_rank,
       curr_yr_games.mpd_game_num,
       curr_yr_games.mpd_opponent,
       --------
    prd.full_date
       --dist_status.host_dist_status,
       --dist_status.host_dist_name,
       --seat_class.tm_seat_class_code,
       --seat_class.tm_seat_class_name
order by curr_yr_games.tm_season_name, curr_yr_games.tm_event_date, prd.full_date, seats_available desc);
'''

# Inventory by Time Category #
inventory = pd.read_sql(inventory_query, engine)
inventory_ = inventory.copy()
inventory = inventory.replace(0, np.nan)
inventory.fillna(method='backfill', inplace = True)
tickets_sold = data.groupby(['tm_event_name','sale_date']).sum()['tickets_sold'].reset_index()
tickets_sold = pd.merge(inventory, tickets_sold, on = ['tm_event_name','sale_date'], how = 'left').fillna(0)
tickets_sold['sales_1'] = tickets_sold[['tm_event_name','tickets_sold']].groupby(['tm_event_name']).shift(1)
tickets_sold['sales_2'] = tickets_sold[['tm_event_name','tickets_sold']].groupby(['tm_event_name']).shift(2)
tickets_sold['sales_3'] = tickets_sold[['tm_event_name','tickets_sold']].groupby(['tm_event_name']).shift(3)
tickets_sold.dropna(inplace = True)
#tickets_sold['sales_4'] = tickets_sold[['tm_event_name','tickets_sold']].groupby(['tm_event_name']).shift(4)
#tickets_sold['sales_5'] = tickets_sold[['tm_event_name','tickets_sold']].groupby(['tm_event_name']).shift(5)
#tickets_sold['rolling_7'] = tickets_sold[['tm_event_name','tickets_sold']].groupby(['tm_event_name']).rolling(7, min_periods=1).mean()['tickets_sold'].values

# reducing ticket sales file to dates, game ID and ranks #
data = data.drop(['nbr_bought','cust_sum','max_date','tm_acct_id','tickets_sold','tm_section_name','tm_row_name','tm_seat_num','tickets_total_revenue','tickets_add_datetime','ticket_sell_location_name'], axis = 1)
data = data.drop_duplicates()

# joining ticket sales & inventory #
data = pd.merge(data, tickets_sold, on = ['tm_event_name','sale_date'], how = 'right')

# adding rank & event month to each record #
day_ranks = data[['tm_event_name','tm_event_date','mpd_indy_rank','tm_season_name']].drop_duplicates().dropna()
data = pd.merge(data.drop(['tm_event_date','mpd_indy_rank','tm_season_name'], axis = 1), day_ranks, on = 'tm_event_name')

data['days_out'] = (data['tm_event_date'] - data['sale_date']).dt.days 
data['day_name'] = pd.to_datetime(data['tm_event_date']).dt.weekday_name
data['event_month'] = pd.DatetimeIndex(data['tm_event_date']).month
data['event_year'] = pd.DatetimeIndex(data['tm_event_date']).year
data['event_month'] = data['event_month'].replace({10:'0',11:'1',12:'2',1:'3',2:'4',3:'5',4:'6'}).astype(int)

# MARKETING IMPACTS #
#impressions = pd.read_excel('/Users/mcnamarp/Downloads/16-17 Knicks Daily Impressions.xlsx', sheet_name = 'Summary')
#impressions.index = impressions['Date'].dt.date
#impressions = (impressions/1000000).drop(['Date'], axis = 1).sort_index()
impressions = pd.read_csv('/Users/mcnamarp/Downloads/FB_Files/fb_impressions.csv').rename(columns = {'Unnamed: 0':'Date'}).set_index('Date')
#web_impressions['DATE'] = pd.to_datetime(social_impressions['DATE']).dt.date
#web_impressions = social_impressions.groupby('DATE').sum()
#impressions.join(web_impressions)

# lag variables #
imp_lag1 = impressions.shift(1).fillna(0)
imp_lag1.columns = [i + '_1' for i in impressions.columns]
imp_lag2 = impressions.shift(2).fillna(0)
imp_lag2.columns = [i + '_2' for i in impressions.columns]
impressions = impressions.join(imp_lag1).join(imp_lag2).reset_index()
impressions.rename(columns = {'Date':'sale_date'}, inplace = True)
impressions['sale_date'] = pd.to_datetime(impressions['sale_date']).dt.date
data = pd.merge(data, impressions, on = 'sale_date', how = 'left')
#data.fillna(0, inplace = True)

# BUILDING FEATURES #
data['event_month'] = data['event_month'].replace({10:'0',11:'1',12:'2',1:'3',2:'4',3:'5',4:'6'}).astype(int)
data['mpd_indy_rank'] =  data['mpd_indy_rank'].replace({'A+':15,'A':13,'B':10,'C':6,'D':'3'}).astype(int)

# Time Categories #
data = data[data['days_out'] != -1]
data['days_out_cat'] = data['days_out'].astype(str)
data.ix[data['days_out'] >29, 'days_out_cat'] = 'Planners'
data.ix[(data['days_out'] > 14) & (data['days_out'] < 30), 'days_out_cat'] = 'Early'
data.ix[(data['days_out'] > 8) & (data['days_out'] < 15), 'days_out_cat'] = '2Weeks'
data['days_out_cat'] = 'D' + data['days_out_cat'].astype(str)

# Creating Dummies #
day_dummies = pd.get_dummies(data['day_name'])
#grade_dummies = pd.get_dummies(data['mpd_indy_rank']).rename(columns={'':'NA'})
days_cat_dummies = pd.get_dummies(data['days_out_cat'])
season_dummies = pd.get_dummies(data['tm_season_name'])
dummies = day_dummies.join(days_cat_dummies).join(season_dummies)
data = data.join(dummies)

# promo dummies #
early_games = ['ENK1029E','ENK1102E','ENK1106M','ENK1109E','ENK1114E','ENK1116E','ENK1120M','ENK1122E','ENK1125E','ENK1128E','ENK1202E','ENK1204E']
preseason_displays = pd.date_range(datetime.date(2016, 9, 26), datetime.date(2016, 10, 2))
preseason_displays = pd.to_datetime(preseason_displays).date
data['ps_display'] = 0
data.ix[data['sale_date'].isin(preseason_displays), 'ps_display'] = 1

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
keeps = ['inventory','tickets_sold','tm_event_name','event_month','days_out','ps_display','mpd_indy_rank','sales_1','sales_2','sales_3','fb_game_specific'] + list(day_dummies.columns) + list(days_cat_dummies.columns) + list(season_dummies.columns) + list(impressions.columns)
reg_data = data[keeps]

shifted_onsale = pd.date_range(datetime.date(2016, 4, 1), datetime.date(2016, 8, 24)).date
reg_data = reg_data[~reg_data['sale_date'].isin(shifted_onsale)]
onsale = [datetime.date(2016,9,20), datetime.date(2016,9,27), datetime.date(2017,9,12)]
reg_data['onsale'] = 0
reg_data.ix[reg_data['sale_date'].isin(onsale), 'onsale'] = 1

reg_data.set_index(['tm_event_name','sale_date'], inplace = True)
reg_data.fillna(0, inplace = True)
#reg_data = reg_data[(reg_data['D2Weeks'] == 0) | (reg_data['DEarly'] == 0) | (reg_data['DPlanners'] == 0)]
#reg_data.drop(['D0','D1','D2','D3','D4','D5','D6','D7','D8'], axis = 1, inplace = True)
#for i in days_cat_dummies.columns:
#	reg_data[i + '*S'] = reg_data[i]*reg_data['2017-18 New York Knicks']

# RUN REGRESSION #
#reg_data['tickets_sold'] = np.log(reg_data['tickets_sold']).replace(-np.inf, 0)
result = sm.OLS(reg_data['tickets_sold'], reg_data.drop(['tickets_sold'], axis = 1)).fit()
result.summary()

reg_data['predicted'] = result.predict(reg_data.drop(['tickets_sold'], axis = 1))
reg_data.ix[reg_data['predicted'] < 0, 'predicted'] = 0

['sale_date','Paid Social','Display','Search','Radio','Paid Social_1','Display_1','Search_1','Radio_1','Paid Social_2','Display_2','Search_2','Radio_2']
'''
# COMPS ANALYSIS # 
comps = data[data['tm_comp_name'] != 'Not Comp']
comps = comps[~comps['tm_comp_name'].isin(['MADISON CLUB','NBA','Media','LEAGUE','PLAYERS'])]
comps.groupby(['tm_season_name','tm_comp_name']).count()['tm_acct_id'].reset_index().set_index('tm_season_name').join(data.groupby('tm_season_name').count()['cust_sum'])
(comps[comps['tm_season_name'] == '2016-17 New York Knicks'].groupby('days_out_cat').count()['nbr_bought']/len(comps[comps['tm_season_name'] == '2016-17 New York Knicks'])).round(2)
(comps[comps['tm_season_name'] == '2015-16 New York Knicks'].groupby('days_out_cat').count()['nbr_bought']/len(comps[comps['tm_season_name'] == '2015-16 New York Knicks'])).round(2)

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
'''
'''
fb_promos = list(pd.date_range(datetime.date(2016, 10, 27), datetime.date(2016, 11, 6))) + list(pd.date_range(datetime.date(2016, 11, 11), datetime.date(2016, 11, 16)))
fb_promos = pd.to_datetime(fb_promos).date
fb_game_promos = list(pd.date_range(datetime.date(2016, 11, 4), datetime.date(2016, 11, 6))) + list(pd.date_range(datetime.date(2016, 11, 7), datetime.date(2016, 11, 9))) + list(pd.date_range(datetime.date(2017, 3, 13), datetime.date(2017, 3, 16))) + list(pd.date_range(datetime.date(2017, 2, 6), datetime.date(2017, 2, 10))) + list(pd.date_range(datetime.date(2017, 2, 24), datetime.date(2017, 2, 27)))
fb_game_promos = pd.to_datetime(fb_game_promos).date
radio_promos = list(pd.date_range(datetime.date(2017, 10, 16), datetime.date(2017, 10, 29)))
radio_promos = pd.to_datetime(radio_promos).date
data['fb_promos'] = 0
data.ix[data['sale_date'].isin(fb_promos), 'fb_promos'] = 1
data['fb_game_promos'] = 0
data.ix[data['sale_date'].isin(fb_game_promos), 'fb_game_promos'] = 1
data['radio'] = 0
data.ix[data['sale_date'].isin(preseason_displays), 'radio'] = 1
'''
'''
result_bo = sm.OLS(reg_data_bo['tickets_sold'], reg_data_bo.drop(['tickets_sold'], axis = 1)).fit()
result_bo.summary()
result_ol = sm.OLS(reg_data_ol['tickets_sold'], reg_data_ol.drop(['tickets_sold'], axis = 1)).fit()
result_ol.summary()
'''