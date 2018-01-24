import pandas as pd
import numpy as np
import datetime
import sqlalchemy
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
import seaborn as sns

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

billy_joel_dates = ['1/9/15','2/18/15','3/9/15','4/3/15','5/28/15','6/20/15','7/1/15','8/20/15','9/26/15','10/21/15','11/19/15','12/17/15','1/7/2016','2/13/2016','3/15/2016','4/15/2016','5/27/2016','6/17/2016','7/20/2016','8/9/2016','10/28/2016','11/21/2016','11/30/2016','12/17/2016','1/11/2017','2/22/2017','3/3/2017','4/14/2017','5/25/17','7/5/17','8/21/17','9/30/2017','10/20/17','11/18/17']

concessions_sales_query = '''
SELECT DISTINCT H.tendered_datetime::DATE AS event_date, ads_fmb_sales_product_detail_id, H.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_3, fmb_category_lvl_4, tendered_terminal_dim_id AS terminal
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_2 = 'Concessions' AND store_name = 'Madison Square Garden' AND product_quantity > 0 AND fmb_category_lvl_3 != '8th Floor'
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.transaction_data_id = D.transaction_data_id AND H.tendered_datetime::DATE IN (''' + str(list(billy_joel_dates)).replace('[','').replace(']','') + ''')
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id AND D.void_reason_dim_id = 0 AND product_name LIKE '%%POPCORN%%'
'''
sales = pd.read_sql(concessions_sales_query, engine)
sales = pd.read_csv('/Users/mcnamarp/Downloads/sales.csv')

merch_sales_query_bj = '''
SELECT DISTINCT H.tendered_datetime::DATE AS event_date, ads_fmb_sales_product_detail_id, H.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_3, fmb_category_lvl_4, tendered_terminal_dim_id AS terminal
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_4 = 'Store 8th Floor'
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.transaction_data_id = D.transaction_data_id AND H.tendered_datetime::DATE IN (''' + str(list(billy_joel_dates)).replace('[','').replace(']','') + ''')
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id;
'''
merch_bj = pd.read_sql(merch_sales_query_bj, engine)

merch_sales_query_nykr = '''
SELECT H.tendered_datetime::DATE AS event_date, ads_fmb_sales_product_detail_id, H.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_2, fmb_category_lvl_3, fmb_category_lvl_4, tendered_terminal_dim_id AS terminal
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_event_master E ON E.event_plan_id= D.event_plan_id AND E.ads_source IN ('RANGERS','KNICKS')
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_2 IN ('Concessions','Stores') AND store_name = 'Madison Square Garden' AND product_quantity > 0
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.transaction_data_id = D.transaction_data_id
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id AND D.void_reason_dim_id = 0;
'''
merch_nykr = pd.read_sql(merch_sales_query_nykr, engine)

popcorn_query = '''
SELECT DISTINCT H.tendered_datetime::DATE AS event_date, ads_fmb_sales_product_detail_id, H.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_3, fmb_category_lvl_4, tendered_terminal_dim_id AS terminal
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_2 = 'Concessions' AND store_name = 'Madison Square Garden' AND product_quantity > 0 AND fmb_category_lvl_3 = '8th Floor'
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.transaction_data_id = D.transaction_data_id AND H.tendered_datetime::DATE IN ('1/9/15','2/18/15','3/9/15','4/3/15','5/28/15','6/20/15','7/1/15','8/20/15','9/26/15','10/21/15','11/19/15','12/17/15','1/7/2016','2/13/2016','3/15/2016','4/15/2016','5/27/2016','6/17/2016','7/20/2016','8/9/2016','10/28/2016','11/21/2016','11/30/2016','12/17/2016','1/11/2017','2/22/2017','3/3/2017','4/14/2017','5/25/17','7/5/17','8/21/17','9/30/2017','10/20/17','11/18/17')
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id AND D.void_reason_dim_id = 0 AND product_name IN ('POPCORN BOX','SM POP BUCKET','LG POP BUCKET')
'''
popcorn_bj = pd.read_sql(popcorn_query, engine)

sales['tendered_datetime'] = pd.to_datetime(sales['tendered_datetime'])
sales['sale_date'] = sales['tendered_datetime'].dt.date
sales['sale_time'] = sales['tendered_datetime'].dt.time
sales['event_time'] = datetime.time(20,0)
sales['time'] = sales['tendered_datetime'].dt.time
sales['event_month'] = pd.DatetimeIndex(sales['tendered_datetime']).month
sales['event_year'] = pd.DatetimeIndex(sales['tendered_datetime']).year
sales['day_name'] = pd.to_datetime(sales['tendered_datetime']).dt.weekday_name
# remove september show where almost no sales come in #
sales = sales[sales['event_date'] != datetime.date(2017,9,30)]
# remove seat vendor sales #
#sales = sales[sales['location_abv'] != 'FVA']
# fix chicken/fries issue #
sales['fmb_category_lvl_4'].replace({'Chicken/Fries':'Chicken / Fries'}, inplace = True)

# fixing shock top cost issue #
shock_top_sales = ['FF SHOCK TOP /BELGIAN WHITE ALE /CONCERT 24OZ /FS','FF SHOCK TOP /BELGIAN WHITE ALE /24OZ /FS']
sales.ix[(sales['product_name'].isin(shock_top_sales)), 'cost'] = 0.84*sales['product_quantity']

# fixing missing costs issue #
first_sale = sales[sales['cost'] > 0].groupby(['product_name']).min()['event_date'].reset_index()
first_sales = pd.merge(sales, first_sale, on = ['product_name','event_date'])
item_costs = first_sales[first_sales['product_quantity'] == 1].drop(['product_quantity'], axis = 1)[['product_name','cost']].groupby('product_name').max().reset_index().rename(columns = {'cost':'item_cost'})
sales = pd.merge(sales, item_costs, on = 'product_name')
sales.ix[pd.isnull(sales['cost']), 'cost'] = sales['product_quantity']*sales['item_cost']
sales['net_revenue'] = sales['gross'] - sales['cost']

#attendance_query = '''
#SELECT tm_event_date::DATE AS event_date, COUNT(*) AS attendance
#FROM ads_main.d_event_plan E
#JOIN (SELECT DISTINCT event_plan_id, section_name, row_name, seat FROM ads_main.f_attendance_event_seat) A ON E.event_plan_id = A.event_plan_id AND eatec_event_name = 'BILLY JOEL'
#GROUP BY tm_event_date::DATE
#'''
#attendance = pd.read_sql(attendance_query, engine)
'''W
event_station_sales = sales.groupby(['location_abv','event_date']).sum()[['gross','cost','net_revenue']].reset_index()

# IMPORTING STAFFING DATA #
xls = pd.ExcelFile('/Users/mcnamarp/Downloads/Staffing/billy joel concert dates.xlsx')
columns = ['Name','ID','Cash','Ca_2','Prep','Cook','Bar','SLS']
staffing = pd.DataFrame()
for i in xls.sheet_names:
	temp = pd.read_excel('/Users/mcnamarp/Downloads/Staffing/billy joel concert dates.xlsx', i, index_col=None, na_values=['NA'], header = None)
	temp.columns = columns
	temp['Date'] = pd.to_datetime(i.replace('_','/')).date()
	staffing = staffing.append(temp)

# clean up staffing data #
staffing = staffing.reset_index().drop(['index'], axis = 1)
staffing.dropna(subset = ['Name'], inplace = True)
staffing.dropna(subset = ['ID'], inplace = True)
name_drops = ['10TH FLOOR','7TH FLOOR','9TH FLOOR','8TH FLOOR','Scheduled Labor','Per Cap','Attendance','Labor %','Budgeted Labor','Proj Revenue','40.28','Healthy','Day Clean','Pool','Food Vending','Beverage Systems','In - Seat Kitchens','Break Room']
staffing = staffing[~staffing['Name'].isin(name_drops)]
id_drops = ['IC1001','FVA','IA6091','IB6061','IC6071','FA5060','CD6011']
staffing = staffing[~staffing['Name'].isin(id_drops)]
staffing = staffing[~(staffing['ID'] == 'FVA')]
staffing = staffing[~(staffing['ID'] == 'IC1001')]
value_drops = ['Cash','Prep','Bar','Cook','SLS']
staffing = staffing[~staffing['Cash'].isin(value_drops)]

# fixing missing staffing data #
staffing.ix[staffing['ID'] == 'FB8170', 'Cash'] = 2
staffing.ix[staffing['ID'] == 'CD6011', 'Cash'] = 1
staffing.ix[staffing['ID'] == 'CD6011', 'Cook'] = 1
staffing.ix[staffing['ID'] == 'CD6011', 'Prep'] = 2
staffing.ix[staffing['ID'] == 'FA6092', 'Cash'] = 1
staffing.ix[staffing['ID'] == 'FA6092', 'Cook'] = 1
staffing.ix[staffing['ID'] == 'FA6092', 'Prep'] = 3
staffing.ix[staffing['ID'] == 'FA6092', 'Ca_2'] = 2
staffing.ix[staffing['ID'] == 'FA6092', 'SLS'] = 1
staffing.ix[(staffing['ID'] == 'FB6051') & (staffing['Ca_2'] == 0), 'Ca_2'] = 1
staffing.ix[(staffing['ID'] == 'FB6051') & (staffing['Cash'] == 0), 'Ca_2'] = 2
staffing.ix[(staffing['ID'] == 'FB8160') & (staffing['Cash'] == 0), 'SLS'] = 1
staffing.ix[(staffing['ID'] == 'FB8160') & (staffing['Cash'] == 0), 'Ca_2'] = 1
staffing.ix[(staffing['ID'] == 'FB8160') & (staffing['Cash'] == 0), 'Cash'] = 2

# removing stores that have never opened #
temp = staffing.groupby('ID').max()[['Cash','Ca_2','Prep','Bar','Cook','SLS']]
store_keeps = list(temp.dropna(how='all').index)
staffing = staffing[staffing['ID'].isin(store_keeps)]
staffing.fillna(0, inplace = True)
staffing = staffing.set_index(['Name','ID','Date']).dropna(how = 'all').reset_index()
staffing['Date'] = staffing['Date'].dt.date

data = pd.merge(staffing.reset_index(), event_station_sales, left_on = ['ID','Date'], right_on = ['location_abv','event_date']).drop(['location_abv','event_date','index'], axis = 1)
data['employees'] =  data[['Cash','Prep','Bar','Cook','SLS','Ca_2']].T.sum()
data['labor_cost'] = data['employees'] * 60
data['profit'] = data['line_item_profit'] - data['labor_cost']

# analyzing stores that have multiple staffing counts #
staffing_counts = data.groupby(['ID','employees']).count().reset_index()['ID'].value_counts()
multi_staff = data[data['ID'].isin(staffing_counts[staffing_counts > 1].index)]
relationships = multi_staff[['ID','employees','profit']].groupby('ID').corr()['employees'].reset_index()
relationships = relationships[relationships['employees'] != 1]
relationships[relationships['employees'] > 0].mean()
relationships[relationships['employees'] < 0].mean()

event_level_sales = pd.merge(data.groupby('Date').sum()[['gross','cost','profit']].reset_index(), attendance, left_on = 'Date', right_on = 'event_date', how = 'left').drop(['event_date'], axis = 1)
event_level_sales['attendance'] = event_level_sales['attendance'].fillna(event_level_sales['attendance'].mean())
event_level_sales['profit_percap'] = event_level_sales['profit'] / event_level_sales['attendance']
event_level_sales['gross_percap'] = event_level_sales['gross'] / event_level_sales['attendance']

data = pd.merge(data, attendance, left_on = 'Date', right_on = 'event_date', how = 'left').drop(['event_date'], axis = 1)
data['attendance'].fillna(data['attendance'].mean(), inplace = True)
'''
# adding F&B categories #
categories = pd.read_csv('/Users/mcnamarp/Downloads/category and volume.csv')
categories.rename(columns = {'product':'product_name'}, inplace = True)
categories['type'] = 'other'
categories.ix[categories['category'].isin(['liquor','wine','beer']), 'type'] = 'alcohol'
categories.ix[categories['category'].isin(['soda','juice','water','lemonade','mug','energy drink','iced tea','coffee']), 'type'] = 'non_alc_bev'
categories.ix[categories['category'].isin(['ice cream','sandwich','hot dog','sushi','chips','cookie','popcorn','french fries','chicken fingers','hamburger','candy','pizza','nuts','pretzel','noodles','nachos']), 'type'] = 'food'
sales = pd.merge(sales, categories, on = 'product_name')

# analyzing distribution of sales #
gross = sales.groupby(['fmb_category_lvl_4','fmb_category_lvl_3','location_abv','event_date','day_name','event_month','event_year','type','category']).sum()['net_revenue'].reset_index()
gross.columns = ['stand','floor','ID','date','day','month','year','type','category','net_revenue']
#gross = gross[gross['gross'] > 100]
products = sales[['event_date','location_abv','type','category','product_name']].drop_duplicates().groupby(['event_date','location_abv','type','category']).count()['product_name'].reset_index().rename(columns = {'product_name':'product_count'})
gross = pd.merge(gross, products, left_on = ['date','ID','type','category'], right_on = ['event_date','location_abv','type','category']).drop(['event_date','location_abv'], axis = 1)
#gross = gross[gross['category'] != 'Portable Bar']
#gross = gross[gross['floor'] != 'Staff  Break Room']
# adding staffing #
#staffing['employees'] = staffing[['Cash','Prep','Bar','Cook','SLS','Ca_2']].T.sum()
#staffing.fillna(0, inplace = True)
#gross = pd.merge(gross, staffing.rename(columns = {'Date':'date','Name':'category'}), on = ['ID','date','category'], how = 'left')

# replace with english-friendly IDs #
old = ['Hot Dog','Bar','Daily Burger','Beer Pub','Chicken / Fries','Pizza','Portable Bar','Garden Bar','Sausage Boss','Simply Chicken','Farley Bar','Madison Bar','Hot Dog / 16 Handles','Bridge Pub','Carneige Deli','Sushi','Gluten-Free','Dunkin Donuts','Momofuk','Hill Country','Uncle Jacks','In Seat Service','Kobeyaki']
new = ['HD','B','DB','BP','CF','PZ','PB','GB','SB','SC','FB','MB','16HD','BRP','CD','SH','GF','DD','MFK','HC','UJ','ISS','KBY']
name_mapping = pd.DataFrame(index = old, data = new, columns = ['short_name']).reset_index()
gross = pd.merge(gross, name_mapping, left_on = 'stand', right_on = 'index').drop(['index'], axis = 1)
gross['sid'] = gross['floor'].apply(lambda x: x.split('t')[0]) + '_' + gross['short_name']
# renaming IDs for graphing purposes #
gross.ix[gross['ID'] == 'BD1003', 'sid'] = '11_B2'
gross.ix[gross['ID'] == 'FC1002', 'sid'] = '10_HD2'
gross.ix[gross['ID'] == 'FD1002', 'sid'] = '11_HD2'
gross.ix[gross['ID'] == 'FA6050', 'sid'] = '6_HD2'
gross.ix[gross['ID'] == 'FB6050', 'sid'] = '6_HD3'
gross.ix[gross['ID'] == 'FD6080', 'sid'] = '6_HD4'
gross.ix[gross['ID'] == 'FA8070', 'sid'] = '8_HD2'
gross.ix[gross['ID'] == 'FB8160', 'sid'] = '8_HD3'
gross.ix[gross['ID'] == 'FB8170', 'sid'] = '8_HD4'
gross.ix[gross['ID'] == 'FC8010', 'sid'] = '8_HD2' # combine for minimal presence #
gross.ix[gross['ID'] == 'FC8120', 'sid'] = '8_HD5'
gross.ix[gross['ID'] == 'FD8110', 'sid'] = '8_HD6'
gross.ix[gross['ID'] == 'FA8150', 'sid'] = '8_HD7'
gross.ix[gross['ID'] == 'FB9110', 'sid'] = '9_HD2'
gross.ix[gross['ID'] == 'FB9120', 'sid'] = '9_HD3'
gross.ix[gross['ID'] == 'FB6060', 'sid'] = '6_CF2'
gross.ix[gross['ID'] == 'CD6010', 'sid'] = '6_DB2'


rf_temp = pd.get_dummies(gross[gross.drop(['net_revenue','product_count'], axis = 1).columns])
rf_temp['net_revenue'] = gross['net_revenue']
rf_temp['product_quantity'] = gross['product_quantity']
predictions = pd.DataFrame()
features_results = {}
rf_results = {}
for i in range(100):
	rf_train = rf_temp.sample(frac = 0.65, random_state = i)
	X_train = rf_train.drop(['net_revenue'], axis = 1)
	y_train = rf_train['net_revenue']
	rf_test = rf_temp[~rf_temp.index.isin(rf_train.index)]
	X_test = rf_test.drop(['net_revenue'], axis = 1)
	y_test = rf_test['net_revenue']
#
	rf = RandomForestRegressor(n_estimators=150, random_state=i, n_jobs = -1, oob_score = True)
	rf.fit(X_train, y_train)
	features_results[i] = rf.feature_importances_
#c
	predicted_train = rf.predict(X_train)
	predicted_test = rf.predict(X_test)
	X_test['predicted'] = predicted_test
	X_test['gross'] = y_test
	predictions = predictions.append(X_test['predicted'])
	test_score = r2_score(y_test, predicted_test)
	rf_results[i] = np.round(test_score, 3)

average_predictions = pd.DataFrame(predictions.mean()).join(rf_temp).rename(columns={0:'predicted'})
average_predictions['diff'] = average_predictions['predicted'] - average_predictions['net_revenue']
average_predictions['diff_pct'] = average_predictions['diff']/average_predictions['net_revenue']
average_predictions['abs_diff'] = abs(average_predictions['predicted'] - average_predictions['net_revenue'])
average_predictions['abs_diff_pct'] = average_predictions['abs_diff']/average_predictions['net_revenue']
gross[gross.index.isin(average_predictions[average_predictions['diff'] > 2000].index)]['category'].value_counts()

rf_temp = rf_temp.join(average_predictions[['diff','diff_pct','abs_diff','abs_diff_pct']])

features_impacts = pd.DataFrame(columns = X_train.columns, index = features_results.keys())
for i in features_results:
	features_impacts.ix[i] = pd.DataFrame(features_results[i]).T.mean().values

features_impacts.mean().to_csv('/Users/mcnamarp/Documents/concessions_presentation/rf_features.csv')

# EXPERIMENT EVALUATION #
sales['exp'] = 0
sales.ix[sales['fmb_category_lvl_3'] == '8th Floor', 'exp'] = 1
eval = sales.groupby(['exp','type','category','location_abv','event_date']).sum()['net_revenue'].reset_index()
eval[eval['category'] == 'water'].groupby('exp').mean() # water sales #

'''
temp_bars = gross[gross['category'].isin(['Bar','Portable Bar','Beer Pub','Garden Bar','Farley Bar','Madison Bar','Bridge Pub'])]
sorted_sids_bars = ['6_B','6_PB','6_FB', '6_MB','6_BP' ,'6_GB','8_BP','8_B','8_GB','8_PB','10_B','10_BRP','11_B2','11_B']
sorted_sids_bars2 = ['10_B','10_BRP','11_B','11_B2','6_B','6_BP','6_FB','6_GB','6_MB','6_PB','8_B','8_BP','8_GB','8_PB']
ax = sns.boxplot(x="sid", y="net_revenue", hue="floor", data=temp_bars, palette="Set1", order = sorted_sids_bars2)
#ax.set_title('Net Revenue for Bars')
#ax1 = plt.axes()
#ax1.xaxis.label.set_visible(False)
#fig = ax.get_figure()
#fig.suptitle('')
ax.set_xticklabels(['%s\n$n$=%d'%(k, len(v)) for k, v in temp_bars.groupby('sid')])
plt.xticks(fontsize=10)
plt.show()

temp_hd = gross[gross['category'] == 'Hot Dog'].drop(['type'], axis = 1).groupby(['sid','date','floor']).sum()['net_revenue'].reset_index()
sorted_sids_hd = ['10_HD','10_HD2','11_HD','11_HD2','6_HD','6_HD2','6_HD3','6_HD4','8_HD','8_HD2','8_HD3','8_HD4','8_HD5','8_HD6','8_HD7','9_HD','9_HD2','9_HD3']   
ax = sns.boxplot(x="sid", y="net_revenue", hue="floor", data=temp_hd, palette="Set1", order = sorted_sids_hd)
#sns.despine(offset=10, trim=True)
#ax.set_title('Net Revenue for Bars')
#ax1 = plt.axes()
#ax1.xaxis.label.set_visible(False)
#fig = ax.get_figure()
#fig.suptitle('')
ax.set_xticklabels(['%s\n$n$=%d'%(k, len(v)) for k, v in temp_hd.groupby('sid')])
plt.xticks(fontsize=10)
plt.show()

temp_cf = gross[gross['category'] == 'Chicken / Fries']
sorted_sids_cf = ['10_CF','6_CF','6_CF2','8_CF']   
ax = sns.boxplot(x="sid", y="net_revenue", hue="floor", data=temp_cf, palette="Set1", order = sorted_sids_cf)
#ax.set_title('Net Revenue for Bars')
#ax1 = plt.axes()
#ax1.xaxis.label.set_visible(False)
#fig = ax.get_figure()
#fig.suptitle('')
ax.set_xticklabels(['%s\n$n$=%d'%(k, len(v)) for k, v in temp_cf.groupby('sid')])
plt.xticks(fontsize=10)
plt.show()

temp_db = gross[gross['category'] == 'Daily Burger']
sorted_sids_db = ['6_DB','6_DB2','8_DB']   
ax = sns.boxplot(x="sid", y="net_revenue", hue="floor", data=temp_db, palette="Set1", order = sorted_sids_db)
#ax.set_title('Net Revenue for Bars')
#ax1 = plt.axes()
#ax1.xaxis.label.set_visible(False)
#fig = ax.get_figure()
#fig.suptitle('')
ax.set_xticklabels(['%s\n$n$=%d'%(k, len(v)) for k, v in temp_db.groupby('sid')])
plt.xticks(fontsize=10)
plt.show()

# TIME ANALYSIS #
sales['time_btwn_txn'] = sales.sort_values(['location_abv','tendered_datetime']).groupby(['event_date'])['tendered_datetime'].diff()
time_analysis = sales[['location_abv','event_date','ads_fmb_sales_hdr_id','time_btwn_txn']].drop_duplicates().drop(['ads_fmb_sales_hdr_id'], axis = 1)
time_analysis['time_btwn_txn'] = time_analysis['time_btwn_txn'].dt.seconds
time_analysis = pd.merge(time_analysis, sales.groupby(['location_abv','event_date']).sum()['net_revenue'].reset_index(), on = ['location_abv','event_date'])
# CATEGORY SALES #
cat_sales = sales.groupby(['location_abv','event_date','type']).sum()[['net_revenue']].reset_index()
cat_sales = pd.merge(cat_sales, sales.groupby(['location_abv','event_date']).sum()['net_revenue'].reset_index(), on = ['location_abv','event_date'])
cat_sales.rename(columns = {'net_revenue_x':'category_NR', 'net_revenue_y':'stand_NR'}, inplace = True)
cat_sales['pct_NR'] = cat_sales['category_NR']/cat_sales['stand_NR']

# 8th Floor Hot Dog Stands Analysis #
hd_8_stands = set(gross[(gross['category'] == 'Hot Dog') & (gross['floor'] == '8th Floor')]['ID'])
hd_8_table = gross[gross['ID'].isin(hd_8_stands)][['ID','date']].drop_duplicates()['date'].value_counts()
hd_8_sales = gross[gross['ID'].isin(hd_8_stands)][['ID','date','net_revenue']].drop_duplicates().groupby(['date']).sum()['net_revenue']
hd_8_table = pd.DataFrame(hd_8_table).join(hd_8_sales)
hd_8_table = hd_8_table.join(gross[['date','day','month']].drop_duplicates().set_index('date'))
temp_analysis_hd8 = pd.merge(sales, hd_8_table['date'].reset_index().rename(columns={'index':'event_date'}), on = 'event_date')
temp_analysis_hd8[['event_date','terminal','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
temp_analysis_hd8[['event_date','ads_fmb_sales_hdr_id','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
temp_analysis_hd8 = temp_analysis[temp_analysis['location_abv'].isin(hd_8_stands)]
temp_analysis_hd8[['event_date','terminal','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
temp_analysis_hd8[['event_date','ads_fmb_sales_hdr_id','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
hd_8_table = hd_8_table.join(time_analysis[time_analysis['location_abv'].isin(hd_8_stands)].groupby('event_date').median()[['time_btwn_txn']])
hd_8_table = hd_8_table.join(cat_sales[cat_sales['location_abv'].isin(hd_8_stands)].groupby(['event_date','type']).mean().reset_index('type'))
hd_8_table.reset_index().groupby(['date','type']).mean()['category_NR'].reset_index()

#analyzing distribution of hd sales #
hd_beer_transactions = set(sales[(sales['location_abv'].isin(hd_8_stands)) & (sales['category'] == 'beer')]['ads_fmb_sales_hdr_id'])
hd_non_beer_transactions = set(sales[(sales['location_abv'].isin(hd_8_stands)) & (sales['category'] != 'beer')]['ads_fmb_sales_hdr_id'])
hd_beer_sales = sales[sales['ads_fmb_sales_hdr_id'].isin(hd_beer_transactions)]
hd_non_beer_sales = sales[(~sales['ads_fmb_sales_hdr_id'].isin(hd_beer_transactions)) & (sales['location_abv'].isin(hd_8_stands))]

# 9th Floor Hot Dog Stands Analysis #
hd_9_stands = set(gross[(gross['category'] == 'Hot Dog') & (gross['floor'] == '9th Floor')]['ID'])
hd_9_table = gross[gross['ID'].isin(hd_9_stands)][['ID','date']].drop_duplicates()['date'].value_counts()
hd_9_sales = gross[gross['ID'].isin(hd_9_stands)][['ID','date','net_revenue']].drop_duplicates().groupby(['date']).sum()['net_revenue']
hd_9_table = pd.DataFrame(hd_9_table).join(hd_9_sales)
hd_9_table = hd_9_table.join(gross[['date','day','month']].drop_duplicates().set_index('date'))
temp_analysis_hd9 = pd.merge(sales, hd_9_table['date'].reset_index().rename(columns={'index':'event_date'}), on = 'event_date')
temp_analysis_hd9 = temp_analysis_hd9[temp_analysis_hd9['location_abv'].isin(hd_9_stands)]
temp_analysis_hd9[['event_date','terminal','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
temp_analysis_hd9[['event_date','ads_fmb_sales_hdr_id','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
hd_9_table = hd_9_table.join(time_analysis[time_analysis['location_abv'].isin(hd_9_stands)].groupby('event_date').median()[['time_btwn_txn']])
hd_9_table = hd_9_table.join(cat_sales[cat_sales['location_abv'].isin(hd_9_stands)].groupby(['event_date','type']).mean().reset_index('type'))
hd_9_table.reset_index().groupby(['date','type']).mean()['category_NR'].reset_index()

hd_10_table = gross[gross['gid'].isin(['101002','101008'])][['ID','date']].drop_duplicates()['date'].value_counts()
hd_10_sales = gross[gross['gid'].isin(['101002','101008'])][['ID','date','net_revenue']].drop_duplicates().groupby(['date']).sum()['net_revenue']
hd_10_table = pd.DataFrame(hd_10_table).join(hd_10_sales)
hd_10_table = hd_10_table.join(gross[['date','day','month']].drop_duplicates().set_index('date'))

bar_11_table = gross[gross['gid'].isin(['111003','111019'])][['ID','date']].drop_duplicates()['date'].value_counts()
bar_11_sales = gross[gross['gid'].isin(['111003','111019'])][['ID','date','net_revenue']].drop_duplicates().groupby(['date']).sum()['net_revenue']
bar_11_table = pd.DataFrame(bar_11_table).join(bar_11_sales)
bar_11_table = bar_11_table.join(gross[['date','day','month']].drop_duplicates().set_index('date'))

cf_stands =  set(gross[(gross['category'] == 'Chicken / Fries') & (gross['floor'] == '6th Floor')]['ID'])
cf_table = gross[gross['ID'].isin(cf_stands)][['ID','date']].drop_duplicates()['date'].value_counts()
cf_sales = hd_9_sales = gross[gross['ID'].isin(cf_stands)][['ID','date','net_revenue']].drop_duplicates().groupby(['date']).sum()['net_revenue']
cf_table = pd.DataFrame(cf_table).join(cf_sales)
#cf_table = cf_table.join(gross[['date','day','month']].drop_duplicates().set_index('date'))
temp_analysis_cf = pd.merge(sales, cf_table['date'].reset_index().rename(columns={'index':'event_date'}), on = 'event_date')
temp_analysis_cf = temp_analysis_cf[temp_analysis_cf['location_abv'].isin(hd_9_stands)]
temp_analysis_cf[['event_date','terminal','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
temp_analysis_cf[['event_date','ads_fmb_sales_hdr_id','date']].drop_duplicates().groupby(['date','event_date']).count().reset_index().groupby('date').mean()
cf_table = cf_table.join(time_analysis[time_analysis['location_abv'].isin(cf_stands)].groupby('event_date').median()[['time_btwn_txn']])
cf_table = cf_table.join(cat_sales[cat_sales['location_abv'].isin(cf_stands)].groupby(['event_date','type']).sum().reset_index('type'))
cf_table.reset_index().groupby(['date','type']).mean()['category_NR'].reset_index()

#analyzing distribution of cf sales #
cf_beer_transactions = set(sales[(sales['location_abv'].isin(cf_stands)) & (sales['category'] == 'beer')]['ads_fmb_sales_hdr_id'])
cf_non_beer_transactions = set(sales[(sales['location_abv'].isin(cf_stands)) & (sales['category'] != 'beer')]['ads_fmb_sales_hdr_id'])
cf_beer_sales = sales[sales['ads_fmb_sales_hdr_id'].isin(cf_beer_transactions)]
cf_non_beer_sales = sales[(~sales['ads_fmb_sales_hdr_id'].isin(cf_beer_transactions)) & (sales['location_abv'].isin(cf_stands))]

all_stands = pd.DataFrame(gross.groupby('date').count()['ID']).join(gross.groupby('date').sum()['net_revenue'])
arena_cat_sales = sales.groupby(['event_date','type']).sum()[['net_revenue']].reset_index(level=1).rename(columns = {'net_revenue':'category_NR'})
all_stands = all_stands.join(arena_cat_sales)

alc_stands = pd.DataFrame(gross[gross['type'] == 'alcohol'].groupby('date').count()['ID']).join(gross.groupby('date').sum()['net_revenue'])

food_sales_by_stand = cat_sales[(cat_sales['type'] == 'food')].groupby('location_abv').mean().sort_values('pct_NR')

food_stands = cat_sales[(cat_sales['location_abv'].isin(food_sales_by_stand[food_sales_by_stand['pct_NR'] > 0.2].index)) & (cat_sales['type'] == 'food')]
food_stands.groupby('event_date').count()[['location_abv']].join(food_stands.groupby('event_date').sum()['category_NR']).groupby('location_abv').mean()

non_food_stands = cat_sales[(cat_sales['location_abv'].isin(food_sales_by_stand[food_sales_by_stand['pct_NR'] < 0.2].index)) & (cat_sales['type'] == 'food')]
non_food_stands.groupby('event_date').count()[['location_abv']].join(non_food_stands.groupby('event_date').sum()['category_NR']).groupby('location_abv').mean()