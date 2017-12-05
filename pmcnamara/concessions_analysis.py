import pandas as pd
import numpy as np
import datetime
import sqlalchemy

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

billy_joel_dates = ['1/9/15','2/18/15','3/9/15','4/3/15','5/28/15','6/20/15','7/1/15','8/20/15','9/26/15','10/21/15','11/19/15','12/17/15','1/7/2016',
					'2/13/2016','3/15/2016','4/15/2016','5/27/2016','6/17/2016','7/20/2016','8/9/2016','10/28/2016','11/21/2016','11/30/2016',
					'12/17/2016','1/11/2017','2/22/2017','3/3/2017','4/14/2017','5/25/17','7/5/17','8/21/17','9/30/2017','10/20/17','11/18/17']

concessions_sales_query = '''
SELECT DISTINCT H.tendered_datetime::DATE AS event_date, ads_fmb_sales_product_detail_id, D.ads_fmb_sales_hdr_id, product_name, H.tendered_datetime::timestamp(0), product_quantity, ROUND(gross_sales_amount, 2) AS gross, D.revenue_cost_amount AS cost, location_abv, fmb_category_lvl_3, fmb_category_lvl_4
FROM ads_main.f_fmb_sales_product_dtls D
JOIN ads_main.d_location L ON L.location_dim_id = D.location_dim_id AND fmb_category_lvl_2 = 'Concessions' AND store_name = 'Madison Square Garden'
JOIN ads_main.f_fmb_sales_hdr_dtls H ON H.ads_fmb_sales_hdr_id = D.ads_fmb_sales_hdr_id AND H.tendered_datetime::DATE IN (''' + str(list(billy_joel_dates)).replace('[','').replace(']','') + ''')
JOIN ads_main.d_product P ON P.product_dim_id = D.product_dim_id;
'''
sales = pd.read_sql(concessions_sales_query, engine)

sales['sale_date'] = sales['tendered_datetime'].dt.date
sales['event_time'] = datetime.time(20,0)
sales['time'] = sales['tendered_datetime'].dt.time
sales['event_month'] = pd.DatetimeIndex(sales['tendered_datetime']).month
sales['event_year'] = pd.DatetimeIndex(sales['tendered_datetime']).year
sales['day_name'] = pd.to_datetime(sales['tendered_datetime']).dt.weekday_name

# fixing shock top cost issue #
shock_top_sales = ['FF SHOCK TOP /BELGIAN WHITE ALE /CONCERT 24OZ /FS','FF SHOCK TOP /BELGIAN WHITE ALE /24OZ /FS']
sales.ix[(sales['product_name'].isin(shock_top_sales)), 'cost'] = 0.84*sales['product_quantity']
sales['line_item_profit'] = sales['gross'] - sales['cost']

#attendance_query = '''
#SELECT tm_event_date::DATE AS event_date, COUNT(*) AS attendance
#FROM ads_main.d_event_plan E
#JOIN (SELECT DISTINCT event_plan_id, section_name, row_name, seat FROM ads_main.vw_attendance_event) A ON E.event_plan_id = A.event_plan_id AND eatec_event_name = 'BILLY JOEL'
#GROUP BY tm_event_date::DATE
#HAVING COUNT(*) > 10000 
#'''
#attendance = pd.read_sql(attendance_query, engine)

event_station_sales = sales.groupby(['location_abv','event_date']).sum()[['gross','cost','line_item_profit']].reset_index()

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