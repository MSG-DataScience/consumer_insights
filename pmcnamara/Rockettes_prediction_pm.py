#import xgboost as xgb
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

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

tickets_query = '''
select
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as cust_sum,
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as sale_date,
tm_acct_id::TEXT,
acct_status,
acct_type_desc AS acct_type,
state,
SUBSTRING(zip, 1, 5) AS zip,
country,
tm_event_name AS event_id,
tm_event_date AS event_date,
tm_event_time AS event_time,
tm_event_day AS event_day,
mpd_indy_rank AS rank,
SUBSTRING(tm_season_name, 1, 4) AS season,
tm_section_name,
tm_row_name,
tm_seat_num,
tm_comp_name,
tm_code,
tickets_sold,
tickets_pc_ticket,
tickets_total_gross_revenue,
tickets_total_revenue,
tickets_discount_amount,
tickets_add_datetime,
ticket_host_flag,
ticket_sell_location_name,
ticket_group_flag
from ads_main.t_ticket_sales_event_seat
where tm_season_name IN ('2015 RCCS 1st Half','2015 RCCS 2nd Half','2016 RCCS 1st Half','2016 RCCS 2nd Half')
AND tm_acct_id NOT IN ('-1','-2');
'''
data = pd.read_sql(tickets_query, engine)

# CLEAN SALES DATA #
data['event_date'] = data['event_date'].dt.date
data['sale_date'] = data['tickets_add_datetime'].dt.date
data['sale_time'] = data['tickets_add_datetime'].dt.time
data['event_time'] = pd.to_datetime(data['event_time'], format='%H:%M:%S').dt.time
data = data[(data['sale_date'] == data['tickets_add_datetime']) & (data['tickets_sold'] > 0) & (data['cust_sum'] > 0)]
data = data[data['sale_date'] <= data['event_date'].max()]
data.drop(['nbr_bought','cust_sum','tickets_total_gross_revenue','tickets_total_revenue'], axis = 1, inplace = True)

# IMPORT SECONDARY TICKET SALES #
secondary = pd.read_csv('/Users/mcnamarp/Downloads/secondary_sales.csv')
secondary = secondary[secondary['Status'] == 'Sold']
secondary['event_time'] = secondary['Event Date'].str[-8:].str.strip()
secondary['event_time'] = pd.to_datetime(secondary['event_time']).dt.time
secondary['event_date'] = pd.to_datetime(secondary['Event Date'].str[4:-8].str.strip()).dt.date

drops = ['Status','Status.1','Home','Home.1','PriceCode','PriceCode.1','Section.1','Row.1','Beg Seat.1','End Seat.1','Qty.1','MSG Total Revenue.1','Customer.1','DaysOut.1','MSG Rev Per Ticket.1','Invoice Date.1','Invoice Time.1','Event DateTime1','Event DateTime2','Event Date']
secondary.drop(drops, axis = 1, inplace = True)

# create ticket sales to date #
sales_15 = data[data['season'] == '2015'].groupby(['event_id','sale_date','season']).count()['tickets_sold'].unstack(level=1).fillna(0).stack()
sales_16 = data[data['season'] == '2016'].groupby(['event_id','sale_date','season']).count()['tickets_sold'].unstack(level=1).fillna(0).stack()
prior_sales = sales_15.append(sales_16)
prior_sales.name = 'tickets_sold'
dollars_15 = data[data['season'] == '2015'].groupby(['event_id','sale_date','season']).sum()['tickets_pc_ticket'].unstack(level=1).fillna(0).stack()
dollars_16 = data[data['season'] == '2016'].groupby(['event_id','sale_date','season']).sum()['tickets_pc_ticket'].unstack(level=1).fillna(0).stack()
prior_dollars = dollars_15.append(dollars_16)
prior_dollars.name = 'tickets_cost'
prior_sales = pd.DataFrame(prior_sales).join(prior_dollars).groupby(level=0).cumsum()
prior_sales['avg_cost'] = prior_sales['tickets_cost']/prior_sales['tickets_sold']

# REMOVING DATES AFTER EVENT #
prior_sales = pd.merge(prior_sales.reset_index(), data[['event_id','event_date']].drop_duplicates(), on = 'event_id')
prior_sales = prior_sales[prior_sales['sale_date'] <= prior_sales['event_date']]

# CREATE PROMO AND PRESALE VARIABLES #
promo_dates = pd.to_datetime(['2016-06-20', '2016-08-02', '2016-08-10', '2016-08-17', '2016-09-01', '2016-09-25', '2016-09-26', '2016-09-27', '2016-09-28', '2016-09-29', '2016-09-30', '2016-10-01', '2016-10-02', '2016-10-03', '2016-10-04', '2016-10-05', '2016-10-06', '2016-10-07', '2016-10-08']).date
data['promo']=[1 if data['sale_date'][i] in promo_dates else 0 for i in data.index]
sale_dates = pd.to_datetime(['2016-06-20', '2016-08-02', '2016-08-10', '2016-08-17']).date
data['sale']=[1 if data['sale_date'][i] in sale_dates else 0 for i in data.index]

# IMPORT SOCIAL AND WEB DATA #
traffic=pd.read_csv('/Users/mcnamarp/Downloads/rockettes_visitors.csv').drop(['Row Number'], axis = 1)
social=pd.read_csv('/Users/mcnamarp/Downloads/rockettes_social.csv')
social=social.rename(columns={'Twitter Organic Impressions':'Twitter','Facebook Page Impressions':'Facebook'})
traffic['Date'] = pd.to_datetime(traffic['Date']).dt.date
social['Date'] = pd.to_datetime(social['Date']).dt.date
social['Facebook']=social['Facebook'].str.replace(',','').fillna(0).astype(int)
social['Twitter']=social['Twitter'].str.replace(',','').fillna(0).astype(int)

# COMBINE SOCIAL AND WEB DATA WITH SALES DATA #
web = pd.merge(social, traffic, on = 'Date')
data = pd.merge(data, web, how ='left', left_on = ['sale_date'], right_on = ['Date'])

#data = data[data.full_date!= '11/20/2015']
#data = data[data.full_date!= '7/13/2017']
#data = data[data.full_date!= '7/12/2017']
#data = data[data.full_date!= '9/22/2016']
#data = data[data.full_date!= '10/18/2016']
#data.weekday_indicator.replace(('Y','N'),(1,0),inplace=True)
#data['calendar_quarter'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')
#data['event_calendar_quarter'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')
#my_col = ['tickets_discount_amount','tickets_surcharge_amount','tickets_pc_tax','tickets_pc_licfee','tickets_total_revenue','tickets_pc_ticket']
#data=data.drop(my_col,axis=1)
#data.tm_event_day.replace(('MON','TUE','WED','THU','FRI','SAT','SUN'),(0,1,2,3,4,5,6),inplace=True)
#group.tm_event_day.replace(('MON','TUE','WED','THU','FRI','SAT','SUN'),(0,1,2,3,4,5,6),inplace=True)
#data['tm_event_time']=data['tm_event_time'].apply(lambda x:str(x)[0:2])
#data['tm_event_date'] = pd.to_datetime(data['tm_event_date'])
#group['tm_event_date'] = pd.to_datetime(group['tm_event_date'])
data['weekend'] = [1 if (data['event_day'][i] == 'SAT' or data['event_day'][i] == 'SUN') else 0 for i in data.index]
data['daysleft'] = (data['event_date'] - data['sale_date']).dt.days

#lala=group.groupby(by=['tm_event_name','daysleft']).sum().sort_index(ascending=False).groupby(level=[0])['count','tickets_purchase_price'].cumsum().reset_index()
#lala=lala.rename(columns = {'count':'cumsum','tickets_purchase_price':'cumprice'})
#lala['avg']=lala['cumprice']/lala['cumsum']
#data=pd.merge(data,lala,how='left',on=['tm_event_name','daysleft'])
 
#data['calendar_quarter']=data['calendar_quarter'].apply(lambda x:int(x))
data['christmas'] = datetime.date(2015, 12, 25)
data.ix[data['season'] == '2016', 'christmas'] = datetime.date(2016, 12, 25)
data['days_xmas'] = (data['christmas']-data['event_date']).dt.days

le = preprocessing.LabelEncoder()
le.fit(data['tm_event_name'])  
data['tm_event_name']=le.transform(data['tm_event_name'])  
data=data.drop(['tm_event_date','ticket_transaction_date','full_date','year_month','week_end_date'],axis=1)
#data['calendar_year']=data['calendar_year'].apply(lambda x:int(x))
#data['event_calendar_year']=data['event_calendar_year'].apply(lambda x:int(x))
hour_frame = pd.DataFrame(index = data['event_time'].value_counts().index)
hour_frame['hour_value'] = np.nan
for i in hour_frame.index:
	hour_frame['hour_value'][i] = i.hour + (np.float(i.minute)/60)

data = pd.merge(data, hour_frame.reset_index(), left_on = ['event_time'], right_on = ['index']).drop(['index'], axis = 1)

#data['priorweek']=data['count']
#data['weeksent']=data['count']
 
data=data.fillna(0)
'''
for i in np.arange(0,len(data),1):
   dataa=data.loc[data['calendar_year']==data.iloc[i][12]]
   datab=data.loc[data['daysleft']==data.iloc[i][1]+7]
   datac=data.loc[data['tm_event_name']==data.iloc[i][0]]
   datad=pd.merge(dataa,datab,how="inner")
   datae=pd.merge(datad,datac,how="inner")
   if len(datae)==1:
       data['weeksent'][i]=datae['sent'][0]
   else:
       data['weeksent'][i]=0
'''
data4=data.iloc[::2, :].reset_index(drop=True)
data5=data.iloc[1::2, :].reset_index(drop=True)
train_X = data4[['traffic','Twitter','Facebook','presale','promo','avg','cumsum','tm_event_time','tm_event_name', 'tm_event_day','day_of_week', 'event_day_of_week','day_in_month','event_day_in_month','week_id', 'event_week_id','day_in_year','event_day_in_year','calendar_year','event_calendar_year','month_number_in_year','event_month_number_in_year','daysleft','christmas']]

train_y = data4.number

test_X = data5[['traffic','Twitter','Facebook','presale','promo','avg','cumsum','tm_event_time','tm_event_name', 'tm_event_day','day_of_week', 'event_day_of_week','day_in_month','event_day_in_month','week_id', 'event_week_id','day_in_year','event_day_in_year','calendar_year','event_calendar_year','month_number_in_year','event_month_number_in_year','daysleft','christmas']]
test_y = data5.number

preds, model = runXGB(train_X, train_y, test_X, num_rounds=1500)

print(r2_score(test_y,preds))


result=pd.DataFrame()
result['predict']=preds
result['real']=test_y

result['error']=result['predict']-result['real']
result['aerror']=result['error'].apply(lambda x:abs(x))

result['ape']=result['error']/result['real']
result['aape']=result['aerror']/result['real']
print(result['aape'].mean())
