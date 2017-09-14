from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_regression
import pandas as pd
import numpy as np
import datetime
import matplotlib.pylab as plt
import sqlalchemy
#import xgboost as xgb
from sklearn.externals import joblib
def runXGB(train_X, train_y, test_X, feature_names=None, num_rounds=5000):
    param = {}
    param['objective'] = "reg:linear"
    param['booster']="gbtree"
    param['eta'] = 0.04
    param['max_depth'] = 9
    param['colsample_bytree'] = 0.4
    param['silent'] = 1
    param['subsample'] = 0.8
    param['colsample_bytree'] = 0.7
    param['eval_metric']="mae"

    plst = list(param.items())
    xgtrain = xgb.DMatrix(train_X, label=train_y)


    xgtest = xgb.DMatrix(test_X)
    model = xgb.train(plst, xgtrain, num_rounds)

    pred_test_y = model.predict(xgtest)
    return pred_test_y, model

engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")

renewals_query = '''
with TABLE_1
  AS (
SELECT tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
FROM msgbiadb.ads_main.t_ticket_sales_event_seat
where tm_event_name_long = 'RADIO CITY CHRISTMAS SPECTACULAR'  and tm_comp_name='Not Comp' and ticket_group_flag='N' and ticket_type_desc LIKE '%%Individual%%' 
GROUP BY tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
having count(*)=1
),
  Table_2 AS (
  SELECT *
  FROM msgbiadb.ads_main.t_ticket_sales_event_seat
  WHERE tm_event_name_long = 'RADIO CITY CHRISTMAS SPECTACULAR' and ticket_group_flag='N' AND tm_comp_name = 'Not Comp' and ticket_type_desc LIKE '%%Individual%%'  

             ),
out AS
  (
      SELECT Table_2.*
      FROM TABLE_1
        LEFT JOIN Table_2
          ON
             TABLE_1.tm_section_name = Table_2.tm_section_name
             AND TABLE_1.tm_row_name = Table_2.tm_row_name
             AND TABLE_1.tm_seat_num = Table_2.tm_seat_num
             AND TABLE_1.tickets_add_datetime = Table_2.tickets_add_datetime
             AND TABLE_1.tm_event_name =Table_2.tm_event_name
  ),
out1 AS
  (
      SELECT
        tm_event_name,
        tm_event_date,
        tm_event_day,
        tm_event_time,
        ticket_transaction_date,
        full_date,
        day_of_week,
        weekday_indicator,
        year_month,
        day_in_month,
        week_id,
        week_end_date,
        day_in_year,
        calendar_year,
        calendar_quarter,
        month_number_in_year,
        count(*),
        sum(tickets_discount_amount)     AS tickets_discount_amount,
        sum(tickets_surcharge_amount)    AS tickets_surcharge_amount,
        sum(tickets_purchase_price)      AS tickets_purchase_price,
        sum(tickets_pc_ticket)           AS tickets_pc_ticket,
        sum(tickets_pc_tax)              AS tickets_pc_tax,
        sum(tickets_pc_licfee)           AS tickets_pc_licfee,
        sum(tickets_total_gross_revenue) AS tickets_total_gross_revenue,
        sum(tickets_total_revenue)       AS tickets_total_revenue
      FROM out
      GROUP BY tm_event_name, tm_event_date, tm_event_day, tm_event_time, ticket_transaction_date, full_date,
        day_of_week, weekday_indicator, year_month, day_in_month, week_id, week_end_date, day_in_year, calendar_year,
        calendar_quarter, month_number_in_year
  ),
out2 AS
  (
    SELECT
      full_date AS tm_event_date,
      day_of_week AS event_day_of_week,
      weekday_indicator AS event_weekday_indicator,
      year_month AS event_year_month,
      day_in_month AS event_day_in_month,
      week_id AS event_week_id,
      week_end_date AS event_week_end_date,
      day_in_year AS event_day_in_year,
      calendar_year AS event_calendar_year,
      calendar_quarter AS event_calendar_quarter,
      month_number_in_year AS event_month_number_in_year
    FROM msgbiadb.ads_main.d_date
  )
select full_date,count from out1 
order by full_date desc
'''

third_query = '''
WITH Table_1
    as(select tm_event_time,tm_event_date
from msgbiadb.ads_main.t_ticket_sales_event_seat
where tm_season_name IN ('2017 RCCS 1st Half','2017 RCCS 2nd Half')
GROUP BY tm_event_time,tm_event_date)
select  tm_event_date, count(*)
from Table_1
GROUP BY tm_event_date
order by tm_event_date DESC
'''
second_query = '''
WITH Table_1
    as(select tm_event_time,tm_event_date
from msgbiadb.ads_main.t_ticket_sales_event_seat
where tm_season_name IN ('2016 RCCS 1st Half','2016 RCCS 2nd Half')
GROUP BY tm_event_time,tm_event_date)
select  tm_event_date, count(*)
from Table_1
GROUP BY tm_event_date
order by tm_event_date DESC
'''
first_query = '''
WITH Table_1
    as(select tm_event_time,tm_event_date
from msgbiadb.ads_main.t_ticket_sales_event_seat
where tm_season_name IN ('2015 RCCS 1st Half','2015 RCCS 2nd Half')
GROUP BY tm_event_time,tm_event_date)
select  tm_event_date , count(*)
from Table_1
GROUP BY tm_event_date
order by tm_event_date DESC
'''
camp_query = '''
with
all_emails as
(select
sj.exctgt_send_id as send_id,
sj.exctgt_sched_time as deploy_date,
sj.exctgt_email_name as email_name ,
sj.exctgt_business_unit_name as business_unit,
sj.exctgt_subject as subject_line,
sum(kpi.exctgt_email_sent_count) as SentCnt,
sum(kpi.exctgt_email_delivered_count) as DeliveredCnt,
sum(kpi.click_email_distinct_count) as DistinctClickCnt,
sum(kpi.exctgt_email_unique_open_count) as DistinctOpenCnt,
sum(kpi.exctgt_email_unsub_count) as UnsubCnt,
case
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Weekly%%' ) then 'Weekly'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%%ATT %%' ) then 'Pregame - Attending'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%%NA %%' ) then 'Pregame - Not Attending'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%%Away %%' ) then 'Pregame - Away'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) then 'Pregame'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Youth Marketing%%' ) then 'Youth Marketing'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Combo%%' ) then 'Combo'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Value%%' ) then 'Value'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% RENW%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% Renew%%' ) then 'Renewal'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Psh%%' ) then 'Preshow'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Pre%%' ) then 'Presale'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% DISC%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% OFF%%' ) then 'Discount'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% EOS%%' ) then 'Early On Sale'
when upper(exctgt_email_name) SIMILAR TO upper('%% RMDR%%' ) then 'Reminder'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% STH%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% STM%%' ) then 'STM'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% ONS%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%%ON Sa%%' )then 'Onsale'
else 'Miscellaneous'
end as EmailCategory
from ads_main.f_exctgt_job_kpis kpi
left join  ads_main.d_exctgt_sendjobs sj
            on sj.exctgt_sendjob_id=kpi.exctgt_sendjob_id
where
1=1
and sj.exctgt_business_unit_name like '%%Radio City%%'
and kpi.exctgt_email_address not like '%%@msg.com%%'
and kpi.exctgt_email_address not like '%%@thegarden.com%%'
and exctgt_sched_time  between cast('05/07/2015' as timestamp) and cast('09/11/2017' as timestamp)
and Exctgt_job_status = 'Complete'
group by sj.exctgt_send_id, sj.exctgt_sched_time, sj.exctgt_email_name , sj.exctgt_business_unit_name, sj.exctgt_subject
order by sj.exctgt_send_id, sj.exctgt_sched_time
)
select
  CAST(deploy_date as DATE) AS full_date,
  sum(SentCnt) as sent
from all_emails
where all_emails.sentcnt >= 10
GROUP BY CAST(deploy_date as DATE)
order by full_date ASC
'''




#camp= pd.read_sql(camp_query, engine)
data = pd.read_sql(renewals_query, engine)

# CLEAN SALES DATA #
data=data.groupby('full_date').sum().reset_index()
data=data.dropna()
data=data.set_index('full_date')
idx = pd.date_range('05-07-2015', '01-01-2018')
data.index = pd.DatetimeIndex(data.index)
data =data.reindex(idx, fill_value=0)
data['date']=data.index
data=data.reset_index(drop=True)
data['date']=pd.to_datetime(data['date'])

data = data[data.date!= '11/20/2015']
data = data[data.date!= '7/13/2017']
data = data[data.date!= '7/12/2017']
data = data[data.date!= '9/22/2016']
data = data[data.date!= '10/18/2016']

#promo,presale
data['date']=pd.to_datetime(data['date']).dt.date

promo_dates = pd.to_datetime(['2016-06-20', '2016-08-02', '2016-08-10', '2016-08-17', '2016-09-01', '2016-09-25', '2016-09-26', '2016-09-27', '2016-09-28', '2016-09-29', '2016-09-30', '2016-10-01', '2016-10-02', '2016-10-03', '2016-10-04', '2016-10-05', '2016-10-06', '2016-10-07', '2016-10-08']).date
data['promo']=[1 if data['date'][i] in promo_dates else 0 for i in data.index]
sale_dates = pd.to_datetime(['2016-06-20', '2016-08-02', '2016-08-10', '2016-08-17']).date
data['sale']=[1 if data['date'][i] in sale_dates else 0 for i in data.index]
'''
promoy_dates = pd.to_datetime(['2016-06-21', '2016-08-03', '2016-08-11', '2016-08-18', '2016-09-02', '2016-09-26', '2016-09-27', '2016-09-28', '2016-09-29', '2016-09-30', '2016-10-01', '2016-10-02', '2016-10-03', '2016-10-04', '2016-10-05', '2016-10-06', '2016-10-07', '2016-10-08', '2016-10-09']).date
data['promoy']=[1 if data['date'][i] in promoy_dates else 0 for i in data.index]
saley_dates = pd.to_datetime(['2016-06-21', '2016-08-03', '2016-08-11', '2016-08-18']).date
data['saley']=[1 if data['date'][i] in saley_dates else 0 for i in data.index]

onsale_dates = pd.to_datetime(['2015-05-19', '2016-08-23', '2017-08-17']).date
data['onsale']=[1 if data['date'][i] in onsale_dates else 0 for i in data.index]

onsaley_dates = pd.to_datetime([ '2015-05-20','2016-08-24', '2017-08-18']).date
data['onsaley']=[1 if data['date'][i] in onsaley_dates else 0 for i in data.index]
'''
#social,traffic,weather 
traffic=pd.read_csv('/Users/mcnamarp/Documents/consumer_insights/thao/rockettes_visitors.csv').drop(['Row Number'], axis = 1)
social=pd.read_csv('/Users/mcnamarp/Documents/consumer_insights/thao/rockettes_social.csv')
weather=pd.read_csv('/Users/mcnamarp/Documents/consumer_insights/thao/rockettes_weather.csv')
social=social.rename(columns={'Twitter Organic Impressions':'Twitter','Facebook Page Impressions':'Facebook'})
#camp['full_date']=pd.to_datetime(camp['full_date']).dt.date
traffic['Date'] = pd.to_datetime(traffic['Date']).dt.date
social['Date'] = pd.to_datetime(social['Date']).dt.date
weather['DATE'] = pd.to_datetime(weather['DATE']).dt.date
social['Facebook']=social['Facebook'].str.replace(',','').fillna(0).astype(int)
social['Twitter']=social['Twitter'].str.replace(',','').fillna(0).astype(int)
web = pd.merge(social, traffic, on = 'Date')

data = pd.merge(data, web, how ='left', left_on = ['date'], right_on = ['Date'])
data = pd.merge(data, weather, how ='left', left_on = ['date'], right_on = ['DATE'])
'''
data = pd.merge(data, camp, how ='left', left_on = ['date'], right_on = ['full_date'])
data['senty']=data['sent']
data.senty=data.senty.shift(1)
data['sent']=data['sent'].fillna(0)
data['senty']=data['senty'].fillna(0)
'''
#christmas and ticket left
first = pd.read_sql(first_query, engine)
second= pd.read_sql(second_query, engine)
third= pd.read_sql(third_query, engine)

first['tm_event_date']=pd.to_datetime(first['tm_event_date']).dt.date
second['tm_event_date']=pd.to_datetime(second['tm_event_date']).dt.date
third['tm_event_date']=pd.to_datetime(third['tm_event_date']).dt.date

first['left']=np.cumsum(first['count'])
second['left']=np.cumsum(second['count'])
third['left']=np.cumsum(third['count'])
first=first.drop(['count'],axis=1)
second=second.drop(['count'],axis=1)
third=third.drop(['count'],axis=1)

data['christmas']=data['date'].apply(lambda x: (x-datetime.date(2015,12,25)).days)
data1=data[data.christmas<10]
data1 = pd.merge(data1, first, how ='left', left_on = ['date'], right_on = ['tm_event_date'])
data['christmas']=data['date'].apply(lambda x: (x-datetime.date(2016,12,25)).days)
data2=data[data.christmas>-357]
data2=data2[data.christmas<9]
data2 = pd.merge(data2, second, how ='left', left_on = ['date'], right_on = ['tm_event_date'])
data['christmas']=data['date'].apply(lambda x: (x-datetime.date(2017,12,25)).days)
data3=data[data.christmas>-357]
data3 = pd.merge(data3, third, how ='left', left_on = ['date'], right_on = ['tm_event_date'])
data=pd.concat([data1,data2,data3]).reset_index(drop=True)
data = data.fillna(method='bfill')

#weekday
data['weekday']=data['date'].apply(lambda x:x.weekday())
#monthday
data['monthday']=data['date'].apply(lambda x:x.day)
#week
data['week']=data['date'].apply(lambda x:x.isocalendar()[1])
#yearday
data['yearday']=data['date'].apply(lambda x:x.timetuple().tm_yday)
#year
data['year']=data['date'].apply(lambda x:x.year)
#month

data['month']=data['date'].apply(lambda x:x.month)
#drop unuseful

data=data.drop(['Brand','Date','DATE','tm_event_date','date'],axis=1)
#data=data.drop(['Brand','Date','DATE','tm_event_date','date','full_date'],axis=1)
#fit model

train_x=data[:849].drop(['count'],axis=1)
train_y=data[:849]['count']
regr = RandomForestRegressor(n_estimators= 1000, n_jobs = -1,min_samples_leaf =50)
a=regr.fit(train_x, train_y)
'''
test_x=data[800:849].drop(['count'],axis=1)
b=a.predict(test_x)
#preds, model = runXGB(train_x, train_y, test_x, num_rounds=1500)

data5=data[800:849].reset_index(drop=True)
test_y=data5['count']
result=pd.DataFrame()
result['predict']=b
result['real']=test_y

result['error']=result['predict']-result['real']
result['aerror']=result['error'].apply(lambda x:abs(x))

result['ape']=result['error']/result['real']
result['aape']=result['aerror']/result['real']
print(result['ape'].mean())
print(result['aape'].mean())
print((sum(b)-sum(test_y))/sum(test_y))
'''
joblib.dump(a,'e.pkl')
#joblib.dump(e.pkl, '/Users/mcnamarp/Documents/consumer_insights/thao/')