import xgboost as xgb
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
and exctgt_sched_time  between cast('04/01/2016' as timestamp) and cast('02/01/2017' as timestamp)
and Exctgt_job_status = 'Complete'
group by sj.exctgt_send_id, sj.exctgt_sched_time, sj.exctgt_email_name , sj.exctgt_business_unit_name, sj.exctgt_subject
order by sj.exctgt_send_id, sj.exctgt_sched_time
)
select
  CAST(deploy_date as DATE) AS full_date,
  sum(SentCnt) as sent,
  sum(DeliveredCnt) as delivered,
  sum(DistinctClickCnt)as click,
  sum(UnsubCnt) as unsub,
  sum(DistinctOpenCnt) as op
from all_emails
where all_emails.sentcnt >= 10
GROUP BY CAST(deploy_date as DATE)
'''

renewals_query = '''
with TABLE_1
  AS (
SELECT tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
FROM msgbiadb.ads_main.t_ticket_sales_event_seat
where tm_event_name_long = 'RADIO CITY CHRISTMAS SPECTACULAR'  and tm_comp_name='Not Comp' and ticket_group_flag='N' 
GROUP BY tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
having count(*)=1
),
  Table_2 AS (
  SELECT *
  FROM msgbiadb.ads_main.t_ticket_sales_event_seat
  WHERE tm_event_name_long = 'RADIO CITY CHRISTMAS SPECTACULAR'and ticket_group_flag='N'
  AND tm_comp_name = 'Not Comp' 

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
select * from out1 LEFT JOIN out2 USING (tm_event_date)
order by full_date desc
'''

group_query = '''
with TABLE_1
  AS (
SELECT tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
FROM msgbiadb.ads_main.t_ticket_sales_event_seat
where tm_event_name_long = 'RADIO CITY CHRISTMAS SPECTACULAR'  and tm_comp_name='Not Comp' 
GROUP BY tm_acct_id,tm_section_name,tm_row_name,tm_seat_num,tm_event_name,tickets_add_datetime
having count(*)=1
),
  Table_2 AS (
  SELECT *
  FROM msgbiadb.ads_main.t_ticket_sales_event_seat
  WHERE tm_event_name_long = 'RADIO CITY CHRISTMAS SPECTACULAR'
  AND tm_comp_name = 'Not Comp' 

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
select * from out1 LEFT JOIN out2 USING (tm_event_date)
order by full_date desc
'''
group= pd.read_sql(group_query, engine)
data= pd.read_sql(renewals_query, engine)
camp= pd.read_sql(camp_query, engine)
data=data[pd.notnull(data['full_date'])]
group=group[pd.notnull(group['full_date'])]
data['full_date'] = data['full_date'].astype(str)
data["promo"]=data["full_date"].map(lambda x: 1 if x =='2016-06-20' else 1 if x =='2016-08-02' else 1 if x =='2016-08-10' else 1 if x =='2016-08-17' else 1 if x =='2016-09-01' else 1 if x =='2016-09-25' else 1 if x =='2016-09-26' else 1 if x =='2016-09-27' else 1 if x =='2016-09-28' else 1 if x =='2016-09-29' else 1 if x =='2016-09-30' else 1 if x =='2016-10-01' else 1 if x =='2016-10-02' else 1 if x =='2016-10-03' else 1 if x =='2016-10-04' else 1 if x =='2016-10-05' else 1 if x =='2016-10-06' else 1 if x =='2016-10-07' else 1 if x =='2016-10-08' else 0 )
data["presale"]=data["full_date"].map(lambda x: 1 if x =='2016-06-20' else 1 if x =='2016-08-02' else 1 if x =='2016-08-10' else 1 if x =='2016-08-17' else 0 )

data['full_date'] = pd.to_datetime(data['full_date'])
group['full_date'] = pd.to_datetime(group['full_date'])
camp['full_date'] = pd.to_datetime(camp['full_date'])
data= pd.merge(data,camp,how='left',on=['full_date'])
data = data[data.full_date!= '11/20/2015']
data = data[data.full_date!= '7/13/2017']
data = data[data.full_date!= '7/12/2017']
data = data[data.full_date!= '9/22/2016']
data = data[data.full_date!= '10/18/2016']
data.weekday_indicator.replace(('Y','N'),(1,0),inplace=True)
data.event_weekday_indicator.replace(('Y','N'),(1,0),inplace=True)
data['calendar_quarter'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')
data['event_calendar_quarter'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')
my_col = ['tickets_discount_amount','tickets_surcharge_amount','tickets_pc_tax','tickets_pc_licfee','tickets_total_revenue','tickets_pc_ticket']
data=data.drop(my_col,axis=1)
data.tm_event_day.replace(('MON','TUE','WED','THU','FRI','SAT','SUN'),(0,1,2,3,4,5,6),inplace=True)
group.tm_event_day.replace(('MON','TUE','WED','THU','FRI','SAT','SUN'),(0,1,2,3,4,5,6),inplace=True)
data['tm_event_time']=data['tm_event_time'].apply(lambda x:str(x)[0:2])
data['tm_event_date'] = pd.to_datetime(data['tm_event_date'])
group['tm_event_date'] = pd.to_datetime(group['tm_event_date'])

data['daysleft']=data['tm_event_date']-data['full_date']
data['daysleft']=data['daysleft'].apply(lambda x:str(x).split(None, 1))
data = data[np.isfinite(data['tm_event_day'])]
data['daysleft']=data['daysleft'].apply(lambda x:x[0])
data['daysleft']=data['daysleft'].apply(lambda x:int(x))
data['number']=data['count']
data=data[data.daysleft>=0]

group['daysleft']=group['tm_event_date']-group['full_date']
group['daysleft']=group['daysleft'].apply(lambda x:str(x).split(None, 1))
group = group[np.isfinite(group['tm_event_day'])]
group['daysleft']=group['daysleft'].apply(lambda x:x[0])
group['daysleft']=group['daysleft'].apply(lambda x:int(x))
group['number']=group['count']
group=group[group.daysleft>=0]

lala=group.groupby(by=['tm_event_name','daysleft']).sum().sort_index(ascending=False).groupby(level=[0])['count','tickets_purchase_price'].cumsum().reset_index()
lala=lala.rename(columns = {'count':'cumsum','tickets_purchase_price':'cumprice'})
lala['avg']=lala['cumprice']/lala['cumsum']
data=pd.merge(data,lala,how='left',on=['tm_event_name','daysleft'])
 
data['calendar_quarter']=data['calendar_quarter'].apply(lambda x:int(x))
data['christmas']=data['tm_event_date'].apply(lambda x: int(str(x-datetime.datetime(2015,12,25)).split(None, 1)[0]))
data1=data[data.christmas<322]
data['christmas']=data['tm_event_date'].apply(lambda x: int(str(x-datetime.datetime(2016,12,25)).split(None, 1)[0]))
data2=data[data.christmas>-357]
data2=data2[data.christmas<320]
data['christmas']=data['tm_event_date'].apply(lambda x: int(str(x-datetime.datetime(2017,12,25)).split(None, 1)[0]))
data3=data[data.christmas>-357]
data=pd.concat([data1,data2,data3]).reset_index(drop=True)

le = preprocessing.LabelEncoder()
le.fit(data['tm_event_name'])  
data['tm_event_name']=le.transform(data['tm_event_name'])  
data=data.drop(['tm_event_date','ticket_transaction_date','full_date','year_month','week_end_date'],axis=1)
data['calendar_year']=data['calendar_year'].apply(lambda x:int(x))
data['event_calendar_year']=data['event_calendar_year'].apply(lambda x:int(x))
data['tm_event_time']=data['tm_event_time'].apply(lambda x:int(x))

data['priorweek']=data['count']
data['weeksent']=data['count']
data['weekdelivered']=data['count']
data['weekclick']=data['count']
data['weekunsub']=data['count']
data['weekop']=data['count']

    
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
train_X = data4[['presale','promo','avg','cumsum','tm_event_time','tm_event_name', 'tm_event_day','day_of_week', 'event_day_of_week','day_in_month','event_day_in_month','week_id', 'event_week_id','day_in_year','event_day_in_year','calendar_year','event_calendar_year','month_number_in_year','event_month_number_in_year','daysleft','christmas']]

train_y = data4.number

test_X = data5[['presale','promo','avg','cumsum','tm_event_time','tm_event_name', 'tm_event_day','day_of_week', 'event_day_of_week','day_in_month','event_day_in_month','week_id', 'event_week_id','day_in_year','event_day_in_year','calendar_year','event_calendar_year','month_number_in_year','event_month_number_in_year','daysleft','christmas']]
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
