# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 11:18:51 2017

@author: haot
"""
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

engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")
query1 = '''
with table1
as
(
select 
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as cust_sum,
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as max_date,
full_date,
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
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2015 RCCS 1st Half','2015 RCCS 2nd Half','2016 RCCS 1st Half','2016 RCCS 2nd Half','2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
AND house_seat_flag='N'
),
table2 as
(select 
tm_acct_id,full_date,count(*)
from table1
where tickets_sold>0
and cust_sum >0
and max_date=tickets_add_datetime
group by tm_acct_id,full_date
order by full_date)
select 
full_date,count(*) as purchaser
from table2
group by full_date
order by full_date
'''
query2='''
with table1 AS (select
sj.exctgt_send_id as send_id,
sj.exctgt_sendjob_id ,
sj.exctgt_sched_time as exctgt_email_send_time,
dt.full_date,
sj.exctgt_email_name as email_name ,
sj.exctgt_business_unit_name ,
sj.exctgt_subject ,
--kpi.exctgt_campaign_code,
--kpi.exctgt_cell_code,
sj.exctgt_from_name,
sum(kpi.exctgt_email_sent_count) as emails_sent,
sum(kpi.exctgt_email_delivered_count) as emails_delivered,
sum(kpi.exctgt_email_bounced_count) as emails_bounced,
sum(kpi.click_email_distinct_count) as Clicks,
sum(kpi.exctgt_email_unique_open_count) as Opens,
sum(kpi.exctgt_email_unsub_count) as opt_outs,
Case
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Ranger%%' ) then 'Rangers'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Knick%%' ) then 'Knicks'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Westchester%%' ) then 'Westchester Knicks'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Liberty%%' ) then 'Liberty'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('MSG Entertainment - NY%%' ) then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Chicago%%' ) then 'Live - Chicago'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Forum%%' ) then 'Live - Los Angeles'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('MSGE Family%%' ) then 'Family Shows - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Marquee%%' ) then 'Marquee'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Spectacular%%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Rockettes%%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('MSG Sports%%' ) then 'MSG Sports'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Chase Lounge MSG%%') and upper(sj.exctgt_email_name) SIMILAR TO upper('%% Rangers %%' ) then 'Rangers'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Chase Lounge MSG%%') and upper(sj.exctgt_email_name) SIMILAR TO upper('%% Knicks %%' ) then 'Knicks'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Chase Lounge MSG%%') then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') and upper(sj.exctgt_email_name) SIMILAR TO upper('%% RCMH %%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') and upper(sj.exctgt_email_name) SIMILAR TO upper('%% RCCS %%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') and upper(sj.exctgt_email_name) SIMILAR TO upper('%% NYS %%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') then 'Family Shows - New York'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%%Rangers%%' ) then 'Rangers'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%%Knick%%' ) then 'Knicks'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%%Liberty%%' ) then 'Knicks'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Knicks %%' ) then 'Knicks'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% NYR %%' ) then 'Rangers'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% NYK %%' ) then 'Knicks'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% NYL %%' ) then 'Liberty'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% MSGS %%' ) then 'MSG Sports'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% MSGE %%' ) then 'Live - New York'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% RCCS %%' ) then 'Rockettes'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% NCAA %%' ) then 'MSG Sports'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%%Chicago%%' ) then 'Live - Chicago'
when upper(sj.exctgt_email_name) SIMILAR  to upper('%%Chase Chicago%%' ) then 'Live - Chicago'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% MSG %%' ) then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Festival%%') then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Prudential%%') then 'Live - New York'
else 'Unmapped'
end as Brand,
case
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Weekly%%' ) then 'Weekly Newsletter'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%%ATT %%' ) then 'Pregame - Purchased'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%%NA %%' ) then 'Pregame - Not Purchased'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%%Away %%' ) then 'Pregame - Away'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Pregame%%')  then 'Pregame'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Combo%%' ) then 'Combo'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Value%%' ) then 'Value'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% RENW%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% Renew%%' ) then 'Renewal'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Psh%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% preshow%%' )then 'Preshow'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Pre%%' ) then 'Presale'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% DISC%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% OFF%%' ) then 'Discount'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% EOS%%' ) then 'Early On Sale'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% RMDR%%' ) then 'Reminder'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%%PARTNR%%' )or  upper(sj.exctgt_email_name) SIMILAR TO upper('%%PRTNR%%' )then 'Partner Offer'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%%SURVEY%%' ) then 'Survey'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Insights%%') then 'Survey'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%%Youth%%') then 'Juniors'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% YH %%' ) then 'Juniors'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% Chase %%' ) then 'Chase Lounge'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% STH%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% STM%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% MINI%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%% HALF%%' )then 'STM'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%% ONS%%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%%ON Sa%%' )then 'Onsale'
else 'Miscellaneous'
end as EmailCategory
from ads_main.f_exctgt_job_kpis kpi
left join  ads_main.d_exctgt_sendjobs sj
            on sj.exctgt_sendjob_id=kpi.exctgt_sendjob_id
right join ads_main.d_date dt
on kpi.exctgt_email_send_day_id=dt.day_id
where 1=1
and  sj.exctgt_job_status = 'Complete'
--and sj.exctgt_business_unit_name like 'Ranger%%'
and kpi.exctgt_email_address not like '%%msg.com'
and kpi.exctgt_email_address not like '%%thegarden.com'
and kpi.exctgt_email_address not like '%%emailonacid.com'
--and upper(kpi.exctgt_cell_code) not like upper('SEED%%')
--and upper(kpi.exctgt_campaign_code) not like upper('%%BRIGIDTESTLIST%%')
--and upper(kpi.exctgt_campaign_code) not like upper('%%TEST_%%')
and sj.exctgt_sched_time between cast('5/7/2015' as timestamp) and cast('11/16/2017' as timestamp)
--and sj.exctgt_business_unit_name='LA Forum'
and sj.exctgt_business_unit_name='Radio City Christmas Spectacular'
group by sj.exctgt_send_id,
sj.exctgt_sendjob_id ,
sj.exctgt_sched_time,
sj.exctgt_email_name ,
sj.exctgt_business_unit_name,
sj.exctgt_subject,
--kpi.exctgt_campaign_code,
--kpi.exctgt_cell_code,
dt.full_date,
sj.exctgt_from_name
order by sj.exctgt_send_id, sj.exctgt_sched_time)
SELECT full_date,count(*) as email
from table1
GROUP BY full_date
ORDER BY full_date
'''
data1= pd.read_sql(query1, engine)
data2= pd.read_sql(query2, engine)
data=pd.merge(data1,data2,how='left',on='full_date')
data['ratio']=data['purchaser']/data['email']
data.to_csv('Rockettes_purchaser_per_email.csv')