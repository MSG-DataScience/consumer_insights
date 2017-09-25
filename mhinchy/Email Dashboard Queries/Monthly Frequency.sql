with 
job as
(
select dt.month_name, 
dt.Calendar_Year,
kpi.exctgt_ads_subscriber_id, 
kpi.exctgt_email_sent_count, 
kpi.exctgt_email_unique_open_count,
Case 
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Ranger%' ) then 'Rangers'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Knick%' ) then 'Knicks'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Westchester%' ) then 'Westchester Knicks'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Liberty%' ) then 'Liberty'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('MSG Entertainment - NY%' ) then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Chicago%' ) then 'Live - Chicago'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Forum%' ) then 'Live - Los Angeles'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('MSGE Family%' ) then 'Family Shows - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Marquee%' ) then 'Marquee'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Spectacular%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Rockettes%' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('MSG Sports%' ) then 'MSG Sports'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Chase Lounge MSG%') and upper(sj.exctgt_email_name) SIMILAR TO upper('% Rangers %' ) then 'Rangers'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Chase Lounge MSG%') and upper(sj.exctgt_email_name) SIMILAR TO upper('% Knicks %' ) then 'Knicks'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Chase Lounge MSG%') then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') and upper(sj.exctgt_email_name) SIMILAR TO upper('% RCMH %' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') and upper(sj.exctgt_email_name) SIMILAR TO upper('% RCCS %' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') and upper(sj.exctgt_email_name) SIMILAR TO upper('% NYS %' ) then 'Rockettes'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('Groups') then 'Family Shows - New York'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%Rangers%' ) then 'Rangers'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%Knick%' ) then 'Knicks'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%Liberty%' ) then 'Knicks'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% Knicks %' ) then 'Knicks'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% NYR %' ) then 'Rangers'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% NYK %' ) then 'Knicks'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% NYL %' ) then 'Liberty'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% MSGS %' ) then 'MSG Sports'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% MSGE %' ) then 'Live - New York'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% RCCS %' ) then 'Rockettes'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% NCAA %' ) then 'MSG Sports'
when upper(sj.exctgt_from_name) SIMILAR  to upper('%Chicago%' ) then 'Live - Chicago'
when upper(sj.exctgt_email_name) SIMILAR  to upper('%Chase Chicago%' ) then 'Live - Chicago'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% MSG %' ) then 'Live - New York'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Festival%') then 'Live - New York'
else 'Unmapped'
end as Brand
from ads_main.f_exctgt_job_kpis kpi
join ads_main.d_date dt
on dt.day_id=kpi.exctgt_email_send_day_id
join  ads_main.d_exctgt_sendjobs sj
on sj.exctgt_sendjob_id=kpi.exctgt_sendjob_id
where 1=1
AND  dt.full_date > CURRENT_DATE - INTERVAL '250 days'
and kpi.exctgt_email_address not like '%msg.com%'
and kpi.exctgt_email_address not like '%thegarden.com%'
and kpi.exctgt_email_address not like '%emailonacid.com'
and upper(kpi.exctgt_cell_code) not like upper('SEED%')
and upper(kpi.exctgt_campaign_code) not like upper('%BRIGIDTESTLIST%')
and upper(kpi.exctgt_campaign_code) not like upper('%TEST_%')
and  sj.exctgt_job_status = 'Complete'
),
job2 as
(
select job.month_name, 
job.Calendar_Year,
job.exctgt_ads_subscriber_id, 
job.brand,
sum(cast(job.exctgt_email_sent_count as float)) as sent, 
sum(cast(job.exctgt_email_unique_open_count as float)) as Opens
from job
group by job.month_name, 
 job.Calendar_Year,
job.exctgt_ads_subscriber_id, 
job.brand
),
job3 as
(
SELECT job2.month_name, job2.Calendar_Year,job2.brand, AVG(job2.sent) sent,AVG(job2.opens) opens
  FROM job2
  group by job2.month_name, job2.Calendar_Year,job2.brand
  ),
min_date as
(
select month_name, Calendar_Year, min(full_date) as full_date
from ads_main.d_date 
group by month_name, Calendar_Year
)
select job3.month_name, job3.Calendar_Year, job3.brand, min_date.full_date, job3.sent, job3.opens
from job3
join min_date
on job3.month_name=min_date.month_name
and job3.Calendar_Year=min_date.Calendar_Year