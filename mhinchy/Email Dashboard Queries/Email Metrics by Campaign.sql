with all_emails as
(
select 
sj.exctgt_send_id, 
sj.exctgt_sendjob_id , 
sj.exctgt_sched_time , 
dt.full_date,
sj.exctgt_email_name , 
sj.exctgt_business_unit_name ,
sj.exctgt_subject ,
kpi.exctgt_campaign_code,
kpi.exctgt_cell_code,
sj.exctgt_from_name,
sum(kpi.exctgt_email_sent_count) as emails_sent,
sum(kpi.exctgt_email_delivered_count) as emails_delivered, 
sum(kpi.exctgt_email_bounced_count) as emails_bounced, 
sum(kpi.click_email_distinct_count) as Clicks,
sum(kpi.exctgt_email_unique_open_count) as Opens,
sum(kpi.exctgt_email_unsub_count) as opt_outs, 
--Email Brand Logic
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
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Prudential%') then 'Live - New York'
else 'Unmapped'
end as Brand,
--Email Category Logic
case 
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Weekly%' ) then 'Weekly Newsletter'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Pregame%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%ATT %' ) then 'Pregame - Purchased'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Pregame%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%NA %' ) then 'Pregame - Not Purchased'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Pregame%' ) AND upper(sj.exctgt_email_name) SIMILAR TO upper('%Away %' ) then 'Pregame - Away'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Pregame%')  then 'Pregame'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% Combo%' ) then 'Combo'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% Value%' ) then 'Value'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% RENW%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% Renew%' ) then 'Renewal'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% Psh%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% preshow%' )then 'Preshow'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% Pre%' ) then 'Presale'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% DISC%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% OFF%' ) then 'Discount'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% EOS%' ) then 'Early On Sale'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% RMDR%' ) then 'Reminder'
when upper(sj.exctgt_email_name) SIMILAR TO upper('%PARTNR%' )or  upper(sj.exctgt_email_name) SIMILAR TO upper('%PRTNR%' )then 'Partner Offer' 
when upper(sj.exctgt_email_name) SIMILAR TO upper('%SURVEY%' ) then 'Survey'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Insights%') then 'Survey'
when upper(sj.exctgt_business_unit_name) SIMILAR  to upper('%Youth%') then 'Juniors' 
when upper(sj.exctgt_email_name) SIMILAR TO upper('% YH %' ) then 'Juniors' 			
when upper(sj.exctgt_email_name) SIMILAR TO upper('% Chase %' ) then 'Chase Lounge' 
when upper(sj.exctgt_email_name) SIMILAR TO upper('% STH%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% STM%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% FULL PLAN%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% MINI%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% HALF%' )then 'STM'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% ONS%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%ON Sa%' )then 'Onsale'                                                             
else 'Miscellaneous'
end as EmailCategory 
from ads_main.f_exctgt_job_kpis kpi
join  ads_main.d_exctgt_sendjobs sj
            on sj.exctgt_sendjob_id=kpi.exctgt_sendjob_id
join ads_main.d_date dt
on kpi.exctgt_email_send_day_id=dt.day_id
join ads_main.d_exctgt_subscribers sub
            on sub.exctgt_ads_subscriber_id=kpi.exctgt_ads_subscriber_id
where 1=1
and  sj.exctgt_job_status = 'Complete'
and kpi.exctgt_email_address not like '%msg.com'
and kpi.exctgt_email_address not like '%thegarden.com'
and kpi.exctgt_email_address not like '%emailonacid.com'
and upper(kpi.exctgt_cell_code) not like upper('SEED%')
and upper(kpi.exctgt_campaign_code) not like upper('%BRIGIDTESTLIST%')
and upper(kpi.exctgt_campaign_code) not like upper('%TEST_%')
and sj.exctgt_sched_time > CURRENT_DATE - INTERVAL '30 days' 
group by sj.exctgt_send_id, 
sj.exctgt_sendjob_id ,
sj.exctgt_sched_time, 
sj.exctgt_email_name , 
sj.exctgt_business_unit_name, 
sj.exctgt_subject,
kpi.exctgt_campaign_code,
kpi.exctgt_cell_code,
dt.full_date,
sj.exctgt_from_name
order by sj.exctgt_send_id, sj.exctgt_sched_time
)
select * from all_emails
where all_emails.emails_sent >= 10
