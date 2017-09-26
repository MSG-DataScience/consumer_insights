select 	 
kpi.exctgt_sendjob_id, 
sj.exctgt_send_id, 
sj.exctgt_subject, 
sj.exctgt_email_name,  
sj.exctgt_business_unit_name,  
kpi.exctgt_campaign_code, 
kpi.exctgt_cell_code, 
kpi.exctgt_email_send_day_id,
kpi.exctgt_ads_subscriber_id,
cl.exctgt_email_address, 
cl.exctgt_isunique_for_url_flag, 
cl.click_email_distinct_url_count, 
cl.exctgt_click_url as all_urls,
case 
when cl.exctgt_click_url like '%unsub%' then 'unsubscribe link'
when cl.exctgt_click_url like '%view.email%' then 'view email' 
when cl.exctgt_click_url like '%mi.msg.com%' then 'exact target redirect' 
when cl.exctgt_click_url like '%click.email1.msg.com%' then 'movable ink' 
else cl.exctgt_click_url end as click_url 
 from
ads_main.f_exctgt_job_kpis kpi
inner join ads_main.f_exctgt_click_emails cl
on kpi.exctgt_sendjob_id=cl.exctgt_sendjob_id
and kpi.exctgt_ads_subscriber_id=cl.exctgt_ads_subscriber_id 
inner join ads_main.d_exctgt_sendjobs sj
            on sj.exctgt_sendjob_id=kpi.exctgt_sendjob_id
 WHERE 1=1
and  sj.exctgt_job_status = 'Complete'
and kpi.exctgt_email_address not like '%msg.com'
and kpi.exctgt_email_address not like '%thegarden.com'
and kpi.exctgt_email_address not like '%emailonacid.com'
and upper(kpi.exctgt_cell_code) not like upper('SEED%')
and upper(kpi.exctgt_campaign_code) not like upper('%BRIGIDTESTLIST%')
and upper(kpi.exctgt_campaign_code) not like upper('%TEST_%')
and kpi.exctgt_email_send_time > CURRENT_DATE - INTERVAL '7 days' 
	