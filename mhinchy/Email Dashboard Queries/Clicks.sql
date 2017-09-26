select 
      sj.exctgt_sendjob_id,
      sj.exctgt_job_status,
      sj.exctgt_business_unit_name,
      sj.exctgt_email_name,
	  sj.exctgt_sched_time,
      cl.exctgt_email_address,
	  cl.exctgt_click_url,
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
end as Brand,
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
when upper(sj.exctgt_email_name) SIMILAR TO upper('% STH%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% STM%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% MINI%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('% HALF%' )then 'STM'
when upper(sj.exctgt_email_name) SIMILAR TO upper('% ONS%' ) or  upper(sj.exctgt_email_name) SIMILAR TO upper('%ON Sa%' )then 'Onsale'                                                             
else 'Miscellaneous'
end as EmailCategory ,
case 
when cl.exctgt_click_url like '%unsub%' then 'Opt Out Link'
when cl.exctgt_click_url like '%view.email%' then 'View Email Link' 
else 'Value Link' end as click_url 
  from ads_main.d_exctgt_sendjobs sj
  inner join ads_main.f_exctgt_click_emails cl
   on sj.exctgt_sendjob_id=cl.exctgt_sendjob_id
  where 1=1
  and sj.exctgt_job_status = 'Complete'
and cl.exctgt_email_address not like '%msg.com'
and cl.exctgt_email_address not like '%thegarden.com'
and cl.exctgt_isunique_for_url_flag = 'True'
and sj.exctgt_sched_time > CURRENT_DATE - INTERVAL '30 days'