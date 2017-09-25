--Find Postings
with posts1
as
(
select
row_number() over(partition by owner_tm_acct_id, tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as nbr_posts,
owner_tm_acct_id, 
tm_event_name, 
tm_section_name, 
tm_row_name, 
tm_seat_num,
ticket_add_datetime as post_ticket_add_datetime
from ads_main.vw_ticket_exchange_event_seat
where 1=1
and tm_season_name IN ('2016-17 New York Knicks')
and owner_tm_acct_id= 2100249 
and ticket_exchange_activity_desc = 'TE Posting'
),
--Remove duplicate and additional Postings
posts2 as
(
select * from posts1
where nbr_posts = 1
),
--Find Resales
resale1
as
(
select
row_number() over(partition by owner_tm_acct_id, tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as nbr_sold,
count(*) over(partition by owner_tm_acct_id, tm_event_name, tm_section_name, tm_row_name, tickets_inet_transaction_amount, ticket_add_datetime) as ticket_div,
owner_tm_acct_id, 
tm_event_name, 
tm_section_name, 
tm_row_name, 
tm_seat_num,
buyer_customer_account_id,
buyer_tm_acct_id,
buyer_name_first,
buyer_name_mi,
buyer_name_last,
buyer_company_name,
buyer_street_addr_1,
buyer_street_addr_2,
buyer_city,
buyer_state,
buyer_zip,
buyer_country,
buyer_email_address,
tickets_orig_purchase_price,
tickets_seller_credit_amount,
tickets_seller_fees,
tickets_posting_price,
tickets_buyer_fees_hidden,
tickets_purchase_price,
tickets_buyer_fees_not_hidden,
tickets_inet_delivery_fee,
tickets_inet_transaction_amount,
ticket_add_datetime as ticket_add_datetime_rs
from ads_main.vw_ticket_exchange_event_seat
where 1=1
and tm_season_name IN ('2016-17 New York Knicks')
and owner_tm_acct_id= 2100249 
and ticket_exchange_activity_desc = 'TE Resale'
),
--Run calculations for resale field aggregated at the transaction level; Remove duplicate and additional Resales
resale2 as
(
select 
nbr_sold,
ticket_div,
owner_tm_acct_id, 
tm_event_name, 
tm_section_name, 
tm_row_name, 
tm_seat_num,
buyer_customer_account_id as buyer_customer_account_id_rs,
buyer_tm_acct_id as buyer_tm_acct_id_rs,
buyer_name_first as buyer_name_first_rs,
buyer_name_mi as buyer_name_mi_rs,
buyer_name_last as buyer_name_last_rs,
buyer_company_name as buyer_company_name_rs,
buyer_street_addr_1 as buyer_street_addr_1_rs,
buyer_street_addr_2 as buyer_street_addr_2_rs,
buyer_city as buyer_city_rs,
buyer_state as buyer_state_rs,
buyer_zip as buyer_zip_rs,
buyer_country as buyer_country_rs,
buyer_email_address as buyer_email_address_rs,
cast(tickets_seller_credit_amount/ticket_div as float) as tickets_seller_credit_amount_rs,
cast(tickets_seller_fees/ticket_div as float) as tickets_seller_fees_rs,
cast(tickets_posting_price/ticket_div as float) as tickets_posting_price_rs,
cast(tickets_buyer_fees_hidden/ticket_div as float) as tickets_buyer_fees_hidden_rs,
cast(tickets_purchase_price/ticket_div as float) as tickets_purchase_price_rs,
cast(tickets_buyer_fees_not_hidden/ticket_div as float) as tickets_buyer_fees_not_hidden_rs,
cast(tickets_inet_delivery_fee/ticket_div as float) as tickets_inet_delivery_fee_rs,
cast(tickets_inet_transaction_amount/ticket_div as float) as tickets_inet_transaction_amount_rs,
ticket_add_datetime_rs
 from resale1
where nbr_sold = 1
),
--Find Forwards
forward1
as
(
select
row_number() over(partition by owner_tm_acct_id, tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as nbr_forward,
owner_tm_acct_id, 
tm_event_name, 
tm_section_name, 
tm_row_name, 
tm_seat_num,
ticket_add_datetime as ticket_add_datetime_fwd,
buyer_customer_account_id as buyer_customer_account_id_fwd,
buyer_tm_acct_id as buyer_tm_acct_id_fwd,
buyer_name_first as buyer_name_first_fwd,
buyer_name_mi as buyer_name_mi_fwd,
buyer_name_last as buyer_name_last_fwd,
buyer_company_name as buyer_company_name_fwd,
buyer_street_addr_1 as buyer_street_addr_1_fwd,
buyer_street_addr_2 as buyer_street_addr_2_fwd,
buyer_city as buyer_city_fwd,
buyer_state as buyer_state_fwd,
buyer_zip as buyer_zip_fwd,
buyer_country as buyer_country_fwd,
buyer_email_address as buyer_email_address_fwd

from ads_main.vw_ticket_exchange_event_seat
where 1=1
and tm_season_name IN ('2016-17 New York Knicks')
and owner_tm_acct_id= 2100249 
and ticket_exchange_activity_desc = 'Forward'
),
--Remove duplicate and additional Forwards
forward2 as
(
select * from forward1
where nbr_forward = 1
),
--Find STMs (t ticket sales event seat)
stms1 as
(
select 
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as cust_sum,
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as max_date,
tm_acct_id,
ticket_status_desc,
full_date,
acct_status,
acct_type_desc,
primary_code,
company_name,
name_first,
name_mi,
name_last,
street_addr_1,
street_addr_2,
city,
state,
zip,
country,
assigned_acct_rep_num,
assigned_acct_rep_name,
email_address,
second_email_address,
tenure_start_date,
high_volume_buyer_flag,
tm_event_name,
tm_event_name_long,
tm_event_date,
tm_event_time,
tm_event_day,
tm_team,
tm_plan_abv,
tm_game_number,
tm_org_name,
tm_plan_flag,
tm_plan_group_name,
tm_plan_event_name,
mpd_indy_rank,
mpd_game_num,
tm_season_name,
tm_section_name,
tm_section_desc,
tm_row_name,
tm_seat_num,
txn_rep_email_addr,
txn_rep_phone,
txn_rep_phone_formatted,
tm_comp_code,
tm_comp_name,
tm_price_code,
tm_code,
tm_price_code_group,
tm_price_code_desc,
ticket_type_price_level,
ticket_product_description,
tickets_sold,
tickets_purchase_price,
tickets_pc_ticket,
tickets_total_gross_revenue,
tickets_total_revenue,
tm_order_id,
tickets_add_datetime,
ticket_sale_report_date,
ticket_premium_desc,
ticket_host_flag,
tm_sell_location_code,
account_manager_rep_full_name,
active_account_manager_rep_full_name,
active_sales_rep_full_name,
ads_source,
ticket_sell_location_name,
credited_sales_rep_full_name,
ticket_group_flag,
ticket_paid_flag
from ads_main.t_ticket_sales_event_seat
where 1=1
and tm_season_name IN ('2016-17 New York Knicks')
and tm_comp_name in('Null','Not Comp')
and ticket_type_price_level IN ('New Fulls', 'Renewals','Half Plan' ) 
--and tm_event_name = 'ENK0412E'
and tm_acct_id= 2100249 
),
--find the last sale of an event, section, row, seat (removing returns)
stms2 as
(
select * 
from stms1
where tickets_sold>0
and cust_sum >0
and max_date=tickets_add_datetime
)
--join all created tables together, outputting 1 row for each event, section, row, seat
select 
a.cust_sum,
a.tm_acct_id,
a.ticket_status_desc,
a.full_date,
a.acct_status,
a.acct_type_desc,
a.primary_code,
a.company_name,
a.name_first,
a.name_mi,
a.name_last,
a.street_addr_1,
a.street_addr_2,
a.city,
a.state,
a.zip,
a.country,
a.assigned_acct_rep_num,
a.assigned_acct_rep_name,
a.email_address,
a.second_email_address,
a.tenure_start_date,
a.high_volume_buyer_flag,
a.tm_event_name,
a.tm_event_name_long,
a.tm_event_date,
a.tm_event_time,
a.tm_event_day,
a.tm_team,
a.tm_plan_abv,
a.tm_game_number,
a.tm_org_name,
a.tm_plan_flag,
a.tm_plan_group_name,
a.tm_plan_event_name,
a.mpd_indy_rank,
a.mpd_game_num,
a.tm_season_name,
a.tm_section_name,
a.tm_section_desc,
a.tm_row_name,
a.tm_seat_num,
a.txn_rep_email_addr,
a.txn_rep_phone,
a.txn_rep_phone_formatted,
a.tm_comp_code,
a.tm_comp_name,
a.tm_price_code,
a.tm_code,
a.tm_price_code_group,
a.tm_price_code_desc,
a.ticket_type_price_level,
a.ticket_product_description,
a.tickets_sold,
a.tickets_purchase_price,
a.tickets_pc_ticket,
a.tickets_total_gross_revenue,
a.tickets_total_revenue,
a.tm_order_id,
a.tickets_add_datetime,
a.ticket_sale_report_date,
a.ticket_premium_desc,
a.ticket_host_flag,
a.tm_sell_location_code,
a.account_manager_rep_full_name,
a.active_account_manager_rep_full_name,
a.active_sales_rep_full_name,
a.ads_source,
a.ticket_sell_location_name,
a.credited_sales_rep_full_name,
a.ticket_group_flag,
a.ticket_paid_flag,
coalesce(b.nbr_posts,0) as nbr_posts,
b.post_ticket_add_datetime,
coalesce(c.nbr_sold,0) as nbr_sold,
coalesce(c.ticket_div,0) as ticket_div,
c.buyer_customer_account_id_rs,
c.buyer_tm_acct_id_rs,
c.buyer_name_first_rs,
c.buyer_name_mi_rs,
c.buyer_name_last_rs,
c.buyer_company_name_rs,
c.buyer_street_addr_1_rs,
c.buyer_street_addr_2_rs,
c.buyer_city_rs,
c.buyer_state_rs,
c.buyer_zip_rs,
c.buyer_country_rs,
c.buyer_email_address_rs,
c.tickets_seller_credit_amount_rs,
c.tickets_seller_fees_rs,
c.tickets_posting_price_rs,
c.tickets_buyer_fees_hidden_rs,
c.tickets_purchase_price_rs,
c.tickets_buyer_fees_not_hidden_rs,
c.tickets_inet_delivery_fee_rs,
c.tickets_inet_transaction_amount_rs,
c.ticket_add_datetime_rs,
coalesce(d.nbr_forward,0) as nbr_forward,
d.ticket_add_datetime_fwd,
d.buyer_customer_account_id_fwd,
d.buyer_tm_acct_id_fwd,
d.buyer_name_first_fwd,
d.buyer_name_mi_fwd,
d.buyer_name_last_fwd,
d.buyer_company_name_fwd,
d.buyer_street_addr_1_fwd,
d.buyer_street_addr_2_fwd,
d.buyer_city_fwd,
d.buyer_state_fwd,
d.buyer_zip_fwd,
d.buyer_country_fwd,
d.buyer_email_address_fwd

from stms2 a
left join posts2 b
on a.tm_acct_id=b.owner_tm_acct_id
and a.tm_event_name=b.tm_event_name
and a.tm_section_name=b.tm_section_name
and a.tm_row_name=b.tm_row_name
and a.tm_seat_num=b.tm_seat_num
left join resale2 c
on a.tm_acct_id=c.owner_tm_acct_id
and a.tm_event_name=c.tm_event_name
and a.tm_section_name=c.tm_section_name
and a.tm_row_name=c.tm_row_name
and a.tm_seat_num=c.tm_seat_num
left join forward2 d
on a.tm_acct_id=d.owner_tm_acct_id
and a.tm_event_name=d.tm_event_name
and a.tm_section_name=d.tm_section_name
and a.tm_row_name=d.tm_row_name
and a.tm_seat_num=d.tm_seat_num
order by a.tm_event_date
