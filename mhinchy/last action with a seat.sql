with table1
as
(
select 
--number of tickets purchased by an account_id
SUM(tickets_sold) over(partition by tm_acct_id) as nbr_bought,
--ticket balance of the seat (1 or 0)
SUM(tickets_sold) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as seat_sum,
--last sale OR return timestamp of the seat
max(tickets_add_datetime) OVER (PARTITION BY tm_event_name, tm_section_name, tm_row_name, tm_seat_num) as max_date,
tm_acct_id,tm_event_name, 
tm_section_name, 
tm_row_name, 
tm_seat_num, 
tickets_sold,
tickets_total_revenue, 
tickets_add_datetime, 
ticket_transaction_date,
ticket_type_price_level
from ads_main.t_ticket_sales_event_seat
where 1=1
and tm_season_name IN ('2016-17 New York Knicks')
and tm_comp_name in('Null','Not Comp')
and tm_event_name='ENK0412E'
--and ticket_type_price_level IN (/*'Half Plan'*/'New Fulls', 'Renewals' ) 
--and tm_section_name = 1
--and tm_row_name= 14
--and tm_seat_num = 6
)
select max_date,
tickets_sold,
nbr_bought,
seat_sum, 
tm_event_name, 
tm_section_name, 
tm_row_name, 
tm_seat_num, 
tickets_add_datetime, 
ticket_type_price_level
from table1
where tickets_sold>0
and seat_sum >0
and max_date=tickets_add_datetime
order by tickets_sold,tm_event_name, tm_section_name, tm_row_name, tm_seat_num
