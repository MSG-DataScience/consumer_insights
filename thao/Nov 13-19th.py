# -*- coding: utf-8 -*-
"""
Created on Thu Nov 16 15:16:50 2017

@author: haot
"""
from datetime import date, timedelta
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
zip,
country,
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
WHERE tm_season_name IN ('2016 RCCS 1st Half','2016 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
and acct_type_desc != 'Tix Pro Seller'
AND house_seat_flag='N'
and tm_acct_id not in(-1,-2)
and full_date<'2017-11-20'
)
select distinct tm_acct_id,zip,country
from table1
'''

query2 = '''
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
zip,country,
tm_event_name_long,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
AND house_seat_flag='N'
and acct_type_desc != 'Tix Pro Seller'
and tm_acct_id not in(-1,-2)
and full_date<'2017-11-20'
)
select tm_acct_id,zip,country
from table1
'''

query3 = '''
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
zip,country,
tm_event_name_long,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2016 RCCS 1st Half','2016 RCCS 2nd Half','2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
AND house_seat_flag='N'
and acct_type_desc != 'Tix Pro Seller'
and tm_acct_id not in(-1,-2)
and full_date>'2017-11-12'
and full_date<'2017-11-20'
)
select tm_acct_id,zip,country
from table1
'''

query4 = '''
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
zip,country,
mpd_indy_rank,
mpd_game_num,
tm_event_name_long,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2016 RCCS 1st Half','2016 RCCS 2nd Half','2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
and acct_type_desc != 'Tix Pro Seller'
AND house_seat_flag='N'
and tm_acct_id not in(-1,-2)
and full_date<'2017-11-13'
)
select distinct tm_acct_id,zip,country
from table1
'''
query5 = '''
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
zip,
country,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
AND house_seat_flag='N'
and acct_type_desc != 'Tix Pro Seller'
and tm_acct_id not in(-1,-2)
and full_date>'2017-11-12'
and full_date<'2017-11-20'
)
select tm_acct_id,zip,country
from table1
'''
query6 = '''
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
zip,
country,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc != 'Tix Pro Seller'
and acct_type_desc!= 'Sponsor'
AND house_seat_flag='N'
and tm_acct_id not in(-1,-2)
and full_date<'2017-11-20'
)
select tm_acct_id,zip,country
from table1
'''

query7 = '''
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
zip,
country,
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
WHERE tm_season_name IN ('2016 RCCS 1st Half','2016 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
and acct_type_desc != 'Tix Pro Seller'
AND house_seat_flag='N'
and tm_acct_id not in(-1,-2)
and full_date<'2017-11-20'
)
select distinct tm_acct_id,zip,country
from table1
'''

query8 = '''
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
zip,country,
mpd_indy_rank,
mpd_game_num,
tm_event_name_long,
ticket_sell_location_name
from ads_main.t_ticket_sales_event_seat
WHERE tm_season_name IN ('2016 RCCS 1st Half','2016 RCCS 2nd Half','2017 RCCS 1st Half','2017 RCCS 2nd Half') 
and ticket_group_flag='N' 
AND tm_comp_name = 'Not Comp' 
and ticket_type_price_level = 'Individuals'
and acct_type_desc!='Trade Desk' 
and acct_type_desc!= 'Sponsor'
and acct_type_desc != 'Tix Pro Seller'
AND house_seat_flag='N'
and tm_acct_id not in(-1,-2)
and full_date<'2017-11-13'
)
select distinct tm_acct_id,zip,country
from table1
'''

#full_date>    DATEADD(DAY,-7,GETDATE())
data1= pd.read_sql(query1, engine)
data2= pd.read_sql(query2, engine)
year=pd.merge(data2,data1,how='inner',on=['tm_acct_id','zip','country'])
print(len(year.index))

data11= pd.read_sql(query7, engine)
data2= pd.read_sql(query2, engine)
year2015=pd.merge(data2,data11,how='inner',on=['tm_acct_id','zip','country'])
print(len(year2015.index))

data3= pd.read_sql(query3, engine)
data4= pd.read_sql(query4, engine)
week=pd.merge(data3,data4,how='inner',on=['tm_acct_id','zip','country'])
print(len(week.index))

data5= pd.read_sql(query5, engine)
location=pd.read_csv('location.csv')
data5['zip']=data5['zip'].apply(lambda x: str(x.encode('utf-8')))
location['zip']=location['zip'].apply(lambda x: str(x))
data5['zip']=data5['zip'].apply(lambda x : x[1:] if x.startswith('0') else x)
filter = data5["zip"] != ""
data1 = data5[filter]
data5= data5[pd.notnull(data5['zip'])]
data5['zip'] = data5['zip'].str[0:5]
data5=pd.merge(data5,location,how='left',on='zip')
data5['digit']=data5['zip'].apply(lambda x: str.isdigit(x))
def f(row):
    if (row['value']==0 and row['country'] == 'United States') or (row['value']==0 and row['country'] == 'US') or (row['value']==0 and row['country'] == 'USA') or (row['value']==0 and row['country'] == '') :
        val = 0
    elif row['country'] == 'United States' or row['country'] == 'US' or row['country'] == 'USA' or (row['country'] == '' and row['digit']==True)  :
        val = 1
    else:
        val = 2
    return val

data5['C'] = data5.apply(f, axis=1)
data7=data5[data5['C']==1]
print(len(data7.index))

data6= pd.read_sql(query6, engine)
location=pd.read_csv('location.csv')
data6['zip']=data6['zip'].apply(lambda x: str(x.encode('utf-8')))
data6['zip']=data6['zip'].apply(lambda x : x[1:] if x.startswith('0') else x)
location['zip']=location['zip'].apply(lambda x: str(x))
filter = data6["zip"] != ""
data6 = data6[filter]
data6= data6[pd.notnull(data6['zip'])]
data6['zip'] = data6['zip'].str[0:5]
data6=pd.merge(data6,location,how='left',on='zip')
data6['digit']=data6['zip'].apply(lambda x: str.isdigit(x))
def f(row):
    if (row['value']==0 and row['country'] == 'United States') or (row['value']==0 and row['country'] == 'US') or (row['value']==0 and row['country'] == 'USA') or (row['value']==0 and row['country'] == '') :
        val = 0
    elif row['country'] == 'United States' or row['country'] == 'US' or row['country'] == 'USA' or (row['country'] == '' and row['digit']==True)  :
        val = 1
    else:
        val = 2
    return val

data6['C'] = data6.apply(f, axis=1)
data8=data6[data6['C']==1]
print(len(data8.index))

location=pd.read_csv('location.csv')
year['zip']=year['zip'].apply(lambda x: str(x.encode('utf-8')))
year['zip']=year['zip'].apply(lambda x : x[1:] if x.startswith('0') else x)
location['zip']=location['zip'].apply(lambda x: str(x))
filter = year["zip"] != ""
year2= year[filter]
year2= year2[pd.notnull(year['zip'])]
year2['zip'] = year2['zip'].str[0:5]
year2=pd.merge(year2,location,how='left',on='zip')
year2['digit']=year2['zip'].apply(lambda x: str.isdigit(x))
def f(row):
    if (row['value']==0 and row['country'] == 'United States') or (row['value']==0 and row['country'] == 'US') or (row['value']==0 and row['country'] == 'USA') or (row['value']==0 and row['country'] == '') :
        val = 0
    elif row['country'] == 'United States' or row['country'] == 'US' or row['country'] == 'USA' or (row['country'] == '' and row['digit']==True)  :
        val = 1
    else:
        val = 2
    return val

year2['C'] = year2.apply(f, axis=1)
year2=year2[year2['C']==1]
print(len(year2.index))


location=pd.read_csv('location.csv')
week['zip']=week['zip'].apply(lambda x: str(x.encode('utf-8')))
week['zip']=week['zip'].apply(lambda x : x[1:] if x.startswith('0') else x)
location['zip']=location['zip'].apply(lambda x: str(x))
filter = week["zip"] != ""
week2= week[filter]
week2= week2[pd.notnull(week2['zip'])]
week2['zip'] = week2['zip'].str[0:5]
week2=pd.merge(week2,location,how='left',on='zip')
week2['digit']=week2['zip'].apply(lambda x: str.isdigit(x))

def f(row):
    if (row['value']==0 and row['country'] == 'United States') or (row['value']==0 and row['country'] == 'US') or (row['value']==0 and row['country'] == 'USA') or (row['value']==0 and row['country'] == '') :
        val = 0
    elif row['country'] == 'United States' or row['country'] == 'US' or row['country'] == 'USA' or (row['country'] == '' and row['digit']==True)  :
        val = 1
    else:
        val = 2
    return val

week2['C'] = week2.apply(f, axis=1)
week2=week2[week2['C']==1]
print(len(week2.index))

print(len(data2.index))
print(len(data3.index))
print(len(data2.index)-len(year.index)-len(data8.index)+len(year2.index))
print(len(data3.index)-len(week.index)-len(data7.index)+len(week2.index))
pastweek = date.today()- timedelta(7)
today = date.today()


dfa= pd.DataFrame(index=['2017_season','past_week'], columns=['repeat_purchased','>75 miles','repeat_purchased_and_>75_ miles','other','total'])
dfa.set_value('2017_season','repeat_purchased',len(year.index))
dfa.set_value('2017_season','>75 miles',len(data8.index))
dfa.set_value('2017_season','repeat_purchased_and_>75_ miles',len(year2.index))
dfa.set_value('2017_season','other',len(data2.index)-len(year.index)-len(data8.index)+len(year2.index))
dfa.set_value('2017_season','total',len(data2.index))
dfa.set_value('past_week','repeat_purchased',len(week.index))
dfa.set_value('past_week','>75 miles',len(data7.index))
dfa.set_value('past_week','repeat_purchased_and_>75_ miles',len(week2.index))
dfa.set_value('past_week','other',len(data3.index)-len(week.index)-len(data7.index)+len(week2.index))
dfa.set_value('past_week','total',len(data3.index))

dfb= pd.DataFrame(index=['2017_season','past_week'], columns=['repeat_purchased','>75 miles','repeat_purchased_and_>75_ miles','other'])
dfb.set_value('2017_season','repeat_purchased',"{0:.3f}".format(float(len(year.index))/len(data2.index)))
dfb.set_value('2017_season','>75 miles',"{0:.3f}".format(float(len(data8.index))/len(data2.index)))
dfb.set_value('2017_season','repeat_purchased_and_>75_ miles',"{0:.3f}".format(float(len(year2.index))/len(data2.index)))
dfb.set_value('2017_season','other',"{0:.3f}".format(float(len(data2.index)-len(year.index)-len(data8.index)+len(year2.index))/len(data2.index)))

dfb.set_value('past_week','repeat_purchased',"{0:.3f}".format(float(len(week.index))/len(data3.index)))
dfb.set_value('past_week','>75 miles',"{0:.3f}".format(float(len(data7.index))/len(data3.index)))
dfb.set_value('past_week','repeat_purchased_and_>75_ miles',"{0:.3f}".format(float(len(week2.index))/len(data3.index)))
dfb.set_value('past_week','other',"{0:.3f}".format(float(len(data3.index)-len(week.index)-len(data7.index)+len(week2.index))/len(data3.index)))

dfc=dfa.append(dfb)
dfc=dfc[['repeat_purchased','>75 miles','repeat_purchased_and_>75_ miles','other','total']]


dfc.to_csv('Rockettes_stats.csv')
'''
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText

emailfrom = "Tianyu.Hao@msg.com"
emailto = "allyson.keane@msg.com"
fileToSend = "Rockettes_stats.csv"
username = "Tianyu.Hao@msg.com"
password = "Dududainb1104"

msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = emailto
msg["Subject"] = "Rockettes_stats"
msg.preamble = "Rockettes_stats"

ctype, encoding = mimetypes.guess_type(fileToSend)
if ctype is None or encoding is not None:
    ctype = "application/octet-stream"

maintype, subtype = ctype.split("/", 1)

if maintype == "text":
    fp = open(fileToSend)
    # Note: we should handle calculating the charset
    attachment = MIMEText(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "image":
    fp = open(fileToSend, "rb")
    attachment = MIMEImage(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "audio":
    fp = open(fileToSend, "rb")
    attachment = MIMEAudio(fp.read(), _subtype=subtype)
    fp.close()
else:
    fp = open(fileToSend, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)
attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
msg.attach(attachment)

server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.starttls()
server.login(username,password)
server.sendmail(emailfrom, emailto, msg.as_string())
server.quit()

emailfrom = "Tianyu.Hao@msg.com"
emailto = "patrick.mcnamara@msg.com"
fileToSend = "Rockettes_stats.csv"
username = "Tianyu.Hao@msg.com"
password = "Dududainb1104"

msg = MIMEMultipart()
msg["From"] = emailfrom
msg["To"] = emailto
msg["Subject"] = "Rockettes_stats"
msg.preamble = "Rockettes_stats"

ctype, encoding = mimetypes.guess_type(fileToSend)
if ctype is None or encoding is not None:
    ctype = "application/octet-stream"

maintype, subtype = ctype.split("/", 1)

if maintype == "text":
    fp = open(fileToSend)
    # Note: we should handle calculating the charset
    attachment = MIMEText(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "image":
    fp = open(fileToSend, "rb")
    attachment = MIMEImage(fp.read(), _subtype=subtype)
    fp.close()
elif maintype == "audio":
    fp = open(fileToSend, "rb")
    attachment = MIMEAudio(fp.read(), _subtype=subtype)
    fp.close()
else:
    fp = open(fileToSend, "rb")
    attachment = MIMEBase(maintype, subtype)
    attachment.set_payload(fp.read())
    fp.close()
    encoders.encode_base64(attachment)
attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
msg.attach(attachment)

server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.starttls()
server.login(username,password)
server.sendmail(emailfrom, emailto, msg.as_string())
server.quit()'''