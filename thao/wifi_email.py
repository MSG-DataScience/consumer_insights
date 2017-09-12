import pandas as pd
import numpy as np
import sqlalchemy
engine = sqlalchemy.create_engine("redshift+psycopg2://haot:Welcome9582!@rsmsgbia.c5dyht7ygr3w.us-east-1.redshift.amazonaws.com:5476/msgbiadb")
email_query = '''
SELECT DISTINCT email_address
FROM msgbiadb.ads_main.d_customer_account
'''
NYL_email_query='''
SELECT DISTINCT email_address
FROM msgbiadb.ads_main.d_customer_account
where tm_database='LIBERTY'
'''


a=pd.read_csv('C:/Users/haot/device-report-2017-09-10.csv')
a=a[a["E-mail"].str.contains("@msg.com") == False]
b=pd.read_csv('C:/Users/haot/device-report-2017-09-09.csv')
b=b[b["E-mail"].str.contains("@msg.com") == False]
c=pd.read_csv('C:/Users/haot/device-report-2017-09-08.csv')
c=c[c["E-mail"].str.contains("@msg.com") == False]

d= pd.read_sql(email_query, engine)
e= pd.read_sql(NYL_email_query, engine)

ad = pd.merge(a, d, how ='left', left_on = ['E-mail'], right_on = ['email_address'])
print(np.sum(pd.notnull(ad.email_address)))
bd = pd.merge(b, d, how ='left', left_on = ['E-mail'], right_on = ['email_address'])
print(np.sum(pd.notnull(bd.email_address)))
cd = pd.merge(c, d, how ='left', left_on = ['E-mail'], right_on = ['email_address'])
print(np.sum(pd.notnull(cd.email_address)))
ae = pd.merge(a, e, how ='left', left_on = ['E-mail'], right_on = ['email_address'])
print(np.sum(pd.notnull(ae.email_address)))
be = pd.merge(b, e, how ='left', left_on = ['E-mail'], right_on = ['email_address'])
print(np.sum(pd.notnull(be.email_address)))
ce = pd.merge(c, e, how ='left', left_on = ['E-mail'], right_on = ['email_address'])
print(np.sum(pd.notnull(ce.email_address)))