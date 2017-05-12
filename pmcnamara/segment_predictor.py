import pandas as pd
import numpy as np
import sqlalchemy
import redshift_sqlalchemy
import glob
import os
from sklearn.multiclass import OneVsRestClassifier
from sklearn.svm import LinearSVC

# importing segment flags #
knicks = pd.read_excel('/Users/mcnamarp/Downloads/MSG Segmentation phase_20170418_Knicks.xlsx', sheetname = 'Knicks')[['uuid','Segment Knicks']]
rangers = pd.read_excel('/Users/mcnamarp/Downloads/MSG Segmentation phase_20170418_Rangers.xlsx', sheetname = 'Rangers')[['uuid','Segment Rangers']]
'''
# creating labels #
segment_labels_knicks = pd.DataFrame(index = range(1,7), columns = ['segment'])
segment_labels_knicks['segment'] = ['Emo-Social','Purist','Root home','Competitor','Die-hards','Old Faithful']
segment_labels_rangers = pd.DataFrame(index = range(1,8), columns = ['segment'])
segment_labels_rangers['segment'] = ['Social Media','Scientist','Root home','Couch Potato','Live Game','Die-hards','Old Faithful']

# labeling survey respondents #
knicks = pd.merge(knicks, segment_labels_knicks, left_on = 'Segment Knicks', right_index = True).drop('Segment Knicks', axis = 1)
rangers = pd.merge(rangers, segment_labels_rangers, left_on = 'Segment Rangers', right_index = True).drop('Segment Rangers', axis = 1)
'''
segments = knicks.append(rangers, ignore_index = True)

# combining survey response data #
sth = pd.read_excel('/Users/mcnamarp/Downloads/fac17002/STH/fac17002.xlsx', sheetname = 'A1')[['uuid','source','Sample','vspt','Q1_Gender','Q1_Age','Q30','Q31','Q32','Q33r1','Q33r2','Q34']]
indy = pd.read_excel('/Users/mcnamarp/Downloads/fac17002/Individual_Game_Purchasers/fac17002.xlsx', sheetname = 'A1')[['uuid','source','Sample','vspt','Q1_Gender','Q1_Age','Q30','Q31','Q32','Q33r1','Q33r2','Q34']]
panel = pd.read_excel('/Users/mcnamarp/Downloads/fac17002/Panel/fac17002.xlsx', sheetname = 'A1')[['uuid','source','Sample','vspt','Q1_Gender','Q1_Age','Q30','Q31','Q32','Q33r1','Q33r2','Q34']]
survey_data = sth.append(panel, ignore_index = True).append(indy, ignore_index = True)
survey_data.replace({'Sample':{1:'Panel',2:'STH',3:'Indy'}}, inplace = True)
survey_data = pd.merge(survey_data, segments, on = 'uuid', how = 'left')

# mapping survey responses #
survey_data.replace({'vspt':{1:'hockey', 2:'basketball'}, 
		'Q1_Gender':{2:0},
		'Q30':{1:'Some high school', 2:'High school graduate', 3:'Vocational/trade school', 4:'Some college', 5:'College graduate', 6:'Post-graduate degree', 7:'Prefer not to say'}, 
		'Q31':{1:'Married or living as married', 2:'Widowed', 3:'Separated or divorced', 4:'Single, never been married', 5:'Prefer not to say'},
		'Q32':{1:'Employed full-time', 2: 'Employed part-time', 3:'Self-employed', 4:'Temporarily unemployed', 5:'Homemaker', 6:'Student', 7:'Retired', 8:'Prefer not to say'},
		'Q34':{1:'Under $25,000', 2:'$25,000 to $34,999', 3:'$35,000 to $49,999', 4:'$50,000 to $99,999', 5:'$100,000 to $149,999', 6:'$150,000 to $199,999', 7:'$200,000 to $249,999', 8:'$250,000 or more', 9:'Prefer not to say'}},
		inplace = True)

survey_data['Q33r2'].fillna(0, inplace = True)
survey_data.set_index('uuid', inplace = True)
dummies = pd.get_dummies(survey_data[['Q30','Q31','Q32','Q34']])
survey_data = survey_data.drop(['Q30','Q31','Q32','Q34'], axis = 1).join(dummies)

datah = survey_data.dropna(subset = ['Segment Rangers']).drop(['vspt','Segment Knicks','source','Sample'], axis = 1)
datab = survey_data.dropna(subset = ['Segment Knicks']).drop(['vspt','Segment Rangers','source','Sample'], axis = 1)

xb, yb = datab.drop(['Segment Knicks'], axis = 1), datab['Segment Knicks']
datab['pred'] = OneVsRestClassifier(LinearSVC(random_state=0)).fit(xb, yb).predict(xb)
len(datab[datab['Segment Knicks'] == datab['pred']])/float(len(datab))

xh, yh = datah.drop(['Segment Rangers'], axis = 1), datah['Segment Rangers']
datah['pred'] = OneVsRestClassifier(LinearSVC(random_state=0)).fit(xh, yh).predict(xh)
len(datah[datah['Segment Rangers'] == datah['pred']])/float(len(datah))

# mapping respondents via Nat's files and combining #
path = '/Users/mcnamarp/Downloads/FW%3a_Survey_Lists'
all_files = glob.glob(os.path.join(path, "*.csv"))
df_from_each_file = (pd.read_csv(f, dtype = {'acct_id':'str'}) for f in all_files)
concatenated_df = pd.concat(df_from_each_file, ignore_index=True)
mapping = concatenated_df[['EMAIL','uid']].rename(columns = {'EMAIL':'email'}).dropna()
mapping['email'] = mapping['email'].str.lower()

data = pd.merge(mapping, survey_data, left_on = 'uid', right_on = 'source').drop('source', axis = 1)

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@msgbiadb-prod.cqp6htpq4zp6.us-east-1.rds.amazonaws.com:5432/msgbiadb")
revenue_query = '''
SELECT email, description, regexp_replace(tm_season_name, '^.* ', '') AS team, cost::integer, tickets FROM (
SELECT lower(email_address) AS email, ticket_product_description AS description, SUM(CASE WHEN tm_price_code_desc = 'Madison Club' THEN 500 ELSE tickets_total_revenue END) AS cost, SUM(tickets_sold) AS tickets, tm_season_name
FROM ads_main.t_ticket_sales_event_seat A
WHERE tm_season_name IN ('2016-17 New York Knicks','2016-17 New York Rangers') AND tm_comp_code = '0'
GROUP BY email_address, ticket_product_description, tm_season_name) A;
'''
tm_revenue = pd.read_sql(revenue_query, engine)
tm_revenue = pd.pivot_table(tm_revenue, index = ['email'], columns = ['team','description'], fill_value = 0)
tm_revenue.columns = [e[0] + e[1] + e[2] for e in tm_revenue.columns.tolist()]

data = pd.merge(data, tm_revenue, left_on ='email', right_index = True)

datah = data.dropna(subset = ['Segment Rangers']).drop(['vspt','Segment Knicks','uid','Sample','email'], axis = 1)
datab = data.dropna(subset = ['Segment Knicks']).drop(['vspt','Segment Rangers','uid','Sample','email'], axis = 1)

xb, yb = datab.drop(['Segment Knicks'], axis = 1), datab['Segment Knicks']
datab['pred'] = OneVsOneClassifier(LinearSVC(random_state=0)).fit(xb, yb).predict(xb)
len(datab[datab['Segment Knicks'] == datab['pred']])/float(len(datab))

xh, yh = datah.drop(['Segment Rangers'], axis = 1), datah['Segment Rangers']
datah['pred'] = OneVsOneClassifier(LinearSVC(random_state=0)).fit(xh, yh).predict(xh)
len(datah[datah['Segment Rangers'] == datah['pred']])/float(len(datah))