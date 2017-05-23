import pandas as pd
import numpy as np
import sqlalchemy
import redshift_sqlalchemy
import glob
import os
from sklearn.linear_model import LogisticRegression
from sklearn import metrics, cross_validation, LogisticRegression
from hep_ml.reweight import GBReweighter

# importing segment flags #
knicks = pd.read_excel('/Users/mcnamarp/Downloads/MSG Segmentation phase_20170418_Knicks.xlsx', sheetname = 'Knicks')[['uuid','Segment Knicks']]
rangers = pd.read_excel('/Users/mcnamarp/Downloads/MSG Segmentation phase_20170418_Rangers.xlsx', sheetname = 'Rangers')[['uuid','Segment Rangers']]

# creating labels #
segment_labels_knicks = pd.DataFrame(index = range(1,7), columns = ['segment'])
segment_labels_knicks['segment'] = ['Emo-Social','Purist','Root home','Competitor','Die-hards','Old Faithful']
segment_labels_rangers = pd.DataFrame(index = range(1,8), columns = ['segment'])
segment_labels_rangers['segment'] = ['Social Media','Scientist','Root home','Couch Potato','Live Game','Die-hards','Old Faithful']

# labeling survey respondents #
knicks = pd.merge(knicks, segment_labels_knicks, left_on = 'Segment Knicks', right_index = True).drop('Segment Knicks', axis = 1)
rangers = pd.merge(rangers, segment_labels_rangers, left_on = 'Segment Rangers', right_index = True).drop('Segment Rangers', axis = 1)

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
SELECT email, (now()::DATE - tenure_start)/365::INT AS tenure, description, regexp_replace(tm_season_name, '^.* ', '') AS team, cost::integer, tickets FROM (
SELECT lower(email_address) AS email, tm_season_name, SUM(tickets_sold) AS tickets, ticket_product_description AS description, 
CASE WHEN ticket_product_description = 'Individuals' THEN NULL ELSE tenure_start_date::DATE END AS tenure_start, SUM(CASE WHEN tm_price_code_desc = 'Madison Club' THEN 500 ELSE tickets_total_revenue END) AS cost
FROM ads_main.t_ticket_sales_event_seat A
WHERE tm_season_name IN ('2016-17 New York Knicks','2016-17 New York Rangers') AND tm_comp_code = '0' AND email_address IS NOT NULL
GROUP BY email_address, tenure_start_date, ticket_product_description, tm_season_name) A;
'''
tm_revenue = pd.read_sql(revenue_query, engine)
knicks_purchasers = tm_revenue[tm_revenue['team'] == 'Knicks']['email'].dropna()
rangers_purchasers = tm_revenue[tm_revenue['team'] == 'Rangers']['email'].dropna()
tm_revenue = pd.pivot_table(tm_revenue.drop(['tenure'], axis = 1), index = ['email'], columns = ['team','description'], fill_value = 0)
tm_revenue.columns = [e[0] + e[1] + e[2] for e in tm_revenue.columns.tolist()]

datab = data[data['email'].isin(knicks_purchasers)]
datab = pd.merge(datab, tm_revenue, left_on ='email', right_index = True, how = 'right').drop(['vspt','uid','Sample','Segment Rangers'], axis = 1)
datah = data[data['email'].isin(rangers_purchasers)]
datah = pd.merge(datah, tm_revenue, left_on ='email', right_index = True, how = 'right').drop(['vspt','uid','Sample', 'Segment Knicks'], axis = 1)


# CREATING SAMPLE WEIGHTS # (https://arogozhnikov.github.io/hep_ml/reweight.html)
res_cols = list(tm_revenue.reset_index().columns)
resampling_b = datab[['Segment Knicks'] + res_cols]
resampling_h = datah[['Segment Rangers'] + res_cols]
sampleb = resampling_b.dropna(subset = ['Segment Knicks']).drop(['Segment Knicks'], axis = 1).set_index('email')
fullb = resampling_b[pd.isnull(resampling_b['Segment Knicks'])].drop(['Segment Knicks'], axis = 1).set_index('email')
sampleh = resampling_h.dropna(subset = ['Segment Rangers']).drop(['Segment Rangers'], axis = 1).set_index('email')
fullh = resampling_h[pd.isnull(resampling_h['Segment Rangers'])].drop(['Segment Rangers'], axis = 1).set_index('email')
reweighter = GBReweighter(learning_rate = 0.01, max_depth = 12)
sampleb['weight'] = reweighter.fit(original = sampleb, target=fullb).predict_weights(sample).round(2)
sampleh['weight'] = reweighter.fit(original = sampleh, target=fullh).predict_weights(sample).round(2)

# PREDICTING SEGMENTS #
targets = data[['uid','vspt','segment']].set_index('uid')
targets['Segment'] = targets['vspt'].astype(str) + '_' + targets['segment'].astype(str)
targets = pd.get_dummies(targets['Segment']).drop(['basketball_nan','hockey_nan'], axis = 1)
data = data.set_index('uid').join(targets)

datah = data[data['vspt'] == 'hockey'].drop(['vspt','Sample','email','segment'], axis = 1)
datab = data[data['vspt'] == 'basketball'].drop(['vspt','Sample','email','segment'], axis = 1)

clf = GaussianNB()
target_columnsb = [x for x in targets.columns if "hockey" not in x]
for j in target_columnsb:
	xb, yb = datab.drop(targets.columns, axis = 1), datab[j]
	clf.fit(xb.values,yb.values).predict_proba(xb).T[0]

target_columnsh = [x for x in targets.columns if "basketball" not in x]
for j in targets_columnsh:
	xh, yh = datah.drop(targets_columnsh, axis = 1), datah[jd]

# LOGIT MODELING #
modeling_bball = survey_data[survey_data['vspt'] == 'basketball'].dropna(subset = ['Segment Knicks']).drop(['source','Sample','vspt'], axis = 1)
modeling_hockey = survey_data[survey_data['vspt'] == 'hockey'].dropna(subset = ['Segment Rangers']).drop(['source','Sample','vspt'], axis = 1)

predicted_b = cross_validation.cross_val_predict(LogisticRegression(), modeling_bball.drop(['Segment Knicks','Segment Rangers'], axis= 1), modeling_bball['Segment Knicks'], cv=10)
metrics.accuracy_score(modeling_bball['Segment Knicks'], predicted_b)
print metrics.classification_report(modeling_bball['Segment Knicks'], predicted_b)
print metrics.confusion_matrix(modeling_bball['Segment Knicks'], predicted_b)
