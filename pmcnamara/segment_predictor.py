import pandas as pd
import numpy as np
import sqlalchemy
import redshift_sqlalchemy
import glob
import os
#from sklearn.linear_model import LogisticRegression
#from sklearn import metrics, cross_validation, LogisticRegression
from hep_ml.reweight import GBReweighter

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

emails = data['email'].values
emails_query = '''
SELECT exctgt_subscriber_email_address AS email_address, exctgt_email_sent_count AS SentCnt, exctgt_email_delivered_count AS DeliveredCnt, 
exctgt_email_bounced_count AS BounceCnt, exctgt_email_unsub_count AS UnsubCnt, click_email_count AS TotalClickCnt, click_email_distinct_count AS DistinctClickCnt,
click_email_distinct_url_count AS DistinctUrlCnt, exctgt_email_open_count AS TotalOpenCnt, exctgt_email_unique_open_count AS DistinctOpenCnt
FROM ads_main.f_exctgt_job_kpis
WHERE exctgt_subscriber_email_address IN'''

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
reweighter = GBReweighter()
sampleb['weight'] = reweighter.fit(original = sampleb, target=fullb).predict_weights(sampleb).round(3)
sampleh['weight'] = reweighter.fit(original = sampleh, target=fullh).predict_weights(sampleh).round(3)

# LOGIT MODELING #
modeling_bball = pd.merge(data[data['vspt'] == 'basketball'], sampleb['weight'].reset_index(), on = 'email').drop(['vspt','Sample','email','Segment Rangers'], axis = 1).set_index('uid')
modeling_hockey = pd.merge(data[data['vspt'] == 'hockey'], sampleh['weight'].reset_index(), on = 'email').drop(['vspt','Sample','email','Segment Knicks'], axis = 1).set_index('uid')

ada_knicks_scores = list()
knicks_scores = pd.DataFrame(columns = range(0,6))
reweighter = GBReweighter()
for i in range(100):
	train = modeling_bball.sample(frac = 0.8, random_state = i)
	test = modeling_bball.loc[~modeling_bball.index.isin(train.index)]
	mdl = AdaBoostClassifier().fit(train.drop(['Segment Knicks','weight'], axis= 1), train['Segment Knicks'], sample_weight = train['weight'].values)
	ada_knicks_scores.append(mdl.score(test.drop(['Segment Knicks','weight'], axis = 1), test['Segment Knicks']))

	temp = pd.DataFrame(mdl.predict_proba(test.drop(['Segment Knicks','weight'], axis= 1)))
	temp['Segment'] = test['Segment Knicks'].values
	knicks_scores = knicks_scores.append(temp)

knicks_scores = knicks_scores.reset_index().drop(['index'], axis = 1)
knicks_scores.columns = [1,2,3,4,5,6,'Segment']
knicks_scores['max'] = knicks_scores.drop(['Segment'], axis = 1).idxmax(axis=1)
limits = ((data['Segment Knicks'].value_counts()/len(data['Segment Knicks'].dropna()))*len(knicks_scores)).round(0)
knicks_scores['new_max'] = np.nan
for i in limits.index:
	temp = knicks_scores[pd.isnull(knicks_scores['new_max'])]
	knicks_scores.ix[knicks_scores.index.isin(temp[i].nlargest(int(limits.ix[i])).index), 'new_max'] = i

rangers_scores = pd.DataFrame(columns = np.arange(10,210,10))
for i in range(1000):
	train = modeling_hockey.sample(frac = 0.8)
	test = modeling_hockey.loc[~modeling_hockey.index.isin(train.index)]
	mdl = LogisticRegression().fit(train.drop(['Segment Rangers','weight'], axis= 1), train['Segment Rangers'], sample_weight = train['weight'])
	scores.append(mdl.score(test.drop(['Segment Rangers','weight'], axis= 1), test['Segment Rangers']))
	rangers_scores[j] = scores


### Testing Classifiers ###
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_moons, make_circles, make_classification
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

names = ["Nearest Neighbors", "RBF SVM", 
         "Decision Tree", "Random Forest", "Neural Net", "AdaBoost",
         "Naive Bayes", "QDA", "Gaussian Process", "Linear SVM"]
         
classifiers = [
    KNeighborsClassifier(3),
    SVC(gamma=2, C=1),
    DecisionTreeClassifier(max_depth=5),
    RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1),
    MLPClassifier(alpha=1),
    AdaBoostClassifier(),
    GaussianNB(),
    QuadraticDiscriminantAnalysis(),
    GaussianProcessClassifier(1.0 * RBF(1.0), warm_start=True),
    SVC(kernel="linear", C=0.025)]
    
modeling_bball = datab.set_index('email').dropna(subset = ['Segment Knicks']).drop(['uuid'], axis = 1)
modeling_bball['Q33r2'].fillna(0, inplace = True)
#modeling_bball['Segment Knicks'] = modeling_bball['Segment Knicks'].replace({3:0,6:0}).replace({2:1,4:1,5:1})
for name, clf in zip(names, classifiers):
	scores = list()
	for i in range(10):
		train = modeling_bball.sample(frac = 0.8, random_state = i)
		test = modeling_bball.loc[~modeling_bball.index.isin(train.index)]
		clf.fit(train.drop(['Segment Knicks'], axis= 1), train['Segment Knicks'])
		scores.append(clf.score(test.drop(['Segment Knicks'], axis= 1), test['Segment Knicks']))
	print name, np.mean(scores).round(3)


'''
http://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html
http://scikit-learn.org/stable/modules/generated/sklearn.feature_selection.SelectPercentile.html#sklearn.feature_selection.SelectPercentile
https://www.analyticsvidhya.com/blog/2016/12/introduction-to-feature-selection-methods-with-an-example-or-how-to-select-the-right-variables/
'''