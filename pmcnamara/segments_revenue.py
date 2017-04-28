import pandas as pd
import sqlalchemy
import redshift_sqlalchemy

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
rangers = pd.merge(rangers, segment_labels_knicks, left_on = 'Segment Rangers', right_index = True).drop('Segment Rangers', axis = 1)
segments = knicks.append(rangers, ignore_index = True)

# combining survey response data #
sth = pd.read_excel('/Users/mcnamarp/Downloads/fac17002 (10)/STH/fac17002.xlsx', sheetname = 'A1')[['uuid','source','Sample','vspt','Q1_Gender','Q1_Age']]
indy = pd.read_excel('/Users/mcnamarp/Downloads/fac17002 (10)/Individual_Game_Purchasers/fac17002.xlsx', sheetname = 'A1')[['uuid','source','Sample','vspt','Q1_Gender','Q1_Age']]
panel = pd.read_excel('/Users/mcnamarp/Downloads/fac17002 (10)/Panel/fac17002.xlsx', sheetname = 'A1')[['uuid','source','Sample','vspt','Q1_Gender','Q1_Age']]
survey_data = sth.append(panel, ignore_index = True).append(indy, ignore_index = True)
survey_data.replace({'Sample':{1:'Panel',2:'STH',3:'Indy'}}, inplace = True)
survey_data = pd.merge(survey_data, segments, on = 'uuid', how = 'left').drop('uuid',axis = 1)

id_mapping1 = pd.read_excel('/Users/mcnamarp/Downloads/Survey update 4.xlsx', sheetname = 'Master List')[['uid','acct_id']]
id_mapping2 = pd.read_excel('/Users/mcnamarp/Downloads/Survey update 4.xlsx', sheetname = 'Master List (2)')[['uid','acct_id']]
id_mapping = id_mapping1.append(id_mapping2)
id_mapping['acct_id'] = id_mapping['acct_id'].astype(str)

sth_data = pd.merge(id_mapping, survey_data, left_on = 'uid', right_on = 'source').drop('uid', axis = 1)

engine = sqlalchemy.create_engine("redshift+psycopg2://mcnamarp:Welcome2859!@msgbiadb-prod.cqp6htpq4zp6.us-east-1.rds.amazonaws.com:5432/msgbiadb")
revenue_query = '''
SELECT A.tm_acct_id::TEXT, description, cost, tickets FROM (
SELECT tm_acct_id, ticket_product_description AS description, SUM(tickets_total_revenue) AS cost, tm_season_name
FROM ads_main.t_ticket_sales_event_seat A
WHERE tm_season_name = '2016-17 New York Knicks' AND tm_acct_id NOT IN ('-1','-2')
GROUP BY tm_acct_id, ticket_product_description, tm_season_name) A
JOIN (SELECT tm_acct_id, ticket_product_description, COUNT(*) AS tickets FROM (SELECT DISTINCT tm_acct_id, ticket_product_description, tm_event_name, tm_event_date, tm_section_name, tm_row_name, tm_seat_num FROM ads_main.t_ticket_sales_event_seat) S GROUP BY tm_acct_id, ticket_product_description) B ON A.tm_acct_id = B.tm_acct_id AND A.description = B.ticket_product_description;
'''
tm_revenue = pd.read_sql(revenue_query, engine)
tm_revenue = tm_revenue[tm_revenue['cost'] != 0]
#labels = tm_revenue.groupby('tm_acct_id').max()[['description','description_level']].reset_index().rename(columns = {'description':'category'}).drop(['description_level'], axis = 1)
#tm_revenue = pd.merge(tm_revenue, labels, on = ['tm_acct_id'], how = 'left').drop(['description_level'], axis = 1)

data = pd.merge(tm_revenue, sth_data, left_on = 'tm_acct_id', right_on = 'acct_id', how = 'left')
data['avg_ticket'] = data['cost'] / data['tickets']
data['segment'].fillna('unknown', inplace = True)
pd.pivot_table(data, values = 'avg_ticket', index = 'description', columns='segment').round()