import pandas as pd
import json
import urllib2

xls = pd.ExcelFile('/Users/mcnamarp/Downloads/msg_append_sample_acxiom_fraday_comp.xlsx')
names = xls.parse('Gender Data').set_index('msg_id')
names['predicted_gender_acxiom'] = None
names['accuracy_acxiom'] = None
myKey = 'rXCcWNTUjHMyFUJmQp'

for i in names.dropna(subset = ['acxiom_person_first_name']).index:
	temp_name = names.loc[i,'acxiom_person_first_name']
	temp_results = json.load(urllib2.urlopen("https://gender-api.com/get?key=" + myKey + "&name=" + temp_name))
	names.loc[i,'predicted_gender_acxiom'] = temp_results['gender']
	names.loc[i,'accuracy_acxiom'] = temp_results['accuracy']
	
names.to_csv('/Users/mcnamarp/Downloads/msg_append_sample_acxiom_fraday_comp_name_predictions.csv')