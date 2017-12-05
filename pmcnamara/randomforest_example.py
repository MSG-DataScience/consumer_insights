import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score

drops = ['D0','D1','D2','D3','D4','D5','D6','D7','D8','D2Weeks','DPlanners','DEarly']
reg_data = reg_data.drop(drops, axis = 1)
# remove weird error date #
weird_error_date = reg_data[(reg_data['sale_date'] == datetime.date(2017,11,16)) & (reg_data['tm_event_name'] == 'ESN1122E')].index[0]
reg_data = reg_data[reg_data.index != weird_error_date]
#
#rf_temp = reg_data.drop(list(impressions.drop(['sale_date'], axis = 1).columns), axis = 1).reset_index()
rf_temp = reg_data
model_date_range = pd.date_range(datetime.date(2017,10,13), datetime.datetime.now().date() - datetime.timedelta(days=1)).date
rf_results = pd.DataFrame(index = model_date_range, columns = range(0,8))
predictions = pd.DataFrame()
features_results = {}
for date in model_date_range:
	features_results[date] = {}
	for days in rf_results.columns:
		rf_train = rf_temp[(rf_temp['sale_date'] < date) | (rf_temp['days_out'] > days)].set_index(['tm_event_name','sale_date'])
		X_train = rf_train.drop(['tickets_sold'], axis = 1)
		y_train = rf_train['tickets_sold']
		rf_test = rf_temp[(rf_temp['sale_date'] >= date) & (rf_temp['days_out'] <= days)].set_index(['tm_event_name','sale_date'])
		X_test = rf_test.drop(['tickets_sold'], axis = 1)
		y_test = rf_test['tickets_sold']
#
		rf = RandomForestRegressor(n_estimators=100, random_state=0, n_jobs = -1, oob_score = True)
		rf.fit(X_train, y_train)
		features_results[date][days] = rf.feature_importances_
#
		predicted_train = rf.predict(X_train)
		predicted_test = rf.predict(X_test)
		X_test['predicted'] = predicted_test
		X_test['tickets_sold'] = y_test
		temp = X_test[['days_out','predicted','tickets_sold']].reset_index()
		temp['prediction_date'] = date
		temp['days_predicted'] = days
		predictions = predictions.append(temp)
		test_score = r2_score(y_test, predicted_test)
		rf_results.loc[date, days] = np.round(test_score, 3)
#
rf_results.to_csv('/Users/mcnamarp/Documents/consumer_insights/pmcnamara/rf_predictions_marketing.csv')
rf_results.mean().mean()

predictions = predictions[predictions['sale_date'] != datetime.datetime.now().date()]
predictions['error'] = predictions['predicted'] - predictions['tickets_sold']
predictions['abs_error'] = abs(predictions['predicted'] - predictions['tickets_sold'])
predictions_range = pd.date_range(datetime.date(2017,11,4), datetime.datetime.now().date() - datetime.timedelta(days=1)).date
predictions_ = predictions[predictions['prediction_date'].isin(predictions_range)].reset_index().drop(['index'], axis = 1)
predictions_.groupby('days_out').mean()
predictions_.groupby('prediction_date').mean()
predictions_.to_csv('/Users/mcnamarp/Documents/consumer_insights/pmcnamara/predictions_marketing.csv', index = False)

# analyzing impacts of marketing over time #
features_impacts = pd.DataFrame(columns = X_train.columns, index = features_results.keys())
for i in features_results:
	features_impacts.ix[i] = pd.DataFrame(features_results[i]).T.mean().values

(pred_mark.groupby(['days_predicted','days_out']).mean()['abs_error']/pred_mark.groupby('days_out').mean()['tickets_sold']).reset_index().pivot(index = 'days_predicted', columns = 'days_out').to_csv('blah.csv')
	'''
	from sklearn.decomposition import PCA
	pca = PCA()
	pca.fit(X_train)
	cpts = pd.DataFrame(pca.transform(X_train))
	x_axis = np.arange(1, pca.n_components_+1)
	pca_scaled = PCA()
	pca_scaled.fit(X_train_scaled)
	cpts_scaled = pd.DataFrame(pca.transform(X_train_scaled))
	#
		scaler = StandardScaler().fit(X_train)
		X_train_scaled = pd.DataFrame(scaler.transform(X_train), index=X_train.index.values, columns=X_train.columns.values)
		X_test_scaled = pd.DataFrame(scaler.transform(X_test), index=X_test.index.values, columns=X_test.columns.values)
	'''
	#print 'Out-of-bag R-2 score estimate:'+str(np.round(rf.oob_score_, 3))
	#print 'Test data R-2 score:'+str(np.round(test_score, 3))