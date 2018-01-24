import pandas as pd
import glob
import os

# list of all files in directory #
files = glob.glob("/Users/mcnamarp/Downloads/billy_files/*.xlsx")
files = [file for file in files if '~' not in file]

for file in files:
	# identify date of event #
	data = pd.read_excel(file)
	date_line = data.ix[0][0].split('START DATE')[1].split('END DATE')[0].strip()
	event_date = pd.to_datetime(date_line).date()
#
	# remove all non-vendor data #
	first_column = data.columns[0]
	start_row = data[data[data.columns[0]] == 'VENDOR NAME'].index[0]
	data = data[start_row:] 
#
	# drop unnecessary rows #
	drop_rows = ['Total ','Subtotal','Group: Alcohol','Group: Beverage','Group: ','Group: Food']
	data = data[~data[first_column].isin(drop_rows)]
	data.dropna(subset = [first_column], inplace = True)
#
	# drop empty/unnecessary columns #
	keeps = []
	for i in data.columns:
		if len(data[i].dropna()) > 100:
			keeps.extend([i])
#
	data = data[keeps]
#
	# map stand names to appropriate records #
	data.index = range(len(data))
	new_stand_rows = pd.DataFrame(data[data[first_column] == 'VENDOR NAME'].index, columns = ['record'])
	new_stand_rows.ix[max(new_stand_rows.index)+1] = len(data)

	stand_ids = []
	for i in new_stand_rows.index[:-1]:
		diff = int(new_stand_rows.ix[i+1] - new_stand_rows.ix[i])
		stand_id = data.ix[int(new_stand_rows.ix[i])]['Unnamed: 4']
		stand_ids.extend(diff*[stand_id])
#
	data['Stand ID'] = stand_ids
#
	# rename columns #
	columns = ['Product','Price','Quantity','Gross Sales','Tax Inclusive','Tax Exclusive','Discount','Commission','Net Sales','Net Revenue','Product ID','Stand ID']
	data.columns = columns
	data['event_date'] = event_date
#
	# remove vendor name rows #
	data = data[data['Product'] != 'VENDOR NAME']
	data.to_csv(file[:-4]+'csv', index = False)
	print len(data)

# combine normalized files into single file #
files = glob.glob("/Users/mcnamarp/Downloads/billy_files/*.csv")
files = [x for x in files if 'full.csv' not in x]
data = pd.DataFrame()
for i in files:
	temp_frame = pd.read_csv(i)
	data = data.append(temp_frame)

data.drop_duplicates().to_csv('/Users/mcnamarp/Downloads/billy_files/full.csv', index = False)