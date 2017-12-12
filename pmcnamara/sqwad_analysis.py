import pandas as pd
import numpy as np
import re

ips = ['184.168.221.5']

data_1 = pd.read_csv('/Users/mcnamarp/Downloads/OneDrive_2_11-29-2017/112017_758.csv').drop(['No.'], axis = 1)
data_1['sqwad'] = 0
data_1.ix[data_1['Destination'].isin(ips), 'sqwad'] = 1
data_1 = data_1[(data_1['Time'] >= data_1[data_1['sqwad'] == 1]['Time'].min()) & (data_1['Time'] <= data_1[data_1['sqwad'] == 1]['Time'].max())]
data_1['time_short'] = data_1['Time'].astype(str).str(1:7)
	
data_2 = pd.read_csv('/Users/mcnamarp/Downloads/OneDrive_2_11-29-2017/112017_750.csv').drop(['No.','Info','Protocol'], axis = 1)
data_2['sqwad'] = 0
data_2.ix[data_2['Destination'].isin(ips), 'sqwad'] = 1
data_2 = data_2[(data_2['Time'] >= data_2[data_2['sqwad'] == 1]['Time'].min()) & (data_2['Time'] <= data_2[data_2['sqwad'] == 1]['Time'].max())] 
# REMOVING LIKELY EQUIPMENT IPs #
data_2 = data_2[~data_2.isin(set(data_1['Source']))]
pd.DataFrame(data_2[['Source','sqwad']].drop_duplicates()['sqwad'].value_counts()).join(data_2.groupby('sqwad').sum()['Length']/1000000)

# PRIME #
connections_9 = pd.read_csv('/Users/mcnamarp/Downloads/KnicksFreeWifiGame_20171010_132557_018.csv').drop(['Association Time','VLAN ID','Protocol'], axis = 1).drop_duplicates()
'''CAN'T REMOVE INTERNAL IPs'''
connections_9[['Client MAC Address','Vendor']].drop_duplicates()['Vendor'].value_counts()

user_9 = pd.read_excel('/Users/mcnamarp/Downloads/Crowd.Game_USERDATA_Knicks_Oct-9-2017.xlsx', 'User Info', index_col='DISPLAY NAME', na_values=['NULL'])
ip_9 = pd.read_excel('/Users/mcnamarp/Downloads/Crowd.Game_USERDATA_Knicks_Oct-9-2017.xlsx', 'IP Info', index_col='USER ID')
ip_9['IP ADDRESS'] = ip_9['IP ADDRESS'].str[7:].astype(str)
ip_9['USER AGENT'].str.contains('Android')

# ANALYSIS #
data = pd.merge(data_2, connections_9, left_on = 'Source', right_on = 'Client IP Address').drop_duplicates()
data = data[['Client MAC Address','sqwad','Length','Session Duration','Map Location','Avg. Session Throughput (Kbps)','Time','AP Name','Vendor']].drop_duplicates()
played_game = data.groupby('Client MAC Address').max()['sqwad'].reset_index()
data = pd.merge(data.drop(['sqwad'], axis = 1), played_game, on = 'Client MAC Address')

# average session throughput for gameplayers vs. non-gameplayers #
data[['Client MAC Address','Avg. Session Throughput (Kbps)','sqwad']].drop_duplicates().groupby('sqwad').mean().astype(int)
data[['Client MAC Address','Avg. Session Throughput (Kbps)','sqwad']].drop_duplicates().groupby('sqwad').median().astype(int)

# average packet size for gameplayers vs. non-gameplayers #
data[['Client MAC Address','Length','sqwad']].groupby('sqwad').mean()

# average per-client data usage in time window (MBs) #
data[['Client MAC Address','Length','sqwad']].groupby(['Client MAC Address','sqwad']).sum().reset_index().groupby('sqwad').mean()/1000000

# APs analysis #
data[data['sqwad'] == 1][['Client MAC Address','Avg. Session Throughput (Kbps)','AP Name']].drop_duplicates().groupby(['AP Name']).count()['Client MAC Address'] # the most connections through any single AP was 5 (W4P-SKY-9A298)#
data[data['sqwad'] == 1][['Client MAC Address','Length','AP Name']].groupby(['AP Name']).sum()['Length']/1000000
x = pd.DataFrame(data[data['sqwad'] == 1][['Client MAC Address','Avg. Session Throughput (Kbps)','AP Name']].drop_duplicates().groupby(['AP Name']).count()['Client MAC Address']).join(np.round(data[data['sqwad'] == 1][['Client MAC Address','Length','AP Name']].groupby(['AP Name']).sum()['Length']/1000000, 1)).rename(columns = {'Length':'Data Used (MBs)'})
y = pd.DataFrame(data[data['sqwad'] == 0][['Client MAC Address','Avg. Session Throughput (Kbps)','AP Name']].drop_duplicates().groupby(['AP Name']).count()['Client MAC Address']).join(np.round(data[data['sqwad'] == 0][['Client MAC Address','Length','AP Name']].groupby(['AP Name']).sum()['Length']/1000000, 1)).rename(columns = {'Length':'Data Used (MBs)'})

# additional data usage #
total_data = x.sum()['Data Used (MBs)'] + y.sum()['Data Used (MBs)']
avg_data_non_player = y.sum()['Data Used (MBs)']/y.sum()['Client MAC Address']
counterfactual_total_data = y.sum()['Data Used (MBs)'] + (x.sum()['Client MAC Address']*avg_data_non_player)
total_data - counterfactual_total_data
total_data/counterfactual_total_data

# importing first access timestamp #
first_access = pd.read_csv('/Users/mcnamarp/Downloads/first-present-2017-10-09.csv')
first_access['First Present'] = pd.to_datetime(first_access['First Present']).dt.time
first_access['First Present'] = first_access['First Present'].apply(lambda x: x.replace(microsecond=0))
filter = first_access['First Present'].astype(str).str[:2]
filter = filter[filter.isin(['18','19'])].index
first_access = first_access[first_access.index.isin(filter)]
filter = first_access[(first_access['First Present'].astype(str).str[3:5].astype(int) > 6) & (first_access['First Present'].astype(str).str[:2] == '19')].index
first_access = first_access[~first_access.index.isin(filter)]

time_data = pd.merge((data.groupby(['Client MAC Address','sqwad']).sum()['Length']/1000000).reset_index(), first_access, left_on = 'Client MAC Address', right_on = 'MAC').drop(['Client MAC Address'], axis = 1)
time_data['mins'] = (time_data['First Present'].astype(str).str[:2].astype(int) - 18)*60 + (time_data['First Present'].astype(str).str[3:5].astype(int))
time_data['mins'] = (time_data['mins'] - 66)*-1

first_access['mins'] = (first_access['First Present'].astype(str).str[:2].astype(int) - 18)*60 + (first_access['First Present'].astype(str).str[3:5].astype(int))
first_access = first_access[first_access['First Present'].astype(str).str[:2].astype(int) > 16]
#first_access['mins'] = (first_access['mins'] - 66)*-1

# AP Filtering #
AP_locations = pd.read_excel('/Users/mcnamarp/Downloads/Arena AP Inventory.xlsx', 'Arena_Access_Points_20171020_08')
bowl_locations = ['Arena Bowl','Scoreboard','ScoreBoard Top Row','Scoreboard Top Row','Underside bridge','Scoreboard IN ring','Scoreboard Out ring','A tower Bridge','C tower Bridge','D tower Bridge','B tower Bridge','A tower Bullnose','B tower Bullnose','C tower Bullnose','D tower Bullnose']
AP_locations.ix[AP_locations['Location'].isin(bowl_locations), 'bowl'] = 1
AP_locations.fillna(0, inplace = True)
data = pd.merge(data, AP_locations, on = ['AP Name','Map Location'])

devices_connected_bowl = list(set(data[data['Location'].isin(bowl_locations)]['Client MAC Address']))
data[data['Client MAC Address'].isin(devices_connected_bowl)][['Client MAC Address','sqwad']].drop_duplicates().groupby('sqwad').count()
time_data[time_data['MAC'].isin(devices_connected_bowl)].groupby('sqwad').mean()