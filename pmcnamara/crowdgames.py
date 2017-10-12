import pandas as pd
import numpy as np

# CLEANING UP WIRESHARK PACKET DATA #
ips = ['10.202.9.101','162.242.150.89','54.205.101.85','23.253.58.227','176.34.241.253','52.85.101.216','52.85.101.116',
		'52.85.101.171','52.85.101.236','52.85.101.254','52.85.101.109','52.85.101.20','52.85.101.86','52.85.101.254','10.202.9.101',
		'54.192.48.232','54.192.48.72','54.192.48.6','54.192.48.28','54.192.48.106','54.192.48.32','54.192.48.16','54.192.48.225']

data_10r0 = pd.read_csv('/Users/mcnamarp/Downloads/Rangers 615-620 10-10.2017.csv').drop(['Info','Protocol'], axis = 1)
data_10r0['crowdgame'] = 0
data_10r0.ix[data_10r0['Destination'].isin(ips), 'crowdgame'] = 1
data_10r0 = data_10r0[(data_10r0['Time'] >= data_10r0[data_10r0['crowdgame'] == 1]['Time'].min()) & (data_10r0['Time'] <= data_10r0[data_10r0['crowdgame'] == 1]['Time'].max())] 
	
data_9k = pd.read_csv('/Users/mcnamarp/Downloads/Knicks FreeWiFi Game Capture.csv', index_col = 'No.').drop(['Info','Protocol'], axis = 1)
data_9k['crowdgame'] = 0
data_9k.ix[data_9k['Destination'].isin(ips), 'crowdgame'] = 1
data_9k = data_9k[(data_9k['Time'] >= data_9k[data_9k['crowdgame'] == 1]['Time'].min()) & (data_9k['Time'] <= data_9k[data_9k['crowdgame'] == 1]['Time'].max())]
played_game9k = data_9k.groupby('Source').max()['crowdgame'].reset_index()
data_9k = pd.merge(data_9k.drop(['crowdgame'], axis = 1), played_game9k, on = 'Source')
# REMOVING LIKELY EQUIPMENT IPs #
data_9k = data_9k[~data_9k.isin(set(data_10r0['Source']))]
pd.DataFrame(data_9k[['Source','crowdgame']].drop_duplicates()['crowdgame'].value_counts()).join(data_9k.groupby('crowdgame').sum()['Length']/1000000)

data_10r1 = pd.read_csv('/Users/mcnamarp/Downloads/Rangers745-750. 10-10-2017.csv').drop(['Info','Protocol'], axis = 1)
data_10r1['crowdgame'] = 0
data_10r1.ix[data_10r1['Destination'].isin(ips), 'crowdgame'] = 1
data_10r1 = data_10r1[(data_10r1['Time'] >= data_10r1[data_10r1['crowdgame'] == 1]['Time'].min()) & (data_10r1['Time'] <= data_10r1[data_10r1['crowdgame'] == 1]['Time'].max())]
played_game10r1 = data_10r1.groupby('Source').max()['crowdgame'].reset_index()
data_10r1 = pd.merge(data_10r1.drop(['crowdgame'], axis = 1), played_game10r1, on = 'Source')
# REMOVING LIKELY EQUIPMENT IPs #
data_10r1 = data_10r1[~data_10r1.isin(set(data_10r0['Source']))]
pd.DataFrame(data_10r1[['Source','crowdgame']].drop_duplicates()['crowdgame'].value_counts()).join(data_10r1.groupby('crowdgame').sum()['Length']/1000000)

data_10r2 = pd.read_csv('/Users/mcnamarp/Downloads/Rangers757-802 Oct 10th 2017.csv').drop(['Info','Protocol'], axis = 1)
data_10r2['crowdgame'] = 0
data_10r2.ix[data_10r2['Destination'].isin(ips), 'crowdgame'] = 1
data_10r2 = data_10r2[(data_10r2['Time'] >= data_10r2[data_10r2['crowdgame'] == 1]['Time'].min()) & (data_10r2['Time'] <= data_10r2[data_10r2['crowdgame'] == 1]['Time'].max())] 

'''
Time = moment from start of game a packet was transferred
Source = Device IP address
Destination = Web Address
Length = Packet Size 
Filter Destination IP by 176.34.241.253, filter the rest via list
Compare game vs. non-game data usage
'''

 # PRIME #
connections_9 = pd.read_csv('/Users/mcnamarp/Downloads/KnicksFreeWifiGame_20171010_132557_018.csv').drop(['Association Time','VLAN ID','Protocol'], axis = 1).drop_duplicates()
'''CAN'T REMOVE INTERNAL IPs'''

user_9 = pd.read_excel('/Users/mcnamarp/Downloads/Crowd.Game_USERDATA_Knicks_Oct-9-2017.xlsx', 'User Info', index_col='DISPLAY NAME', na_values=['NULL'])
ip_9 = pd.read_excel('/Users/mcnamarp/Downloads/Crowd.Game_USERDATA_Knicks_Oct-9-2017.xlsx', 'IP Info', index_col='USER ID')
ip_9['IP ADDRESS'] = ip_9['IP ADDRESS'].str[7:].astype(str)

# ANALYSIS #
data = pd.merge(data_9k, connections_9, left_on = 'Source', right_on = 'Client IP Address').drop_duplicates()
data = data[['Client MAC Address','crowdgame','Length','Session Duration','Map Location','Avg. Session Throughput (Kbps)','Time','AP Name']].drop_duplicates()
played_game = data.groupby('Client MAC Address').max()['crowdgame'].reset_index()
data = pd.merge(data.drop(['crowdgame'], axis = 1), played_game, on = 'Client MAC Address')

# average session throughput for gameplayers vs. non-gameplayers #
data[['Client MAC Address','Avg. Session Throughput (Kbps)','crowdgame']].drop_duplicates().groupby('crowdgame').mean().astype(int)
data[['Client MAC Address','Avg. Session Throughput (Kbps)','crowdgame']].drop_duplicates().groupby('crowdgame').median().astype(int)

# average packet size for gameplayers vs. non-gameplayers #
data[['Client MAC Address','Length','crowdgame']].groupby('crowdgame').mean()

# average per-client data usage in time window (MBs) #
data[['Client MAC Address','Length','crowdgame']].groupby(['Client MAC Address','crowdgame']).sum().reset_index().groupby('crowdgame').mean()/1000000

# APs analysis #
data[data['crowdgame'] == 1][['Client MAC Address','Avg. Session Throughput (Kbps)','AP Name']].drop_duplicates().groupby(['AP Name']).count()['Client MAC Address'] # the most connections through any single AP was 5 (W4P-SKY-9A298)#
data[data['crowdgame'] == 1][['Client MAC Address','Length','AP Name']].groupby(['AP Name']).sum()['Length']/1000000
x = pd.DataFrame(data[data['crowdgame'] == 1][['Client MAC Address','Avg. Session Throughput (Kbps)','AP Name']].drop_duplicates().groupby(['AP Name']).count()['Client MAC Address']).join(np.round(data[data['crowdgame'] == 1][['Client MAC Address','Length','AP Name']].groupby(['AP Name']).sum()['Length']/1000000, 1)).rename(columns = {'Length':'Data Used (MBs)'})
y = pd.DataFrame(data[data['crowdgame'] == 0][['Client MAC Address','Avg. Session Throughput (Kbps)','AP Name']].drop_duplicates().groupby(['AP Name']).count()['Client MAC Address']).join(np.round(data[data['crowdgame'] == 0][['Client MAC Address','Length','AP Name']].groupby(['AP Name']).sum()['Length']/1000000, 1)).rename(columns = {'Length':'Data Used (MBs)'})

# additional data usage #
total_data = x.sum()['Data Used (MBs)'] + y.sum()['Data Used (MBs)']
avg_data_non_player = y.sum()['Data Used (MBs)']/y.sum()['Client MAC Address']
counterfactual_total_data = y.sum()['Data Used (MBs)'] + (x.sum()['Client MAC Address']*avg_data_non_player)
total_data - counterfactual_total_data
total_data/counterfactual_total_data