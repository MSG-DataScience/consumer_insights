import pandas as pd
import numpy as np
import datetime

seen_macs_1017 = pd.read_csv('/Users/mcnamarp/Downloads/seen_msg_macs.csv')
seen_macs_1017['MAC'] = [i.upper() for i in seen_macs_1017['MAC']]

sent_data = pd.read_csv('/Users/mcnamarp/Downloads/mac_addresses_ssi.csv')
sent_data['MAC'] = [i.upper() for i in sent_data['MAC']]

matched_macs = pd.merge(sent_data, seen_macs_1017, on = 'MAC')

wifi_data = pd.read_csv('/Users/mcnamarp/Downloads/wifi_data.csv')
wifi_data.drop(['Unnamed: 0','Locale','Time Zone','Min Age','Max Age','Facebook ID','First Name','Last Name','User Agent','Email Verified','Gender'], axis = 1, inplace = True)
wifi_data['Date'] = pd.to_datetime(wifi_data['Date']).dt.date
wifi_data[wifi_data['Date'] < datetime.date(2017, 10, 17)]

wifi_data['ssi'] = 0
wifi_data.ix[wifi_data['MAC'].isin(seen_macs_1017['MAC']), 'ssi'] = 1

