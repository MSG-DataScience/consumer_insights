import pandas as pd
import sqlalchemy
import time
import requests
import base64
import json
import pprint
import pandas as pd
import numpy as np
import datetime
import os

#connect to stubhub API
engine = sqlalchemy.create_engine("mysql+mysqldb://Rangers:19931104@rangers.cbtjzutpd1ig.us-east-2.rds.amazonaws.com:3306/Rangers")

app_token = "e7449bda-b8b1-3345-a9f2-d21fc9f6a2e7"
consumer_key = "NHx7zundSqf2YThqfbdimE_jzqUa"
consumer_secret = "CPJmFDZgbXoeX1Bjg7lzF78OF9ca"
stubhub_username = "Tianyu.Hao@msg.com"
stubhub_password = "Dududai1104"
combo = consumer_key + ':' + consumer_secret
basic_authorization_token = base64.b64encode(combo.encode('utf-8'))
headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Authorization':'Basic '+basic_authorization_token.decode('utf-8'),}
body = {
        'grant_type':'password',
        'username':stubhub_username,
        'password':stubhub_password,
        'scope':'PRODUCTION'}
url = 'https://api.stubhub.com/login'
r = requests.post(url, headers=headers, data=body)
token_respoonse = r.json()
access_token = token_respoonse['access_token']
user_GUID = r.headers['X-StubHub-User-GUID']
inventory_url = 'https://api.stubhub.com/search/inventory/v2'

headers['Authorization'] = 'Bearer ' + access_token
headers['Accept'] = 'application/json'
headers['Accept-Encoding'] = 'application/json'
#search for all Rangers games' id
id_df=pd.DataFrame()
search_url = 'https://api.stubhub.com/search/catalog/events/v3?name=rangers&parking=false&city=New York&rows=100&status=active'
search = requests.get(search_url, headers=headers)
search_dict= search.json()
id_df=id_df.append(pd.DataFrame(search_dict['events']))

#get tickets information from each game
for id in id_df['id']:    
    eventid = '%s'%id
    listing_df=pd.DataFrame()

    for i in np.arange(0,4,1):
        data = {'eventid':eventid,'start':250*i,'rows':250}
        inventory = requests.get(inventory_url, headers=headers, params=data)
        inv = inventory.json()
        time.sleep(7)
        if 'listing' in inv :
            listing_df = listing_df.append(pd.DataFrame(inv['listing']))
        else:
            continue
    t=datetime.datetime.now()
    listing_df['listingPrice'] = listing_df.apply(lambda x: x['listingPrice']['amount'], axis=1)
    listing_df['currentPrice'] = listing_df.apply(lambda x: x['currentPrice']['amount'], axis=1)
    listing_df['eventName'] = id_df.loc[id_df['id']==int(eventid),'name'].iloc[0]
    listing_df['eventDate'] = id_df.loc[id_df['id']==int(eventid),'eventDateLocal'].iloc[0][:10] 
    listing_df['timestamp'] = t
    if 'listingAttributeCategoryList' in listing_df and 'faceValue' in listing_df:
        my_col = ['isGA','deliveryMethodList', 'deliveryTypeList', 'dirtyTicketInd','faceValue','listingAttributeCategoryList','score',
                       'listingAttributeList','sectionId','sellerOwnInd','sellerSectionName','splitOption','splitVector','ticketSplit','zoneId','zoneName']
    elif 'listingAttributeCategoryList' in listing_df:
        my_col = ['isGA','deliveryMethodList', 'deliveryTypeList', 'dirtyTicketInd','listingAttributeCategoryList', 'score',
                       'listingAttributeList','sectionId','sellerOwnInd','sellerSectionName','splitOption','splitVector','ticketSplit','zoneId','zoneName']
    elif 'faceValue' in listing_df:
        my_col = ['isGA','deliveryMethodList', 'deliveryTypeList', 'dirtyTicketInd','faceValue', 'score',
                       'listingAttributeList','sectionId','sellerOwnInd','sellerSectionName','splitOption','splitVector','ticketSplit','zoneId','zoneName']
    else:
        my_col = ['isGA','deliveryMethodList', 'deliveryTypeList', 'dirtyTicketInd', 'score',
                       'listingAttributeList','sectionId','sellerOwnInd','sellerSectionName','splitOption','splitVector','ticketSplit','zoneId','zoneName']        
    result=listing_df.drop(my_col,axis=1)
# store data into the cloud database
    result.to_sql(name='Rangers', con=engine, if_exists = 'append', index=False)