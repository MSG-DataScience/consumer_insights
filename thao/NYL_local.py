import time
import requests
import base64
import json
import pprint
import pandas as pd
import numpy as np
import datetime

app_token = "a7e64ebe-0998-3dcd-afa8-88b5594440c9"
consumer_key = "ebxARAhZKo2_4abI3vroPlkUOTEa"
consumer_secret = "TuefeH8yl8rhzjXnBg1spQD4qmsa"
stubhub_username = "haotianyu1104@gmail.com"
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


eventid = '9742147'
listing_df=pd.DataFrame()
for i in np.arange(0,1000,10):
    data = {'eventid':eventid,'rows':1000,'pricemin':i,'pricemax':i+10}
    inventory = requests.get(inventory_url, headers=headers, params=data)
    inv = inventory.json()
    if 'listing' in inv :
        listing_df = listing_df.append(pd.DataFrame(inv['listing']))
    else:
        continue
t=str(time.ctime())
z=t.replace(':','')
z=z.replace(' ','')
    
listing_df[z+'amount'] = listing_df.apply(lambda x: x['currentPrice']['amount'], axis=1)
    
    
listing_df.to_csv('tickets_listing.csv', index=False)
    
    
info_url = 'https://api.stubhub.com/catalog/events/v2/' + eventid
info = requests.get(info_url, headers=headers)
    
    
info_dict = info.json()
event_date = datetime.datetime.strptime(info_dict['eventDateLocal'][:10], '%Y-%m-%d')
    
event_name = info_dict['title']
event_date = info_dict['eventDateLocal'][:10]
venue = info_dict['venue']['name']
    
    
    
listing_df['eventName'] = event_name
listing_df['eventDate'] = event_date
listing_df['venue'] = venue
    


my_col = ['currentPrice','deliveryMethodList', 'deliveryTypeList', 'dirtyTicketInd', 'faceValue',
               'listingAttributeList', 'listingId','listingPrice','sectionId','sellerOwnInd','sellerSectionName','splitOption','splitVector','ticketSplit','zoneId','zoneName']
    
    
   
last_df=pd.read_csv("C:/project/stubhub scraping/NYL.csv")
listing_df.drop(my_col,axis=1).to_csv("now.csv", index=False)
now_df=pd.read_csv("C:/project/stubhub scraping/now.csv")

new_df=pd.merge(last_df,now_df,how="outer")    

new_df.to_csv("NYL.csv", index=False)
