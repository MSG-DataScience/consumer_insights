# -*- coding: utf-8 -*-
"""
Created on Fri Jun  9 15:06:52 2017

@author: haot
"""
import time
import requests
import base64
import json
import pprint
import pandas as pd
import numpy as np
import datetime
import os


columns=pd.read_csv('C:/Users/haot/Desktop/columns.csv')
primary=pd.read_csv('C:/Users/haot/Downloads/ticketsale.csv')
secondary=pd.read_csv('C:/Users/haot/Desktop/RangersAWS.csv')
primary.columns=(columns.ix[:,0])
secondary['sectionname']=secondary['sectionname'].str[-3: ]
secondary['sectionname']=secondary['sectionname'].replace(' WC','VP4WC')
secondary['sectionname']=secondary['sectionname'].replace('e 8','8')

secondary = secondary[pd.notnull(secondary['seatnumbers'])]
secondary=secondary[secondary.seatnumbers.str.contains("General Admission") == False]
lst_col = 'seatnumbers'
x = secondary.assign(**{lst_col:secondary[lst_col].str.split(',')})
secondary=pd.DataFrame({col:np.repeat(x[col].values,x[lst_col].str.len())for col in x.columns.difference([lst_col]) }).assign(**{lst_col:np.concatenate(x[lst_col].values)})[x.columns.tolist()] 
col_list=['tm_acct_id','tm_section_name','tm_row_name','tm_seat_num']  
df=primary[col_list]
df.columns=['id','sectionname','row','seatnumbers']
secondary=secondary[secondary['timestamp'].str.contains("2017-07-10")]
df=df.drop_duplicates()
secondary=secondary.drop(['currentprice','eventdate','eventname','listingid','listingprice','quantity','timestamp'],axis=1)
secondary=secondary.drop_duplicates()
df['id']=df['id'].apply(str)
df=df.groupby(['sectionname', 'seatnumbers','row'])['id'].apply(' '.join).reset_index()
df['len']=df['id'].str.len()
df['len']=np.where(df['len']>10,0,1)
df=df.drop('id',axis=1)
secondary=secondary[['sectionname','row','seatnumbers']]
df=df[['sectionname','row','seatnumbers','len']]
secondary['row']=secondary['row'].apply(str)
secondary['seatnumbers']=secondary['seatnumbers'].apply(str)
secondary['sectionname']=secondary['sectionname'].apply(str)
df['row']=df['row'].apply(str)
df['seatnumbers']=df['seatnumbers'].apply(str)
df['sectionname']=df['sectionname'].apply(str)
