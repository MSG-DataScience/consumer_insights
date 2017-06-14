import pandas as pd
import sqlalchemy
import redshift_sqlalchemy
import statsmodels.api as sm
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import math

LA_data = pd.read_csv('/Volumes/NO_NAME/MSG/Live Analytics Match/MSG_Cust_20170602.txt', sep = '|')

LA_data = LA_data[LA_data['livea_match_cd'] == 'A']

acxiom_data = pd.read_csv('/Users/mcnamarp/Documents/ACXIOM_TRANS_DEMOS.txt')

acxiom_data.drop([u'NUMBER_OF_TRANSACTIONS', u'MAX_SALE_DT',
       u'MAX_EVENT_DT', u'MIN_SALE_DT', u'MIN_EVENT_DT', u'ACCT_CREATE_DT',
       u'NAME_FIRST', u'NAME_LAST', u'FULL_ADDR_LINE_1', u'FULL_ADDR_LINE_2',
       u'CITY', u'STATE_REGION', u'POSTAL_CD',u'DO_NOT_EMAIL_IND',u'DO_NOT_MAIL_IND', 
       u'HPHONE_DNC_IND', u'BPHONE_DNC_IND', u'MPHONE_DNC_IND', u'FA_CODE', 
       u'FA_DATE', u'FA_COMMENT', u'RELIGION', u'ETHNICITY'], axis = 1, inplace = True)

repeat_emails = acxiom_data['EMAIL'].value_counts()[acxiom_data['EMAIL'].value_counts() > 1].index
potential_dupes = acxiom_data[acxiom_data['EMAIL'].isin(repeat_emails)]
potential_dupes.dropna(subset = [u'DISCRET_INCOME_PERCENTILE', u'AGE', u'OCCUPATION', u'GENDER', u'EDUCATION', u'MARITAL_STATUS', u'PRESENCE_OF_CHILDREN', u'NETWORTH', u'INCOME_LOWRANGES'])