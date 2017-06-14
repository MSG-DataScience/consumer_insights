import pandas as pd
import sqlalchemy
import redshift_sqlalchemy
import statsmodels.api as sm
import numpy as np
from datetime import datetime.datetime
import matplotlib.pyplot as plt
import math

# DATA IMPORT #
cols = [u'INDIVIDUAL_ID',u'NUMBER_OF_TRANSACTIONS', u'MAX_SALE_DT', u'MAX_EVENT_DT',
       u'MIN_SALE_DT', u'MIN_EVENT_DT', u'ACCT_CREATE_DT', u'NAME_FIRST',
       u'NAME_LAST', u'FULL_ADDR_LINE_1', u'FULL_ADDR_LINE_2', u'CITY',
       u'STATE_REGION', u'POSTAL_CD', u'EMAIL', u'DO_NOT_EMAIL_IND',
       u'DO_NOT_MAIL_IND', u'HPHONE_DNC_IND', u'BPHONE_DNC_IND',
       u'MPHONE_DNC_IND', u'FA_CODE', u'FA_DATE', u'FA_COMMENT',
       u'DISCRET_INCOME_PERCENTILE', u'AGE', u'OCCUPATION', u'GENDER',
       u'RELIGION', u'ETHNICITY', u'EDUCATION', u'MARITAL_STATUS',
       u'PRESENCE_OF_CHILDREN', u'NETWORTH', u'INCOME_LOWRANGES']
drop_cols = ['MAX_EVENT_DT','MIN_EVENT_DT','FA_CODE','FA_DATE'] 
keep_cols = list(set(cols) - set(drop_cols))
data = pd.read_csv('/Users/mcnamarp/Documents/ACXIOM_TRANS_DEMOS.txt', usecols = keep_cols, index_col = 'INDIVIDUAL_ID')
data[['HPHONE_DNC_IND','BPHONE_DNC_IND','MPHONE_DNC_IND']] = data[['HPHONE_DNC_IND','BPHONE_DNC_IND','MPHONE_DNC_IND']].fillna(0).astype(int)

# DATE PARSING #
def fast_date(s):
    """
    This is an extremely fast approach to datetime parsing.
    For large data, the same dates are often repeated. Rather than
    re-parse these, we store all unique dates, parse them, and
    use a lookup to convert all dates.
    """
    dates = {date:pd.to_datetime(date).date() for date in s.unique()}
    return s.map(dates)

data['MAX_SALE_DT'] = fast_date(data['MAX_SALE_DT'])
data['MIN_SALE_DT'] = fast_date(data['MIN_SALE_DT'])
data['ACCT_CREATE_DT'] = fast_date(data['ACCT_CREATE_DT'])

# NUMBER OF TRANSACTIONS #
data['NUMBER_OF_TRANSACTIONS'].fillna(0, inplace = True)

# DAYS BETWEEEN TODAY AND MOST RECENT PURCHASE #
data['years_since_purchase'] = (datetime.date.today() - data['MAX_SALE_DT']).dt.years
data['years_since_purchase'].hist(bins = 18)
pd.DataFrame((np.ceil(data['years_since_purchase'] / 1) * 1).value_counts()/len(data['years_since_purchase'].dropna())).sort_index()
