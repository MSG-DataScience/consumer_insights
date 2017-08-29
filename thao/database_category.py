# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 13:55:32 2017

@author: haot
"""

import pandas as pd
import numpy as np
#read all the distinct food beverage and merchandise
distinct=pd.read_csv("c:/project/fnb label/how many food sold.csv")


#read all the distinct item for each category
beer=pd.read_csv("c:/project/fnb label/beer.csv")
wine=pd.read_csv("c:/project/fnb label/wine.csv",encoding = "ISO-8859-1")
soda=pd.read_csv("c:/project/fnb label/soda.csv")
water=pd.read_csv("c:/project/fnb label/water.csv")
mer=pd.read_csv("c:/project/fnb label/mer.csv")
# parse beverage by ounce
for i in np.arange(1,100,1):
    distinct['volume'][distinct['product'].str.contains('%d oz' % i,case=False)]='%d oz' % i
    distinct['volume'][distinct['product'].str.contains('%doz' % i,case=False)]='%d oz' % i

 
for i in [ '%.1f' % elem for elem in np.arange(1,100,0.1) ]:
        distinct['volume'][distinct['product'].str.contains('%s oz' % i,case=False)]='%s oz' % i
        distinct['volume'][distinct['product'].str.contains('%soz' % i,case=False)]='%s oz' % i

for i in [ '%.2f' % elem for elem in np.arange(1,100,0.01) ]:
        distinct['volume'][distinct['product'].str.contains('%s oz' % i,case=False)]='%s oz' % i
        distinct['volume'][distinct['product'].str.contains('%soz' % i,case=False)]='%s oz' % i
# parse food by pieces
for i in np.arange(1,100,1):
    distinct['volume'][distinct['product'].str.contains('%d pcs' % i,case=False)]='%d oz' % i
    distinct['volume'][distinct['product'].str.contains('%dpc' % i,case=False)]='%d oz' % i
    


distinct['volume'][distinct['product'].str.contains('187ML',case=False)]='187 ml'        
# categorized food beverage and merchandise        
resultb=distinct['product'].str.contains('|'.join(beer['name']),case=False)
distinct['category']=np.where(resultb==True,'beer','other')

resulta=distinct['product'].str.contains('|'.join(wine['name']),case=False)
distinct['category'][resulta]='wine'

results=distinct['product'].str.contains('|'.join(soda['name']),case=False)
distinct['category'][results]='soda'

resultw=distinct['product'].str.contains('|'.join(water['name']),case=False)
distinct['category'][resultw]='water'

resultm=distinct['product'].str.contains('|'.join(mer['name']),case=False)
distinct['category'][resultm]='merchandise'

distinct['category'][distinct['product'].str.contains('FREDS RUBY RED|tito|liq|Scotch|SANGRIA|VODKA|MARGARITA|bourbon|rum|gin|tequila|Whisky|GREY GOOSE|tonic',case=False)]='liquor'
distinct['category'][distinct['product'].str.contains('iced tea',case=False)]='iced tea'
distinct['category'][distinct['product'].str.contains('lemonade|LEMON',case=False)]='lemonade'
distinct['category'][distinct['product'].str.contains('coffee',case=False)]='coffee'
distinct['category'][distinct['product'].str.contains('juice',case=False)]='juice'
distinct['category'][distinct['product'].str.contains('popcorn|CRACKERJACK',case=False)]='popcorn'
distinct['category'][distinct['product'].str.contains('PRETZEL',case=False)]='PRETZEL'
distinct['category'][distinct['product'].str.contains('nacho',case=False)]='nachos'
distinct['category'][distinct['product'].str.contains('m&m|HI-CHEW|SKITTLES|TWIZZLER|REESE|KLONDIKE',case=False)]='candy'
distinct['category'][distinct['product'].str.contains('CHIPS',case=False)]='chips'
distinct['category'][distinct['product'].str.contains('CONE|MAGNUM|bar|CHERRY GARCIA|cup|icecream|ice cream',case=False)]='icecream'
distinct['category'][distinct['product'].str.contains('POWERADE',case=False)]='energy drink'
distinct['category'][distinct['product'].str.contains('DOG',case=False)]='hot dog'
distinct['category'][distinct['product'].str.contains('cookie|oreo',case=False)]='cookie'
distinct['category'][distinct['product'].str.contains('frys|fries',case=False)]='french fires'
distinct['category'][distinct['product'].str.contains('FNGRS|fng|FINGERS',case=False)]='chicken fingers'
distinct['category'][distinct['product'].str.contains('burger',case=False)]='hamburger'
distinct['category'][distinct['product'].str.contains('piz',case=False)]='pizza'
distinct['category'][distinct['product'].str.contains('noodles',case=False)]='noodles'
distinct['category'][distinct['product'].str.contains('nuts',case=False)]='nuts'
distinct['category'][distinct['product'].str.contains('deli|pastrami|SAND',case=False)]='sandwiches'
distinct['category'][distinct['product'].str.contains('sushi',case=False)]='sushi'
distinct['category'][distinct['product'].str.contains('jersey|jer ',case=False)]='jersey'
distinct['category'][distinct['product'].str.contains('hat',case=False)]='hat'
distinct['category'][distinct['product'].str.contains('pom|cuffed',case=False)]='cuffed pom'
distinct['category'][distinct['product'].str.contains('tee',case=False)]='tee'
distinct['category'][distinct['product'].str.contains('bear',case=False)]='bear'
distinct['category'][distinct['product'].str.contains('puck',case=False)]='puck'
distinct['category'][distinct['product'].str.contains('lanyard',case=False)]='lanyard'
distinct['category'][distinct['product'].str.contains('foam finger',case=False)]='foam finger'
distinct['category'][distinct['product'].str.contains('scarf',case=False)]='scarf'
distinct['category'][distinct['product'].str.contains('replica|OYO',case=False)]='model'
distinct['category'][distinct['product'].str.contains('glass',case=False)]='glass'
distinct['category'][distinct['product'].str.contains('mug',case=False)]='mug'
distinct['category'][distinct['product'].str.contains('decal sheet',case=False)]='decal sheet'
distinct['category'][distinct['product'].str.contains('striker',case=False)]='striker'
distinct['category'][distinct['product'].str.contains('hoopset',case=False)]='hoopset'
distinct['category'][distinct['product'].str.contains('pullover',case=False)]='pullover'
distinct['category'][distinct['product'].str.contains('hood',case=False)]='hoodie'
distinct['category'][distinct['product'].str.contains('headband',case=False)]='headband'
distinct['category'][distinct['product'].str.contains('wristband',case=False)]='wristband'
distinct['category'][distinct['product'].str.contains('book',case=False)]='yearbook'
# parse merchandise by size
distinct['volume'][distinct['product'].str.contains(' s ',case=False)]='s'
distinct['volume'][distinct['product'].str.contains(' m ',case=False)]='m'
distinct['volume'][distinct['product'].str.contains(' l ',case=False)]='l'
distinct['volume'][distinct['product'].str.contains(' xl ',case=False)]='xl'
distinct['volume'][distinct['product'].str.contains(' l/xl ',case=False)]='l/xl'



distinct.to_csv('category and volume.csv', index=False)