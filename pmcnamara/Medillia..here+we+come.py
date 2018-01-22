
# coding: utf-8

# In[19]:


import nltk
from nltk.corpus import PlaintextCorpusReader
from nltk import FreqDist
from nltk.collocations import*
import re
import re
import csv
import codecs


# In[15]:


import os
print os.getcwd()


# In[158]:


import os
files = os.listdir(os.curdir)
#print(files)


# In[159]:


mycorpus = PlaintextCorpusReader('.','.*\txt')


# In[179]:


def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]


# In[180]:


filename = 'MS_0118.csv'
reader = unicode_csv_reader(open(filename)) 


# In[177]:


import codecs
f = []
delimiter = ','
reader = codecs.open('MS_0118.csv', 'r', encoding='utf-8')
for line in reader:
    row = f.append(reader['text'])
    print(row)
    #print len(row)
        # do something with your row ...
    newtweetfile = open('myfile1.txt','w')
    for item in row:
        newtweetfile.write("\n" % item)
    


# In[229]:


import pandas as pd
#cols_to_use = [15,16,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,71]
cols_to_use = [47,48]
SurveyResults=pd.read_csv('MS_0118.csv',index_col=0, parse_dates=True, usecols = cols_to_use)
SR1 = pd.read_csv('MS_0118.csv',index_col=0, parse_dates=True, usecols = cols_to_use)




# In[231]:



print (SurveyResults.columns)


# In[232]:


SurveyResults.head(n=10)
#print(SurveyResults.columns)


# In[233]:


# Removing the Nan and replacing it with a blank string
import numpy as np
SurveyResults = SurveyResults.replace(np.nan, '', regex=True)
# Displaying the data frame
SurveyResults.head(n=250)
#print(type(SurveyResults))


# In[284]:


np.savetxt(r'mytweetfile1.txt', SurveyResults.values, fmt='%s')


# In[285]:


mytweetfile = mycorpus.raw('mytweetfile1.txt')


# In[544]:


rePattern = r''' (?x)	# set flag to allow verbose regexps
      (?:https?://|www)\S+      # simple URLs
      | (?::-\)|;-\))		# small list of emoticons
      | &(?:amp|lt|gt|quot);    # XML or HTML entity
      | \#\w+                 # hashtags
      | \u'\w+                 # TEST
      | @\w+                  # mentions
      | \d+:\d+               # timelike pattern
      | \d+\.\d+              # number with a decimal
      | (?:\d+,)+?\d{3}(?=(?:[^,]|$))   # number with a comma
      | (?:[A-Z]\.)+                    # simple abbreviations
      | (?:--+)               # multiple dashes
      | \w+(?:-\w+)*          # words with internal hyphens or apostrophes
      | ['\".?!,:;/]+         # special characters
      '''


# In[545]:


mytokenizedtweet = nltk.regexp_tokenize(mytweetfile,rePattern)


# In[546]:


mytweethashtags = []
for w in mytokenizedtweet:
    mytweethashtags.append(w)


# In[547]:


myuniquehashtags = set(mytweethashtags)
fdist2 = FreqDist(mytweethashtags)
top20hashtags = fdist2.most_common(50)[:20]


# In[548]:


print('Which hashtags have I (or someone) used in my (or someone’s) tweets in the past?')
print(' Answer : These are the top 20 hashtags along with their frequencies :' + str(top20hashtags))


# In[549]:


CommentFile = mycorpus.raw('Comments.txt')
# to convert the unicode file into string
CommentFile = CommentFile.encode('ascii', 'ignore')
print(type(CommentFile))


# In[552]:


tokenizedcommentfile = nltk.regexp_tokenize(CommentFile,rePattern)
print(type(tokenizedcommentfile))
#print(mytokenizedtweet1)
retokenized = []
for w in tokenizedcommentfile:
    retokenized.append(w)                                                               


# In[553]:


retokenized2 = []
for l in retokenized:
    k = l.lower()
    retokenized2.append(k)  


# In[554]:


print(retokenized2[:20])


# In[555]:


from nltk.corpus import stopwords
filtered_words = [word for word in retokenized2 if word not in stopwords.words('english')]


# In[587]:


ignorelist = [',','.',':','!','''''','/','....','"','!!','!!!',"'" ]
filtered_words = [word for word in filtered_words if word not in ignorelist]


# In[588]:


filtered_words1 = set(filtered_words)
fdist2 = FreqDist(filtered_words1)
#print(type(fdist2))
top20commentwords = fdist2.most_common(50)[:20]
print(type(top20commentwords))
print(top20commentwords)


# In[589]:


print('Which are the most common words in the comments?')
print(' Answer : These are the top 20 words along with their frequencies :' + str(top20commentwords))


# In[590]:


from collections import defaultdict


# In[591]:


d = defaultdict(int)
for k in filtered_words:
    d[k] +=1
    


# In[592]:


k = list(d.items())
#print(k)
print(type(k))
print(sorted(k))

    


# In[593]:


def getKey(item):
    return item[1]
Wordlist = sorted(k, key=getKey)
print(Wordlist)


# In[594]:


print(len(Wordlist))


# **Results to ponder**

# In[595]:


print(Wordlist[3150:])


# **Comments we can use to improvise** (SoI - Scope of Improvement metrics)

# In[596]:


SoICommentsFile = mycorpus.raw('SoIComments.txt')
# to convert the unicode file into string
SoICommentFile = SoICommentsFile.encode('ascii', 'ignore')
print(type(SoICommentFile))


# In[597]:


SoItokenizedcommentfile = nltk.regexp_tokenize(SoICommentFile,rePattern)
print(type(SoItokenizedcommentfile))
#print(mytokenizedtweet1)
SoIretokenized = []
for w in SoItokenizedcommentfile:
    SoIretokenized.append(w) 


# In[598]:


SoIretokenized2 = []
for l in SoIretokenized:
    k = l.lower()
    SoIretokenized2.append(k)

print(SoIretokenized2[:20])


# In[599]:


from nltk.corpus import stopwords
SoIfiltered_words = [word for word in SoIretokenized2 if word not in stopwords.words('english')]


# In[600]:


ignorelist = [',','.',':','!','''''','/','....','"','!!','!!!',"'" ]
SoIfiltered_words = [word for word in SoIfiltered_words if word not in ignorelist]


# In[601]:


from collections import defaultdict

d = defaultdict(int)
for k in SoIfiltered_words:
    d[k] +=1


# In[602]:


SoIk = list(d.items())
#print(k)
print(type(SoIk))
#print(sorted(SoIk))


# In[603]:


def getKey(item):
    return item[1]
SoIWordlist = sorted(SoIk, key=getKey)
#print(SoIWordlist)


# In[604]:


print(len(SoIWordlist))
print(SoIWordlist[400:])

