import pandas as pd
# IMPORT AND FORMAT #
data = pd.read_csv('/Users/mcnamarp/Downloads/aff_download/ECN_2012_US_52A1_with_ann.csv', header = 1)
data.replace({'Revenue ($1,000)' : { 'D' : 0, 'Q' : 0, 'N' : 0 }}, inplace = True)
data['Annual payroll ($1,000)'].replace({'D':0}, inplace = True)
data[['Revenue ($1,000)','Annual payroll ($1,000)']] = data[['Revenue ($1,000)','Annual payroll ($1,000)']].astype(int)

# REMOVE UNWANTED INDUSTRIES AND CITIES #
industry_codes = [52,522,523,524,5221,522110,5231,523920,5242,5239,523120,52311,52412,5221102,524113]
data = data[data['2012 NAICS code'].isin(industry_codes)]

cities = [35620,31080,16980,14460,41860,37980,19100,33460,47900,26420,12060,33100,38060,19740,19820,12580,42660,45300,41740,38300,34980,12420,36740,29820]
data = data[data['Id2'].isin(cities)]

# COMBINE DC AND BALTIMORE #
data.replace({'Geographic area name' : { 'Baltimore-Columbia-Towson, MD Metro Area' : 'Washington-Baltimore-Arlington-Alexandria-DC-VA-MD Metro Area', 'Washington-Arlington-Alexandria, DC-VA-MD-WV Metro Area' : 'Washington-Baltimore-Arlington-Alexandria-DC-VA-MD Metro Area'}}, inplace = True)

data.groupby(['Id2','Geographic area name']).sum()[['Number of establishments','Revenue ($1,000)','Annual payroll ($1,000)']].to_csv('/Users/mcnamarp/Documents/metro_finance_metrics.csv')