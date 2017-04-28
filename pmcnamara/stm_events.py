import pandas as pd

# IMPORT RANGERS RSVPs #
nyr_fan_forum = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Rangers Fan Forum Mail Merge 0109.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_kids_camp = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/NYR Kids Training Camp STM List 1110 v2.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_11_25_ = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/RANGERS - 2016-17 90th Year Communication (11 to 25 Year Subs) 11.12.16.....xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_viewing = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Viewing Party Invite List.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_ob = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Rangers Tenure Events - ALL OB and LEG 1_24_17.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_vet = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Rangers Veteran Tenure Event Invite MM.xlsx', sheetname = 'Sheet1')[['acct_id','email_addr','tag','acct_type']]
nyr_half = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/1617 NYR Half Plan Viewing Parties.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_rookie = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Mail Merge NYR Rookie Accts 0110.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]

# IMPORT KNICKS RSVPs #
nyk_11_25_ = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/KNICKS - 11 to 25 Year Tenure (mail merge) 12.20.16.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyk_leg = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Legends Knicks Tenure Event 3_1_17.xlsx', sheetname = 'Sheet1')[['acct_id','email_addr','tag','acct_type']]
nyk_night_out = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/2.2 Knicks Night Out - Full Invite List Account IDs.xlsx', sheetname = 'Sheet1')[['acct_id']]
nyk_rookie = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/NYK Rookie Accounts 01117.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]
nyr_gold = pd.read_excel('/Users/mcnamarp/Downloads/STM Events/Golden Knicks Tenure Event 3_1_17.xlsx', sheetname = 'temp')[['acct_id','email_addr','tag','acct_type']]

# IMPORT ATTENDANCE DATA #
xls = pd.ExcelFile('/Users/mcnamarp/Downloads/STM Events/Event Attendance Numbers with Archtics IDs v lookup.xlsx')