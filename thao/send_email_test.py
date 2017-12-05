#!/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.starttls()
server.login("Tianyu.Hao@msg.com", "Dududainb1104")
 
msg = MIMEMultipart()
msg['From'] = 'Tianyu.Hao@msg.com'
msg['To'] = 'Tianyu.Hao@msg.com'
msg['Subject'] = ''
message = 'Standby'
msg.attach(MIMEText(message))
server.sendmail("Tianyu.Hao@msg.com", "Tianyu.Hao@msg.com",msg.as_string())
server.quit()
