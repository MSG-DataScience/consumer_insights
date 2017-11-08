#!/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.starttls()
server.login("Tianyu.Hao@msg.com", "Dududai1104")
 
msg = MIMEMultipart()
msg['From'] = 'Tianyu.Hao@msg.com'
msg['To'] = 'patrick.mcnamara@msg.com'
msg['Subject'] = 'simple email in python'
message = 'Standby'
msg.attach(MIMEText(message))
server.sendmail("Tianyu.Hao@msg.com", "patrick.mcnamara@msg.com",msg.as_string())
server.quit()
