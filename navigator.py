import imaplib
from email.parser import Parser
import email
import sys
import numpy
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import datetime
from datetime import date
from datetime import timedelta
import time
import subprocess
import re
import sqlalchemy as sa


def generatePDF():
  c =r"C:\Program Files\Tableau\Tableau Server\9.2\extras\Command Line Utility\tabcmd.exe  login -s https://tableau.nationalfunding.com/ -u svc_reporter -p ---"
  x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
  out, error  = x.communicate()
  c =r'C:\Program Files\Tableau\Tableau Server\9.2\extras\Command Line Utility\tabcmd.exe export  "NavigatorDailyReport/NavigatorDailyReport-Total" --pdf -f "C:\NF.Tableau\Output\CombinedNavigatorDailyReport.pdf"'
  x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE,universal_newlines = True)
  out, error  = x.communicate()
  c =r'C:\Program Files (x86)\Febooti Command line email\febootimail.exe -SMTP  10.0.0.198 -PORT 25 -AUTH AUTO -USER svc_reporter@nationalfunding.com -PASS --- -TO cgrininger@nationalfunding.com  -FROM svc_reporter@nationalfunding.com -SUBJECT Combined Daily Navigator Report   -BODY Please see attched report. -ATTACH C:\NF.Tableau\Output\CombinedNavigatorDailyReport.pdf'
  x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE,universal_newlines = True)
  out, error  = x.communicate()
  c =r"C:\Program Files\Tableau\Tableau Server\9.2\extras\Command Line Utility\tabcmd.exe logout"
  x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
  out, error  = x.communicate()

#   filepath="powershell -file c:\powershell\TSTNav1.ps1"
#   p = subprocess.Popen(filepath, shell=True, stdout = subprocess.PIPE)
#   stdout, stderr = p.communicate()
#   print(p.returncode) # is 0 if success
    #sys.exit()


# execute Tableau commands to schedule workbook refresh
def scheduleRefresh():
    c =r"C:\Program Files\Tableau\Tableau Server\9.2\extras\Command Line Utility\tabcmd.exe  login -s https://tableau.nationalfunding.com/ -u svc_reporter -p ---"
    x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
    out, error  = x.communicate()
    c =r'C:\Program Files\Tableau\Tableau Server\9.2\extras\Command Line Utility\tabcmd.exe refreshextracts --project "Management"  --workbook "Navigator Daily Report"'
    x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE,universal_newlines = True)
    out, error  = x.communicate()
    c =r"C:\Program Files\Tableau\Tableau Server\9.2\extras\Command Line Utility\tabcmd.exe logout"
    x = subprocess.Popen(c,stdout = subprocess.PIPE,stderr = subprocess.PIPE)
    out, error  = x.communicate()


#check if refresh process is complete
def refreshCompleteCheck():
  engine = create_engine("postgresql+pg8000://username:password@tableau.nationalfunding.com:8060/workgroup")
  connection = engine.connect()
  for row in engine.execute("SELECT extracts_refreshed_at From workgroup.public.workbooks where id=1027"):
    refreshPST = row.extracts_refreshed_at - timedelta(hours=7)
    print(refreshPST)
    t = datetime.datetime.now()
    timecheckpoint = t.replace(hour=13,minute=30,second=0,microsecond=0)
    print(timecheckpoint)
    print(refreshPST > timecheckpoint)
    if refreshPST > timecheckpoint:
      return True 
#sys.exit()

# Connect to an IMAP server
def connect(server, user, password):
    m = imaplib.IMAP4_SSL(server, 993)
    m.login(user, password)
    m.select('INBOX')
    return m

# Download all attachment files for a given email
def downloadAttachment(m, emailid, outputdir):
    resp, data = m.fetch(emailid, "(BODY.PEEK[])")
    email_body = data[0][1]
    mail = email.message_from_string(email_body.decode('utf-8'))
    if mail.get_content_maintype() != 'multipart':
        return
    for part in mail.walk():
        if part.get_content_maintype() != 'multipart' and part.get('Content-Disposition') is not None:
            if part.get_filename() == "DailyNavigator.xlsx":
                open(outputdir + '/' + part.get_filename(), 'wb').write(part.get_payload(decode=True))
                print('Wrote attachment to ' + outputdir  + part.get_filename())
                return True

#delete all email messages in mailbox
def deleteMessages(m):
    typ, data = m.search(None, 'ALL')
    for num in data[0].split():
       m.store(num, '+FLAGS', '\\Deleted')
    m.expunge()
    m.close()
    m.logout()

def writeToDB():
    xlsx = pd.read_excel("C:\\projects\\navigator\\DailyNavigator.xlsx")
    engine = create_engine("mssql+pymssql://s28/Analytics_WS")
    connection = engine.connect()
    x= xlsx.to_sql('Navigator_QB_Daily',engine,if_exists='replace',index=False,dtype={'DealId': sa.Float(),
               'Submission Date': sa.DateTime(),
               'DealTypeId': sa.Float(),
               'BrokerId': sa.Float(),
               'OpportunityId': sa.NVARCHAR(length=255),
               'First Approval': sa.DateTime(),
               'FundingDate': sa.DateTime(),
               'BrokerCommission1': sa.DECIMAL(38,0),
               'FundedAmount': sa.DECIMAL(38,0),
               'TotalRepayment': sa.DECIMAL(38,0),
               'Docs_out_Date': sa.DateTime(),
               'Docs_In_Date': sa.DateTime(),
               'DecisionDate': sa.DateTime(),              
               'DealType': sa.NVARCHAR(length=255),
               'BrokerName': sa.NVARCHAR(length=255),              
               'BusinessName': sa.NVARCHAR(length=255),
               'BusinessDba': sa.NVARCHAR(length=255),      
               'ApprovedAmount': sa.DECIMAL(38,0),
               'Status': sa.NVARCHAR(length=255),              
               'LoanNumber': sa.NVARCHAR(length=255),
               'Business Name': sa.NVARCHAR(length=255),
                     'Sub Today': sa.Float(),              
               'Decisioned Today': sa.Float()})
    connection.close()

    
if __name__ == "__main__":
  url = '10.0.0.198'
  done=False
  complete=False
  user,password = ('navigator','---')
  outputdir = "C:\\projects\\navigator\\"
  while(not done):
    time.sleep(10)         
    try:
        conn = connect(url, user, password)
    except Exception as e:
        print('Error connecting to email server: ' + e.args[0].decode('utf-8'))
        sys.exit()

    print('Successfully logged in as ' + user)  

    results,data = conn.search(None,'ALL')
    msg_ids = data[0]

    msg_id_list = msg_ids.split()
    print (len(msg_id_list))
    for s in msg_id_list:
      success = downloadAttachment(conn, s, outputdir)
      writeToDB()
      print('wrote to DB')
      deleteMessages(conn)
      print('del messages')
      scheduleRefresh()
      print(success)
      if success==True:
        done=True 
    
  while(not complete):
    time.sleep(300)
    complete = refreshCompleteCheck()

  if complete:
    generatePDF()






