#!/usr/bin/python

'''
Author : Suraj Kumar J
Company : Tata Consultancy Services
Script : Daemon to monitor SAS VA Autoload process.
'''

import os
import glob
import sys
import smtplib
import ConfigParser
import time
from os.path import isfile
from smtplib import SMTPException
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


def search_tables(action, log_file):
    '''To search the lines between start and end patterns'''
    start_exp = 'TABLES TO %s' % action
    end_exp = '--'
    function = "awk '/%s/{f=1;next} /%s/{f=0} f' %s | awk {'print $1'}" % (start_exp, end_exp, log_file)
    list = os.popen(function).read().split()
    return list


def check_error(action, log_file):
    '''To check whether any errors are present in logs'''
    error_tables = []
    table_list = search_tables(action, log_file)
    for i in range(0, len(table_list)):
        start_exp = '%sING DB.%s' % (action, table_list[i])
        function = "awk 'c&&c--;/%s/{c=6}' %s | grep ERROR" % (start_exp, log_file)
        status = os.system(function)
        if status == 0:
            error_tables.append(table_list[i])
        else:
            print 'going to next table'
    return error_tables


def check_running_autoload():
    '''To check whether any auto load process is running or not'''
    pid_file = '/data/sas94/config/Lev1/Applications/SASVisualAnalytics/VisualAnalyticsAdministrator/VA_Autoload/autoload.pid'
    if not isfile(pid_file):
        print 'Error: "%s" not found or not a file' % pid_file
        return -1
    f = open(pid_file, "r")
    for line in f:
        pid = line.strip()
    f.close()
    pid_int = int(pid)
    try:
        os.kill(pid_int, 0)
    except OSError:
        return False
    else:
        return True


def send_alert_mail(attachment, error_message_load, error_message_refresh, error_message_append, error_message_unload):
    sender = ''
    receivers = ['receivers@example.com'] #configure receivers mail address
    message = """Error Summary:
--------------------------------------
The following tables failed to LOAD, \n %s
--------------------------------------
The following tables failed to REFRESH, \n %s
--------------------------------------
The following tables failed to APPEND, \n %s
--------------------------------------
The following table failed to UNLOAD, \n %s
--------------------------------------
Please find the attached log file for more information.
    """ % (error_message_load, error_message_refresh, error_message_append, error_message_unload)

    msg = MIMEMultipart()
    msg['From'] = 'sas-va@example.com'
    msg['To'] = COMMASPACE.join(receivers)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = 'SAS VA Tables Autoload Status'

    msg.attach(MIMEText(message))

    with open(attachment, "rb") as log_file:
            part = MIMEApplication(
                log_file.read(),
                Name=basename(attachment)
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(attachment)
            msg.attach(part)


    try:
        smtpObj = smtplib.SMTP('example.com')
        smtpObj.sendmail(sender, receivers, msg.as_string())
        smtpObj.close()
        print "Successfully sent email"
    except SMTPException:
        print "Error: unable to send email"


def main():

    flag = None
    while True:

        path = '/path/to/*.log'
        list_of_files = glob.glob(path)
        latest_file = max(list_of_files, key=os.path.getctime)
        time.sleep(5)

        if latest_file != flag:
            flag = latest_file
            if not isfile(latest_file):  # change it to latest_file
                print 'Error: "%s" not found or not a file' % latest_file  # change it to latest_file
                return -1
            check_status = None
            while check_status != False:
                check_status = check_running_autoload()
                time.sleep(10)
            error_tables_load = check_error('LOAD', latest_file) # nclude action as well
            error_tables_refresh = check_error('REFRESH', latest_file)
            error_tables_append = check_error('APPEND', latest_file)
            error_tables_unload = check_error('UNLOAD', latest_file)
            if len(error_tables_load) != 0 or len(error_tables_refresh) != 0 or \
                            len(error_tables_append) != 0 or len(error_tables_unload) != 0:
                error_message_load = '\n'.join(error_tables_load)
                error_message_refresh = '\n'.join(error_tables_refresh)
                error_message_append = '\n'.join(error_tables_append)
                error_message_undload = '\n'.join(error_tables_unload)
                send_alert_mail(latest_file, error_message_load, error_message_refresh,
                                error_message_append, error_message_undload)

if __name__ == '__main__':
    try:
        sys.exit(main())

    except Exception as e:
        print "Exiting..."
        raise e
