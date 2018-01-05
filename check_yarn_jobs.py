#!/usr/bin/python

'''
Author : Suraj Kumar J
Company : Tata Consultancy Services
Script : Killing long running yarn/spark jobs.
'''

import urllib2
import json
import ConfigParser
import sys
import time
import os
from collections import OrderedDict
from datetime import datetime, timedelta

def send_alert_mail(application_id, error_msg):

 sender = 'sender@no-reply.com'
 receivers = ['example@example.com']

 message = """To: example@example.com;
Subject: Successfully killed long yarn application

Killed application with application id : %s.

 """ % (application_id)

 try:
   smtpObj = smtplib.SMTP('example.com')
   smtpObj.sendmail(sender, receivers, message)
   print "Successfully sent email"
   smtpObj.quit()
 except smtplib.SMTPException:
   print "Error: unable to send email"


def fetch_yarn_job_details(url, username, password):

    '''
    To fetch data for passed yarn jobs
    '''

    p = urllib2.HTTPPasswordMgrWithDefaultRealm()
    p.add_password(None, url, username, password)
    handler = urllib2.HTTPBasicAuthHandler(p)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    read_rest = urllib2.urlopen(url).read()
    rest_data = json.loads(read_rest)
    return rest_data

def check_long_job(rest_data):
    '''
    Check long job from passed job related data
    :return:
    '''
    threshold = 3300000
    for i in range(0, len(rest_data.get('apps').get('app'))):
          start_timestamp = rest_data.get('apps').get('app')[i].get('startedTime')
          application_id = rest_data.get('apps').get('app')[i].get('id')
          application_name = rest_data.get('apps').get('app')[i].get('name')
          present_timestamp = int(round(time.time() * 1000))
          query_duration = present_timestamp - start_timestamp
          if query_duration > threshold:
                os.system('yarn application -kill %s' % application_id)
				send_alert_mail(application_id)


def main():

    config_vault = 'vault.ini'
    config = ConfigParser.RawConfigParser()
    config.read(config_vault)

    urls = []

    username = config.get('yarn', 'username')
    password = config.get('yarn', 'password')
    urls.append(config.get('yarn', 'url_3'))
    urls.append(config.get('yarn', 'url_2'))
    urls.append(config.get('yarn', 'url_1'))

    for url in urls:
        try:
           rest_data = fetch_yarn_job_details(url, username, password)
        except Exception:
             pass

    check_long_job(rest_data)


if __name__ == '__main__':
    try:
        sys.exit(main())

    except Exception as e:
        raise e