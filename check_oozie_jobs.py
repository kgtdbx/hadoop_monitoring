#!/usr/bin/python

'''
Author : Suraj Kumar J
Company : Tata Consultancy Services
Script : Checking Oozie Skipped and Killed job details.
'''

import urllib2
import json
import ConfigParser
import sys
from collections import OrderedDict
from datetime import datetime, timedelta

# Thu, 20 Jul 2017 09:01:00 GMT
# to 2017-07-20 09:01

def fetch_oozie_job_details(url, username, password):

    '''
    To fetch data for passed oozie job id
    '''

    p = urllib2.HTTPPasswordMgrWithDefaultRealm()
    p.add_password(None, url, username, password)
    handler = urllib2.HTTPBasicAuthHandler(p)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    read_rest = urllib2.urlopen(url).read()
    rest_data = json.loads(read_rest)
    return rest_data

def check_skipped_job(rest_data):
    '''
    Check skipped job from passed job related data
    :return:
    '''
    formatted_date = []
    formatted_date_daily = []
    formatted_list = []
    for i in range(0, len(rest_data.get('actions'))):
        if rest_data.get('actions')[i].get('status') == 'SKIPPED' or rest_data.get('actions')[i].get('status') == 'KILLED':
            normal_time = rest_data.get('actions')[i].get('nominalTime')
            date_form = (datetime.strptime(normal_time, "%a, %d %b %Y %H:%M:%S %Z") + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M")
            formatted_date.append(date_form)
            extract_hour = int(datetime.strptime(date_form, "%Y-%m-%d %H:%M").strftime("%H"))
            if extract_hour in [0, 1, 2, 3, 4, 6]:
                  formatted_date_daily.append(date_form)

    formatted_list.append(formatted_date)
    formatted_list.append(formatted_date_daily)
    return formatted_list


def prepare_re_run_list(re_run_file_stage, re_run_file, formatted_date):
    '''
    Prepares files for hourl and daily re-runs
    '''
    x = OrderedDict.fromkeys(formatted_date)
    with open(re_run_file_stage, "r") as f:
     lines = list(OrderedDict.fromkeys(f.read().splitlines()))
    y = OrderedDict.fromkeys(lines)

    for k in x:
        if k in y:
            x.pop(k)


    for date in x.keys():
        with open(re_run_file_stage, "a+") as f:
           f.write(date + '\n')
    for date in x.keys():
        with open(re_run_file, "a+") as f:
           f.write(date + '\n')

def main():

    re_run_file_stage = 're_run_list_stage.txt'
    re_run_file_stage_daily = 're_run_list_stage_daily.txt'
    re_run_file = '/tmp/hourly_re_run.list'
    re_run_file_daily = '/tmp/daily_re_run.list'
    config_vault = 'vault.ini'
    config = ConfigParser.RawConfigParser()
    config.read(config_vault)


    username = config.get('oozie', 'username')
    password = config.get('oozie', 'password')
    url = config.get('oozie', 'url')


    rest_data = fetch_oozie_job_details(url, username, password)
    formatted_list = check_skipped_job(rest_data)
    prepare_re_run_list(re_run_file_stage, re_run_file, formatted_list[0])
    prepare_re_run_list(re_run_file_stage_daily, re_run_file_daily, formatted_list[1])


if __name__ == '__main__':
    try:
        sys.exit(main())

    except Exception as e:
        raise e