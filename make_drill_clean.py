#!/usr/bin/python

'''
Author : Suraj Kumar J
Company : Tata Consultancy Services
Script : Handling long running and unresponsive Apache Drill queries.
'''



from pydrill.client import PyDrill
from smtplib import SMTPException
import time
import os
import sys
import smtplib
import ConfigParser

def report_generator(incoming, forman_nodes, stuck_queries, long_queries):
    report_file = 'report.ini'
    config = ConfigParser.RawConfigParser()
    config.read(report_file)
    if incoming == 'restart':
        for nodes in forman_nodes:
            initial_value = config.getint('report', nodes)
            final_value = initial_value + 1
            config.set('report', nodes, final_value)
    elif incoming == 'stuck_queries':
        for queries in stuck_queries:
            initial_value = config.getint('report', 'stuckQueries')
            final_value = initial_value + 1
            config.set('report', 'stuckQueries', final_value)
    elif incoming == 'long_running':
        for queries in long_queries:
            initial_value = config.getint('report', 'longQueries')
            final_value = initial_value + 1
            config.set('report', 'longQueries', final_value)
    else:
        print 'Invalid input'

    with open(report_file, 'w') as configfile:
        config.write(configfile)


def send_alert_mail(action, foreman, status):
    sender = 'drillProd-cleaner@example.com'
    receivers = ['receivers@example.com']

    message = """From: Drill Cleaner <drillProd-cleaner@example.com>
Subject: Drill Cleaner : %s triggered

The %s node(s) has been %sed. Exit status for %s is %s.
     """ % (action, foreman, action, action, status)

    try:
        smtpObj = smtplib.SMTP('smtp.example.com')
        smtpObj.sendmail(sender, receivers, message)
        print "%s : Successfully sent email" % time.strftime('%Y/%m/%d %H:%M:%S')
    except SMTPException:
        print "%s : Error: unable to send email" % time.strftime('%Y/%m/%d %H:%M:%S')


def authenticate_drill_client(drill_bit, credentials):
    '''
    Fuction to authenticate drill client
    :return:
    '''

    drill = PyDrill(host=drill_bit, port=8047, auth=credentials, use_ssl=True, verify_certs=False)

    print '% s : Drill client Authenticated Successfully' % time.strftime('%Y/%m/%d %H:%M:%S')

    return drill


def check_drill_active_queries(drill):
    '''
     Function to check drill active query

     :return:
    '''
    print '%s : Checking active drill queries' % time.strftime('%Y/%m/%d %H:%M:%S')
    prof = drill.profiles(timeout=30)
    for key, value in prof.data.items():
        if key == 'runningQueries':
            if len(value) > 0:
                print '%s : Active queries found' % time.strftime('%Y/%m/%d %H:%M:%S')
            else:
                print '%s : No active queries' % time.strftime('%Y/%m/%d %H:%M:%S')
            active_queries = value
            return active_queries


def check_running_queries(drill):
    '''

    :return:
    '''
    active_queries = check_drill_active_queries(drill)
    normal_running_queries = []
    for i in range(0, len(active_queries)):
        if active_queries[i].get('state') == 'RUNNING' or active_queries[i].get('state') == 'STARTING':
            normal_running_queries.append(active_queries[i].get('queryId'))
    if len(normal_running_queries) > 0:
        print '%s : Found running queries' % time.strftime('%Y/%m/%d %H:%M:%S')
        return False
    else:
        print '%s : No Running queries' % time.strftime('%Y/%m/%d %H:%M:%S')
        return True


def check_long_running_queries(drill):
    '''

    :return:
    '''
    active_queries = check_drill_active_queries(drill)
    long_running_queries = []
    threshold_duration = 120000
    for i in range(0, len(active_queries)):
        if active_queries[i].get('state') == 'RUNNING' and \
           active_queries[i].get('user') == 'aisti-cognos-svc-prod' or \
           active_queries[i].get('user') == 'sade-flume-svc-prod':
            start_timestamp = active_queries[i].get('startTime')
            start_time = active_queries[i].get('time')
            present_timestamp = int(round(time.time() * 1000))
            query_duration = present_timestamp - start_timestamp
            if query_duration > threshold_duration:
                long_running_queries.append(active_queries[i].get('queryId'))
                pr = drill.profile(active_queries[i].get('queryId'))
                pr.data.get('query')
                pr.data.get('foreman')
                with open("/home/e908106/longRunningQueries.log", "a") as myfile:
                    myfile.write('\n------------------------------')
                    myfile.write('\nQuery id : %s' % active_queries[i].get('queryId'))
                    myfile.write("\nForeman node is : %s" % pr.data.get('foreman'))
                    myfile.write("\n%s" % pr.data.get('query'))
                    myfile.write('\n------------------------------')

    if len(long_running_queries) > 0:
        print '%s : Found long running queries' % time.strftime('%Y/%m/%d %H:%M:%S')
        cancel_drill_queries(drill, long_running_queries)
        report_generator('long_running', None, None, long_running_queries)
        return True
    else:
        print '%s : No long Running queries' % time.strftime('%Y/%m/%d %H:%M:%S')
        return False



def check_stuck_queries(drill):
    '''


    '''
    active_queries = check_drill_active_queries(drill)
    status = None
    long_status = None
    iterate = 0
    stuck_queries = []
    foreman_nodes = []
    for i in range(0, len(active_queries)):
        if active_queries[i].get('state') == 'CANCELLATION_REQUESTED':
            print 'Stuck query with queryId : %s' % active_queries[i].get('queryId')
            stuck_queries.append(active_queries[i].get('queryId'))
            # start_timestamp = value[i].get('startTime')
            start_time = active_queries[i].get('time')
            foreman_nodes.append(active_queries[i].get('foreman'))
    if len(stuck_queries) > 0:
        print '%s : Stuck Queries found' % time.strftime('%Y/%m/%d %H:%M:%S')
        while status != True and iterate < 18:
            #status = check_running_queries(drill)
            long_status = check_long_running_queries(drill)
            status = check_running_queries(drill)
            iterate = iterate + 1
        print '%s : Stuck Queries found, Triggering safe Drillbit restart' % time.strftime('%Y/%m/%d %H:%M:%S')
        restart_drill_bits(list(set(foreman_nodes)))
        report_generator('stuck_queries', None, stuck_queries, None)
        report_generator('restart', list(set(foreman_nodes)), None, None)
    else:
        print '%s : No Stuck Queries found' % time.strftime('%Y/%m/%d %H:%M:%S')


def cancel_drill_queries(drill, long_running_queries):
    '''
    Function to cancel drill queries.
    :return:
    '''
    for query_id in long_running_queries:
        print '%s : Cancelling long running query with queryID : %s' % (time.strftime('%Y/%m/%d %H:%M:%S'), query_id)
        try:
         status = drill.profile_cancel(query_id, timeout=30)
        except Exception as e:
         print 'Exception Occurred at cancel profile'
         send_alert_mail('Cancel', query_id, status.data)

def restart_drill_bits(foreman_nodes):
    '''
    Function to restart drill bits
    :return:
    '''
    drill_hosts = ' '.join(foreman_nodes)
    print '%s : Restarting drillbit on %s' % (time.strftime('%Y/%m/%d %H:%M:%S'), drill_hosts)
    status = os.system(
          "maprcli node services -name drill-bits -action restart -nodes %s" % drill_hosts)
    print '%s : Exit status for restart is %d' % (time.strftime('%Y/%m/%d %H:%M:%S'), status)
    send_alert_mail('Restart', drill_hosts, status)


def main():
    status = None
    drill_bit = 'drill-bit-host'
    credentials = 'user:pass'
    print '--------------------------------------------------------'
    print '%s : Started Apache Drill Cleaner' % time.strftime('%Y/%m/%d %H:%M:%S')
    drill = authenticate_drill_client(drill_bit, credentials)
    print '%s : Checking Stuck Queries' % time.strftime('%Y/%m/%d %H:%M:%S')
    check_stuck_queries(drill)
    print '%s : Checking Long running Queries' % time.strftime('%Y/%m/%d %H:%M:%S')
    status = check_long_running_queries(drill)
    if status == True:
        print 'Checks in next 5 minutes and treats as stuck query and restarts if exists'
    print '--------------------------------------------------------'
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())

    except Exception as e:
        raise e