# hadoop_monitoring

# Introduction
Set of python scripts to monitor Hadoop / Ecosystem components for effective maintenance/support work

## Pre-requisites

* Python 2.7.13

## List of Scripts

* check_yarn_jobs.py
* check_oozie_jobs.py
* make_drill_clean.py
* sas_autoload_monitor_daemon.py

## Usage

* check_yarn_jobs.py - Monitors Apache YARN Jobs and kills if job runs more than threshold time.
* check_oozie_jobs.py - Monitors Apahe Oozie jobs and finds jobs which are skipped / killed because of other dependency workflows 
* make_drill_clean.py - Monitors Apache Drill queries and kills queries if it runs more than threshold time , also checks any 'CANCELLATION_REQUESTED' queries then restarts the respective foreman if there is NO ACTIVE QUERIES running. 
* sas_autoload_monitor_daemon.py Program is created to continuously checks for the latest log created by Auto load process, an alert mail is also sent if any failure occurs. The mail will have name of the table(s) which failed to load, refresh, append or unload. Also an attachment of the processed log file is sent for detailed information. 

