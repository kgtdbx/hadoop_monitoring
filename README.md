# hadoop_monitoring

# Introduction
Set of python scripts to monitor Hadoop / Ecosystem components for effective maintenance/support work

## Pre-requisites

* Python 2.7.13

## List of Scripts

* check_yarn_jobs.py
* check_oozie_jobs.py
* make_drill_clean.py

## Usage

* check_yarn_jobs.py - Monitors Apache YARN Jobs and kills if job runs more than threshold time.
* check_oozie_jobs.py - Monitors Apahe Oozie jobs and finds jobs which are skipped / killed beacuse of other dependency workflows 
* make_drill_clean.py - Monitors Apache Drill queries and kills queries if it runs more than threshold time , also checks any 'CANCELLATION_REQUESTED' queries then restarts the respective foreman if there is NO ACTIVE QUERIES running. 

