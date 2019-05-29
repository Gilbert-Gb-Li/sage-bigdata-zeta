#!/bin/sh
  # myjsvc This shell script takes care of starting and stopping
  #
  # chkconfig: - 60 50
  # description: sage-bigdata-etl-daemon-service stat is a stat data daemon.
  # processname: sage-bigdata-etl-daemon-service
  # Source function library.



if  `chkconfig | grep sage-bigdata-etl-daemon-service | egrep -v grep >/dev/null`; then
    if ! `ps -ef | grep sage-bigdata-etl-daemon-service | egrep -v grep >/dev/null`; then
        echo ' service[sage-bigdata-etl-daemon-service] had stopped'
    else
        service sage-bigdata-etl-daemon-service stop
    fi
else
    echo 'please install service[sage-bigdata-etl-daemon-service] first'
fi
