#!/bin/sh
  # myjsvc This shell script takes care of starting and stopping
  #
  # chkconfig: - 60 50
  # description: sage-bigdata-etl-daemon-service stat is a stat data daemon.
  # processname: sage-bigdata-etl-daemon-service
  # Source function library.
cygwin=false
darwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
Darwin*) darwin=true;;
esac

home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
real=${home//\//\\/}

if $darwin; then
    echo "Starting on Darwin OS..."
    sed -i '' "s/\.\//$real\//g" $home/conf/daemon.conf
    sed -i '' "s/>logs/>$real\/logs/g" $home/conf/logback-daemon.xml
else
    echo "Starting on *nix OS..."
    sed -i  "s/\.\//$real\//g" $home/conf/application.conf
    sed -i  "s/>logs/>$real\/logs/g" $home/conf/logback-daemon.xml
fi


if `chkconfig | grep sage-bigdata-etl-daemon-service | egrep -v grep >/dev/null`; then
    if [ -n `ps -ef | grep sage-bigdata-etl-daemon-service | egrep -v grep >/dev/null` ]; then
        service sage-bigdata-etl-daemon-service start
    else
        echo ' service[sage-bigdata-etl-daemon-service] had started,please stop it first'
    fi
else
    echo 'please install service[sage-bigdata-etl-daemon-service] first'
fi


