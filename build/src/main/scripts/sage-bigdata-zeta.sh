#!/bin/bash

cygwin=false
darwin=false
aix=false
hpux=false
nix=false
case "`uname`" in
(*CYGWIN*) cygwin=true;;
(*Darwin*) darwin=true;;
(*AIX*) aix=true;;
(*HP-UX*) hpux=true;;
(*Linux*) nix=true;;
esac

home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
startStop=$1
shift
command=$1
shift
defaultMemory=1
memoryUnit="g"

params=$@
JVM_OPTS="$(cat $home/conf/zeta.vmoptions|awk '{{printf"%s ",$0}}')"

if [ -d "${home}/jre/1.8" ]
then
    JAVA_HOME="$home/jre/1.8"
    JAVA="${JAVA_HOME}/bin/java"
else
    if [ -d "${JAVA_HOME}" ]
    then
      JAVA="${JAVA_HOME}/bin/java"
    else
      if [ `command -v java` ]
      then
        JAVA="java"
      else
        echo "JAVA_HOME is not set" >&2
        exit 1
      fi
    fi

    JAVA_VER=`$JAVA -version 2>&1 >/dev/null | grep 'java version' | awk -F"\"" '{print $2}'|cut -d . -f 1,2`
    if [ "$JAVA_VER" != "1.8" ]
    then
        echo "Found java with version: $JAVA_VER, but 1.8 is required!"
        exit 1
    fi
fi

echo "JAVA_HOME: $JAVA_HOME"
echo $JAVA

function classpath() {
    arr=("$( ls $1 )")
    first="1"
    cp=""
    for jar in $arr; do
        if [[ "$first" = "1" ]]; then
            first="0"
            cp="$1/$jar"
        else
            cp="$cp:$1/$jar"
        fi
    done;
    echo $cp;
}

function check() {
    if [ -f $pid ]; then
	if kill -0 `cat $pid` > /dev/null 2>&1; then
	    echo $command running as process `cat $pid`.  Stop it first.
	    exit 1
	fi
    fi
}

function hardwareCheck() {
   if $darwin; then
        start
   else
      freeCmd="$(free -$memoryUnit)"
      freeMemorySize="$(echo $freeCmd | cut -d ' ' -f 10)"
      echo "System free memory size: $freeMemorySize$memoryUnit"
        start
       #if [ $freeMemorySize -gt $defaultMemory ]
       #then
           #start
       #else
           #echo "Can't start $command, Because the system memory is less than $defaultMemory$memoryUnit !"
           #exit 1
       #fi
   fi
}

function start() {
###
#-Djava.rmi.server.hostname=0.0.0.0 -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
###
    security_file=$conf/security.keygen
    if [ ! -f "$security_file" ]; then
    touch "$security_file"
    fi
    #java  -Xms128M -Xmx1G -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6006 -Doracle.jdbc.J2EE13Compliant=true -cp $conf:$cp $main $params > $out 2>&1 &
    $JAVA $JVM_OPTS -cp $conf:$cp $main $params > $out 2>&1 &
    echo $! > $pid
}
function startfg() {
###
#-Djava.rmi.server.hostname=0.0.0.0 -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false
###
    security_file=$conf/security.keygen
    if [ ! -f "$security_file" ]; then
    touch "$security_file"
    fi
    #java -Xms128M -Xmx1G -Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=6006 -Doracle.jdbc.J2EE13Compliant=true  -cp $conf:$cp $main $params
    $JAVA $JVM_OPTS -cp $conf:$cp $main $params > $out 2>&1 &
    echo $! > $pid
}

function stop() {
    if [ -f $pid ]; then
	if kill -0 `cat $pid` > /dev/null 2>&1; then
            echo stopping $command
	    echo `cat $pid`
            kill `cat $pid`
	else
            echo no $command to stop
	fi
    else
	echo no $command to stop
    fi
}

#dir
conf="$home/conf"
pids="$home/pids"
logs="$home/logs"

#file
out="$logs/$command.out"
pid="$pids/$command.pid"

#java
cp="$( classpath "$home/lib" )"
#init
mkdir -p $conf $logs $pids


case $command in
    worker)
	    main="com.haima.sage.bigdata.etl.server.Worker"
	    ;;
    master)
	    main="com.haima.sage.bigdata.etl.server.Master"
	    ;;
    daemon)
	    main="com.haima.sage.bigdata.etl.daemon.SageDaemon"
	    ;;
	auth)
	    main="porter.runner.Auth"
	    ;;
esac
case  $startStop in
    start)
	check
	hardwareCheck
	;;
    stop)
	stop
	;;
esac
