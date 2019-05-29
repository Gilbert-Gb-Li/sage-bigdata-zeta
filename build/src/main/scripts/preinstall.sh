#!/bin/bash

PWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." && pwd )"
echo $PWD_HOME

CWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
echo $CWD_HOME

CONF=""
UPGRADE=0

## -f <setting.env> file
## -U or -u, app upgrade
while [ -n "$1" ]
do
    case "$1" in
	-U|-u) UPGRADE=1; shift;;
	-f) CONF=$2; shift 2;;
	*) echo "Error with unknown parameters!"; exit 1;;
    esac
done


echo "CONF: $CONF"
echo "UPGRADE: $UPGRADE"

if [ "X$CONF" != "X" ]
then
    if [ -f $CONF ]
    then
	echo "Get configuration from file : $CONF"
	source $CONF
    else
	echo "Using default setting.env file..."
	if [ -f $CWD_HOME/setting.env ]
	then
	    source $CWD_HOME/setting.env
	else
	    echo "Please specify configuration file!"
	    exit -1
	fi
    fi
else
    echo "Using default setting.env file..."
    if [ -f $CWD_HOME/setting.env ]
    then
	source $CWD_HOME/setting.env
    else
	echo "Please specify configuration file!"
	exit -1
    fi

fi

netstat -tln >/dev/null 2>/dev/null
netstatFlat=`echo $?`
if [ $netstatFlat -ne 0 ]
then
	echo "ERROR: The net-tools(netstat) is not installed, Please reinstall after installing net-tools(netstat)."
	exit 1;
fi


arr=("$( netstat -tln |grep -o -E -w $MASTER_WEB_PORT )")
len=${#arr}
if [ $len -gt 0 ]
then
	echo "ERROR: Master web address or port already in use, please reedit master web address or port"
	exit 1;
fi

arr=("$( netstat -tln |grep -o -E -w $MASTER_PORT_INTERNAL )")
len=${#arr}
if [ $len -gt 0 ]
then
	echo "ERROR: Master internal address or port already in use, please reedit master internal address or port"
	exit 1;
fi

arr=("$( netstat -tln |grep -o -E -w $WORKER_PORT )")
len=${#arr}
if [ $len -gt 0 ]
then
	echo "ERROR: Agent(worker) internal address or port already in use, please reedit agent(worker) internal address or port"
	exit 1;
fi

arr=("$( netstat -tln |grep -o -E -w $DAEMON_PORT )")
len=${#arr}
if [ $len -gt 0 ]
then
	echo "ERROR: Daemon address or port already in use, please reedit daemon address or port"
	exit 1;
fi




SAGE_PKG=`ls $CWD_HOME/sage-bigdata-zeta-*.tar.gz`
SAGE_BN=`basename $SAGE_PKG`
SAGE_PKG_FOLDER=${SAGE_BN/.tar.gz}
SYS_VERSION=`echo $SAGE_PKG_FOLDER|awk -F"-" '{print $2}'`

case $SYS_VERSION in
(*nix*) SAGE_VERSION=${SAGE_PKG_FOLDER/sage-bigdata-zeta-nix-};;
(*aix*) SAGE_VERSION=${SAGE_PKG_FOLDER/sage-bigdata-zeta-aix-};;
(*hpux*) SAGE_VERSION=${SAGE_PKG_FOLDER/sage-bigdata-zeta-hpux-};;
esac

SAGE_PKG_TAR=${SAGE_BN/.gz}
mkdir $TOP_INSTALL_DIR
chmod 755 $TOP_INSTALL_DIR
gunzip -c $SAGE_PKG > $TOP_INSTALL_DIR/$SAGE_PKG_TAR
cd $TOP_INSTALL_DIR
tar -xvf $TOP_INSTALL_DIR/$SAGE_PKG_TAR

echo "sage-bigdata-zeta package: $SAGE_PKG"


if [ "X"$MASTER_INSTALL = "X1" ]
then
    MASTER_DEST_DIR=$TOP_INSTALL_DIR
    MASTER_PKG_FOLDER=$SAGE_PKG_FOLDER
fi

if [ "X"$WORKER_INSTALL = "X1" ]
then
    WORKER_DEST_DIR=$TOP_INSTALL_DIR
    WORKER_PKG_FOLDER=$SAGE_PKG_FOLDER
fi

SEDOPT=""
case "`uname`" in
    Darwin) SEDOPT="\"\""
esac

if [ "X"$MASTER_INSTALL = "X" ]
then
    echo "Master will not be installed."
else
    if [ "X"$MASTER_WEB_HOST" = "X" ]
    then
	MASTER_WEB_HOST=127.0.0.1
    fi

    if [ "X"$MASTER_WEB_PORT" = "X" ]
    then
	MASTER_WEB_PORT=19090
    fi
fi

if [ "X"$WORKER_INSTALL = "X" ]
then
    echo "Worker will not be installed."
else
    if [ "X"$WORKER_HOST = "X" ]
    then
	WORKER_HOST=127.0.0.1
    fi

    if [ "X"$WORKER_PORT = "X" ]
    then
	WORKER_PORT=19093
    fi
fi

if [ "X"$AUTH_ENABLED = "X" ]
then
    echo "Auth will not be enabled."
else
    if [ "X"$AUTH_HOST = "X" ]
    then
	AUTH_HOST=127.0.0.1
    fi

    if [ "X"$AUTH_PORT = "X" ]
    then
	AUTH_PORT=9000
    fi
fi

if [ "X"$DAEMON_PORT = "X" ]
then
    DAEMON_PORT=19095
fi

echo "Information collected as following before launch installation:"
echo "Master destination dir: $MASTER_DEST_DIR"
echo "Master IP: $MASTER_WEB_HOST"
echo "Master Port: $MASTER_WEB_PORT"
echo "Worker destination dir: $WORKER_DEST_DIR"
echo "Worker IP: $WORKER_HOST"
echo "Worker Port: $WORKER_PORT"
echo "Worker ID: $WORKER_ID"

handlePlugin(){
    PLUGIN_PKG=$1
    echo "$PLUGIN_PKG"
    TEMPDIR=`dirname $PLUGIN_PKG`
    ZIPFILE=`basename $PLUGIN_PKG`
    PLUGIN_PKG_FOLDER=${ZIPFILE/.tar.gz}
    PLUGIN_PKG_TAR=${ZIPFILE/.gz}
    gunzip -c $PLUGIN_PKG > $TEMPDIR/$PLUGIN_PKG_TAR
    cd $TEMPDIR
    tar -xvf $TEMPDIR/$PLUGIN_PKG_TAR
    rm -r $TEMPDIR/$PLUGIN_PKG_TAR

    cp $TEMPDIR/$PLUGIN_PKG_FOLDER/lib/*.jar $WORKER_DEST_DIR/worker/lib/ 2>/dev/null
    cp $TEMPDIR/$PLUGIN_PKG_FOLDER/conf/*.conf $WORKER_DEST_DIR/worker/conf/ 2>/dev/null
    rm -rf $TEMPDIR/$PLUGIN_PKG_FOLDER
}

installPlugins(){
    if [ -d "$CWD_HOME/plugin" ]
    then
    VERSION_PLUGIN=$SAGE_VERSION
    fi
    
    # We need to check not install both es2 and es5, so does sqlserver and sqlserver-jdts
    ES_COUNT=`echo $WORKER_PLUGINS|awk -F 'es[25]' '{print NF-1}'`
    SQLSERVER_COUNT=`echo $WORKER_PLUGINS|awk -F 'sqlserver' '{print NF-1}'`
    echo "WORKER_PLUGINS: $WORKER_PLUGINS"
    echo "ES_COUNT: $ES_COUNT"
    echo "SQLSERVER_COUNT: $SQLSERVER_COUNT"
    if [ $ES_COUNT -gt 1 -o $SQLSERVER_COUNT -gt 1 ]
    then
	echo "es2 and es5 or sqlserver and sqlserver-jdts can not be both installed!"
	exit -1
    fi

    if [ "X"$WORKER_PLUGINS != "X" ]
    then
        LD_IFS="$IFS"
        IFS=","
        arr=($WORKER_PLUGINS)
        IFS="$OLD_IFS"
        for s in ${arr[@]}
        do
            echo "install $s plugin..."
            PLUGINZIP=`ls $CWD_HOME/plugin/*-$s-$VERSION_PLUGIN*.tar.gz`
            if [ $? -eq 0 ]
            then
            handlePlugin $PLUGINZIP
            fi
        done
    else
	    echo "No plugin specified, ignore all plugins..."
    fi
}

checkPID(){
    PID=$1
    if kill -0 $PID > /dev/null 2>&1
    then
	echo "PID: $PID is running, stopping it..."
	kill $PID
	echo "PID: $PID stopped"
	sleep 5s
    fi
}

startDaemon(){
    #检查daemon是否启动
    if [ -f $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids/daemon.pid ];then
        checkPID `cat $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids/daemon.pid`
    fi
    pushd $WORKER_DEST_DIR/worker
    bin/start-daemon.sh
    sleep 2s
    daemonPid="$WORKER_DEST_DIR/worker/pids/daemon.pid"
    if [ -f ${daemonPid} ]
    then
        PID=`cat $WORKER_DEST_DIR/worker/pids/daemon.pid`
        ps aux|awk -F" " '{print $2}'|grep $PID
        REV=$?
        popd
        echo "REV=$REV"
        return $REV
    else
        return 1
    fi
}

installMaster(){
    if [ ! -d $MASTER_DEST_DIR ]
    then
	mkdir -p $MASTER_DEST_DIR
    fi

    if [ -d $MASTER_DEST_DIR/master ]
    then
	if [ "X$UPGRADE" = "X1" ]
	then
	    if [ -f $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/pids/master.pid ]
        then
        checkPID `cat $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/pids/master.pid`
        fi

	    echo "Launch master upgrade process..."
	    mv $MASTER_DEST_DIR/$MASTER_PKG_FOLDER $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back
	else
	    echo "Detination folder already exists, if you want upgrade, using -U or -u option."
	    echo "Exit without change..."
	    exit -1
	fi
    fi

    if [ "X$UPGRADE" = "X1" ]
    then
	echo "Upgrade master database..."
	cp -Rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back/db $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/ 2>/dev/null
	cp -Rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back/conf/[!ui]* $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/conf/ 2>/dev/null
	cp -Rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back/logs $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/ 2>/dev/null
	cp -Rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back/pids $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/ 2>/dev/null
	echo "Remove the backup master file..."
	rm -rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back 2>/dev/null
    else
        rm -rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER.back 2>/dev/null
        rm -rf $MASTER_DEST_DIR/master 2>/dev/null
        ln -s $MASTER_DEST_DIR/$MASTER_PKG_FOLDER $MASTER_DEST_DIR/master
        sed -i $SEDOPT "s/akka.remote.netty.tcp.hostname *= *127.0.0.1/akka.remote.netty.tcp.hostname = $MASTER_HOST_INTERNAL/g" $MASTER_DEST_DIR/master/conf/master.conf
        sed -i $SEDOPT "s/akka.remote.netty.tcp.port *= *19091/akka.remote.netty.tcp.port = $MASTER_PORT_INTERNAL/g" $MASTER_DEST_DIR/master/conf/master.conf
	    TMPSTR=${MASTER_CLUSTER_SEED_NODES//\//\\\/}
	    sed -i $SEDOPT "s/akka.cluster.seed-nodes *= *\[\"akka.tcp:\/\/master@127.0.0.1:19091\"\]/akka.cluster.seed-nodes = $TMPSTR/g" $MASTER_DEST_DIR/master/conf/master.conf
        sed -i $SEDOPT "s/master.web.http.host *= *0.0.0.0/master.web.http.host = $MASTER_WEB_HOST/g" $MASTER_DEST_DIR/master/conf/master.conf
        sed -i $SEDOPT "s/master.web.http.port *= *19090/master.web.http.port = $MASTER_WEB_PORT/g" $MASTER_DEST_DIR/master/conf/master.conf
        if [ "X$AUTH_ENABLED" = "X1" ]
        then
            sed -i $SEDOPT "s/master.web.auth.enable *= *off/master.web.auth.enable = on/g" $MASTER_DEST_DIR/master/conf/master.conf
            sed -i $SEDOPT "s/var auth *= *\!1/var auth=$AUTH_ENABLED/g" $MASTER_DEST_DIR/master/conf/ui/scripts/app.min.js
        else
            sed -i $SEDOPT "s/master.web.auth.enable *= *off/master.web.auth.enable = off/g" $MASTER_DEST_DIR/master/conf/master.conf
            sed -i $SEDOPT "s/var auth *= *\!1/var auth=$AUTH_ENABLED/g" $MASTER_DEST_DIR/master/conf/ui/scripts/app.min.js
        fi
        sed -i $SEDOPT "s/master.web.auth.host *= *127.0.0.1/master.web.auth.host = $AUTH_HOST/g" $MASTER_DEST_DIR/master/conf/master.conf
        sed -i $SEDOPT "s/master.web.auth.port *= *9000/master.web.auth.port = $AUTH_PORT/g" $MASTER_DEST_DIR/master/conf/master.conf
    fi
}

startMaster(){
    pushd $MASTER_DEST_DIR/master
    bin/start-master.sh
    sleep 2s
    masterPid="$MASTER_DEST_DIR/master/pids/master.pid"
    if [ -f ${masterPid} ]
    then
        PID=`cat $MASTER_DEST_DIR/master/pids/master.pid`
        ps aux|awk -F" " '{print $2}'|grep $PID
        REV=$?
        popd
        echo "REV=$REV"
        return $REV
    else
        return 1
    fi
}

installWorker(){
    if [ ! -d $WORKER_DEST_DIR ]
    then
	mkdir -p $WORKER_DEST_DIR
    fi

    if [ -d $WORKER_DEST_DIR/worker ]
    then
	if [ "X$UPGRADE" = "X1" ]
	then
        if [ -f $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids/worker.pid ]
        then
        checkPID `cat $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids/worker.pid`
        fi

	    echo "Launch worker upgrade process..."
	    mv $WORKER_DEST_DIR/$WORKER_PKG_FOLDER $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back
	else
	    echo "Detination folder already exists, if you want to upgrade, using -U or -u option."
	    echo "Exit without change..."
	    exit -1
	fi
    fi

    if [ "X$UPGRADE" = "X1" ]
    then
	echo "Upgrade worker database..."
	cp -Rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back/db $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/ 2>/dev/null
	cp -Rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back/conf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/ 2>/dev/null
	cp -Rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back/logs $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/ 2>/dev/null
	cp -Rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back/pids $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/ 2>/dev/null
	echo "Remove the backup worker file..."
	rm -rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back 2>/dev/null
    else
	rm -rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER.back 2>/dev/null
	rm -rf $WORKER_DEST_DIR/worker 2>/dev/null
        ln -s $WORKER_DEST_DIR/$WORKER_PKG_FOLDER $WORKER_DEST_DIR/worker
        sed -i $SEDOPT "s/akka.remote.netty.tcp.hostname *= *127.0.0.1/akka.remote.netty.tcp.hostname = $WORKER_HOST/g" $WORKER_DEST_DIR/worker/conf/worker.conf
        sed -i $SEDOPT "s/akka.remote.netty.tcp.port *= *19093/akka.remote.netty.tcp.port = $WORKER_PORT/g" $WORKER_DEST_DIR/worker/conf/worker.conf
        sed -i $SEDOPT "s/worker.id *= *#workerid#/worker.id = $WORKER_ID/g" $WORKER_DEST_DIR/worker/conf/worker.conf
	    TMPSTR=${MASTER_CLUSTER_SEED_NODES//\//\\\/}
	    sed -i $SEDOPT "s/worker.remote *= *\[\"akka.tcp:\/\/master@127.0.0.1:19091\"\]/worker.remote = $TMPSTR/g" $WORKER_DEST_DIR/worker/conf/worker.conf
        sed -i $SEDOPT "s/worker.daemon.host *= *127.0.0.1/worker.daemon.host = $WORKER_HOST/g" $WORKER_DEST_DIR/worker/conf/worker.conf
        sed -i $SEDOPT "s/worker.daemon.port *= *19095/worker.daemon.port = $DAEMON_PORT/g" $WORKER_DEST_DIR/worker/conf/worker.conf
        sed -i $SEDOPT "s/worker.daemon.enable *= *on/worker.daemon.enable = $DAEMON_ENABLED/g" $WORKER_DEST_DIR/worker/conf/worker.conf

	    #install worker will also install and config daemon
        sed -i $SEDOPT "s/akka.remote.netty.tcp.hostname *= *127.0.0.1/akka.remote.netty.tcp.hostname = $WORKER_HOST/g" $WORKER_DEST_DIR/worker/conf/daemon.conf
        sed -i $SEDOPT "s/akka.remote.netty.tcp.port *= *19095/akka.remote.netty.tcp.port = $DAEMON_PORT/g" $WORKER_DEST_DIR/worker/conf/daemon.conf
    fi
}


startWorker(){
    pushd $WORKER_DEST_DIR/worker
    bin/start-worker.sh
    sleep 2s
    workPid="$WORKER_DEST_DIR/worker/pids/worker.pid"
    if [ -f ${workPid} ]
    then
        PID=`cat $WORKER_DEST_DIR/worker/pids/worker.pid`
        ps aux|awk -F" " '{print $2}'|grep $PID
        REV=$?
        popd
        echo "REV=$REV"
        return $REV
    else
        return 1
    fi
}

resetDB(){
    sed -i $SEDOPT "s/#ES_CLUSTER#/$ES_CLUSTER/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#ES_HOSTPORTS#/$ES_HOSTPORTS/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#SYSLOG_HOST#/$SYSLOG_HOST/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#SYSLOG_PORT#/$SYSLOG_PORT/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#WRITER_KAFKA_HOSTPORTS#/$WRITER_KAFKA_HOSTPORTS/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#WRITER_KAFKA_TOPIC#/$WRITER_KAFKA_TOPIC/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#DATASOURCE_KAFKA_HOSTPORTS#/$DATASOURCE_KAFKA_HOSTPORTS/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#DATASOURCE_KAFKA_TOPIC#/$DATASOURCE_KAFKA_TOPIC/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#OMDB_HOST#/$OMDB_HOST/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#OMDB_PORT#/$OMDB_PORT/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#OMDB_SCHEMA#/$OMDB_SCHEMA/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#OMDB_USER#/$OMDB_USER/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#OMDB_PWD#/$OMDB_PWD/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#FLINK_HOSTPORTS#/$FLINK_HOSTPORTS/g" $MASTER_DEST_DIR/master/bin/insert.sql
    TMPSTR=${MASTER_DEST_DIR//\//\\\/}
    sed -i $SEDOPT "s/#MASTER_DATASOURCE#/$TMPSTR\/master\/logs\/master-.*\.log/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#MASTER_PARSER_WRITER#/$TMPSTR\/master\/logs\/master-parser.log/g" $MASTER_DEST_DIR/master/bin/insert.sql
    sed -i $SEDOPT "s/#MASTER_ANALYZER_WRITER#/$TMPSTR\/master\/logs\/master-analyzer.log/g" $MASTER_DEST_DIR/master/bin/insert.sql

    pushd $MASTER_DEST_DIR/master
    bin/reset_db.sh
    sleep 2s
    REV=0
    popd
    echo "REV=$REV"
    return $REV
}

if [ "X"$MASTER_INSTALL = "X1" ]
then
    echo "installing master..."
    installMaster
    if [ $? -ne 0 ]
    then
    echo "Master install failed, please check your environment."
    exit -1
    fi

    if [ "X"$RESET_DB = "X1" ]
    then
        echo "reseting db..."
        resetDB
        if [ $? -ne 0 ]
        then
        echo "reset db failed, please check your environment."
        exit -1
        fi
    fi
fi

if [ "X"$WORKER_INSTALL = "X1" ]
then
    echo "installing worker..."
    installWorker
    if [ $? -ne 0 ]
    then
	echo "Worker install failed, please check your environment."
	exit -1
    fi
fi

installPlugins

if [ $? -eq 0 ]
then
    echo "Proceed installation successful!"
fi

if [ "X"$RUNNING_AFTER_INSTALL = "X1" ]
then
    if [ "X"$MASTER_INSTALL = "X1" ]
    then
	echo "Prepare to start Master..."
	startMaster
	if [ $? -eq 0 ]
	then
	    echo "Master started!"
	else
	    echo "Error occured when starting Master, check log."
	    exit -1
	fi
    fi

    if [ "X"$WORKER_INSTALL = "X1" ]
    then
	echo "Prepare to start Worker..."
	startWorker
	if [ $? -eq 0 ]
	then
	    echo "Worker started!"
	else
	    echo "Error occured when starting Worker, check log."
	    exit -1
	fi
	if [ $DAEMON_ENABLED = "on" ]
    then
	echo "Prepare to start Daemon..."
	startDaemon
	if [ $? -eq 0 ]
	then
	    echo "Daemon started!"
	else
	    echo "Error occured when starting Daemon, check log."
	    exit -1
	fi
    fi
    fi

    echo "All task done, enjoy!"
fi

