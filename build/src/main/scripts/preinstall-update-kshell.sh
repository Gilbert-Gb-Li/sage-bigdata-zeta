#!/bin/ksh

PWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
echo $PWD_HOME

CWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo $CWD_HOME

source $CWD_HOME/setting-update.env



sage-bigdata-etl_PKG=`ls $CWD_HOME/sage-bigdata-etl-*.tar.gz`
sage-bigdata-etl_BN=`basename $sage-bigdata-etl_PKG`
sage-bigdata-etl_PKG_FOLDER=`echo $sage-bigdata-etl_BN | sed "s/.tar.gz//g"`
SYS_VERSION=`echo $sage-bigdata-etl_PKG_FOLDER|awk -F"-" '{print $2}'`

case $SYS_VERSION in
(*nix*) sage-bigdata-etl_VERSION=`echo $sage-bigdata-etl_PKG_FOLDER | sed "s/sage-bigdata-etl-nix-//g"`;;
(*aix*) sage-bigdata-etl_VERSION=`echo $sage-bigdata-etl_PKG_FOLDER | sed "s/sage-bigdata-etl-aix-//g"`;;
(*hpux*) sage-bigdata-etl_VERSION=`echo $sage-bigdata-etl_PKG_FOLDER | sed "s/sage-bigdata-etl-hpux-//g"`;;
esac

sage-bigdata-etl_PKG_TAR=`echo $sage-bigdata-etl_BN | sed "s/.gz//g"`
mkdir $TOP_INSTALL_DIR 2>/dev/null
chmod 755 $TOP_INSTALL_DIR
gunzip -c $sage-bigdata-etl_PKG > $TOP_INSTALL_DIR/$sage-bigdata-etl_PKG_TAR
cd $TOP_INSTALL_DIR
tar -xvf $TOP_INSTALL_DIR/$sage-bigdata-etl_PKG_TAR

echo "sage-bigdata-etl package: $sage-bigdata-etl_PKG"


if [ "X"$MASTER_UPDATE = "X1" ]
then
    MASTER_DEST_DIR=$TOP_INSTALL_DIR
    MASTER_PKG_FOLDER=$sage-bigdata-etl_PKG_FOLDER
fi

if [ "X"$WORKER_UPDATE = "X1" ]
then
    WORKER_DEST_DIR=$TOP_INSTALL_DIR
    WORKER_PKG_FOLDER=$sage-bigdata-etl_PKG_FOLDER
fi

function handlePlugin {
    PLUGIN_PKG=$1
    echo "$PLUGIN_PKG"
    TEMPDIR=`dirname $PLUGIN_PKG`
    ZIPFILE=`basename $PLUGIN_PKG`
    PLUGIN_PKG_FOLDER=`echo $ZIPFILE | sed "s/.tar.gz//g"`
    PLUGIN_PKG_TAR=`echo $ZIPFILE | sed "s/.gz//g"`
    gunzip -c $PLUGIN_PKG > $TEMPDIR/$PLUGIN_PKG_TAR
    cd $TEMPDIR
    tar -xvf $TEMPDIR/$PLUGIN_PKG_TAR
    rm -r $TEMPDIR/$PLUGIN_PKG_TAR

    cp $TEMPDIR/$PLUGIN_PKG_FOLDER/lib/*.jar $WORKER_DEST_DIR/worker/lib/ 2>/dev/null
    cp $TEMPDIR/$PLUGIN_PKG_FOLDER/conf/*.conf $WORKER_DEST_DIR/worker/conf/ 2>/dev/null
    rm -rf $TEMPDIR/$PLUGIN_PKG_FOLDER
}

function installPlugins {
    if [ -d "$CWD_HOME/plugin" ]
    then
    VERSION_PLUGIN=$sage-bigdata-etl_VERSION
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
        WORKER_PLUGINS_TMP=`echo $WORKER_PLUGINS | sed "s/,/ /g"`
        set -A arr $WORKER_PLUGINS_TMP
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

function checkPID {
    PID=$1
    if kill -0 $PID > /dev/null 2>&1
    then
	echo "PID: $PID is running, stopping it..."
	kill $PID
	echo "PID: $PID stopped"
	sleep 5
    fi
}

function startDaemon {
    #检查daemon是否启动
    if [ -f $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids/daemon.pid ];then
        checkPID `cat $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids/daemon.pid`
    fi
    cd $WORKER_DEST_DIR/worker/bin
    ksh start-daemon.sh
    sleep 2
    PID=`cat $WORKER_DEST_DIR/worker/pids/daemon.pid`
    ps aux|awk -F" " '{print $2}'|grep $PID
    REV=$?
    cd -
    echo "REV=$REV"
    return $REV
}

function updateMaster {
    if [ -d $MASTER_DEST_DIR ]
    then
	    if [ -d $MASTER_DEST_DIR/master ]
	    then
	        checkPID `cat $MASTER_DEST_DIR/master/pids/master.pid`
	        echo "Launch master upgrade process..."
            rm -rf $MASTER_DEST_DIR/master
            ln -s $MASTER_DEST_DIR/$MASTER_PKG_FOLDER $MASTER_DEST_DIR/master
            rm -rf $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/conf/master*
            mkdir -p $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/db && cp -Rf $MASTER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/db/master "$_" 2>/dev/null
            mkdir -p $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/conf && cp -Rf $MASTER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/conf/master* "$_" 2>/dev/null
            mkdir -p $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/logs && cp -Rf $MASTER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/logs/master* "$_" 2>/dev/null
            mkdir -p $MASTER_DEST_DIR/$MASTER_PKG_FOLDER/pids && cp -Rf $MASTER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/pids/master.pid "$_" 2>/dev/null
            if [ "X$AUTH_ENABLED" != "X1" ]
            then
                perl -pi -e "s/master.web.auth.enable *= *off/master.web.auth.enable = off/g" $MASTER_DEST_DIR/master/conf/master.conf
                perl -pi -e "s/var auth *= *\!1/var auth=\!$AUTH_ENABLED/g" $MASTER_DEST_DIR/master/conf/ui/scripts/app.min.js
                perl -pi -e "s/master.web.auth.enable *= *on/master.web.auth.enable = off/g" $MASTER_DEST_DIR/master/conf/master.conf
                perl -pi -e "s/var auth *= *\!0/var auth=\!$AUTH_ENABLED/g" $MASTER_DEST_DIR/master/conf/ui/scripts/app.min.js
            else
                perl -pi -e "s/master.web.auth.enable *= *off/master.web.auth.enable = on/g" $MASTER_DEST_DIR/master/conf/master.conf
                perl -pi -e "s/var auth *= *\!1/var auth=\!$AUTH_ENABLED/g" $MASTER_DEST_DIR/master/conf/ui/scripts/app.min.js
                perl -pi -e "s/master.web.auth.enable *= *on/master.web.auth.enable = on/g" $MASTER_DEST_DIR/master/conf/master.conf
                perl -pi -e "s/var auth *= *\!0/var auth=\!$AUTH_ENABLED/g" $MASTER_DEST_DIR/master/conf/ui/scripts/app.min.js
            fi
	    else
	        echo $MASTER_DEST_DIR"/master non-existent"
	    fi
	else
        echo $MASTER_DEST_DIR" non-existent"
    fi
}

function startMaster {
    cd $MASTER_DEST_DIR/master/bin
    ksh start-master.sh
    sleep 2
    PID=`cat $MASTER_DEST_DIR/master/pids/master.pid`
    ps aux|awk -F" " '{print $2}'|grep $PID
    REV=$?
    cd -
    echo "REV=$REV"
    return $REV
}

function updateWorker {
    if [ -d $WORKER_DEST_DIR ]
    then
	    if [ -d $WORKER_DEST_DIR/worker ]
	    then
	        checkPID `cat $WORKER_DEST_DIR/worker/pids/daemon.pid`
	        checkPID `cat $WORKER_DEST_DIR/worker/pids/worker.pid`
	        echo "Launch worker upgrade process..."
            rm -rf $WORKER_DEST_DIR/worker
            ln -s $WORKER_DEST_DIR/$WORKER_PKG_FOLDER $WORKER_DEST_DIR/worker

            rm -rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/conf/daemon*
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/conf && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/conf/daemon* "$_" 2>/dev/null
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/logs && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/logs/daemon* "$_" 2>/dev/null
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/pids/daemon.pid "$_" 2>/dev/null

            rm -rf $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/conf/worker*
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/db && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/db/worker "$_" 2>/dev/null
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/conf && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/conf/worker* "$_" 2>/dev/null
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/logs && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/logs/worker* "$_" 2>/dev/null
            mkdir -p $WORKER_DEST_DIR/$WORKER_PKG_FOLDER/pids && cp -Rf $WORKER_DEST_DIR/sage-bigdata-etl-nix-$OLD_VERSION/pids/worker.pid "$_" 2>/dev/null
	    else
	        echo $WORKER_DEST_DIR"/worker non-existent"
	    fi
	else
        echo $WORKER_DEST_DIR" non-existent"
    fi
}


function startWorker {
    cd $WORKER_DEST_DIR/worker/bin
    ksh start-worker.sh
    sleep 2
    PID=`cat $WORKER_DEST_DIR/worker/pids/worker.pid`
    ps aux|awk -F" " '{print $2}'|grep $PID
    REV=$?
    cd -
    echo "REV=$REV"
    return $REV
}

if [ "X"$MASTER_UPDATE = "X1" ]
then
    echo "updateing master..."
    updateMaster
    if [ $? -ne 0 ]
    then
    echo "Master update failed, please check your environment."
    exit -1
    fi
fi

if [ "X"$WORKER_UPDATE = "X1" ]
then
    echo "updateing worker..."
    updateWorker
    if [ $? -ne 0 ]
    then
	echo "Worker update failed, please check your environment."
	exit -1
    fi
fi

installPlugins

if [ $? -eq 0 ]
then
    echo "Proceed installation successful!"
fi

if [ "X"$RUNNING_AFTER_UPDATE = "X1" ]
then
    if [ "X"$MASTER_UPDATE = "X1" ]
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

    if [ "X"$WORKER_UPDATE = "X1" ]
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

        startDaemon
        if [ $? -eq 0 ]
        then
            echo "Daemon started!"
        else
            echo "Error occured when starting Daemon, check log."
            exit -1
        fi
    fi

    echo "All task done, enjoy!"
fi

