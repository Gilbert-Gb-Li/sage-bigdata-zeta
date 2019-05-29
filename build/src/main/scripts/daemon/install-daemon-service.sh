#!/bin/bash

    if [ -n ${JAVA_HOME} ];then
        echo $JAVA_HOME
    else
        echo 'env JAVA_HOME NOT Found,please  set it first'
        exit 1;
    fi
    if [  `command -v jsvc` ];then
        JSVC=`which jsvc`
        #echo "server[jsvc] in ${JSVC} " >&2
    else
        echo "please install server[jsvc] first ,will use default files" >&2
    fi

    HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    echo $HOME

    #    pushd $HOME/bin/native/unix
    #    sh support/buildconf.sh
    #    sleep 2s
    #    ./configure
    #    sleep 2s
    #    ./make
    #    sleep 2s
    CYGWIN=false
    DARWIN=false
    case "`uname`" in
    CYGWIN*) CYGWIN=true;;
    Darwin*) DARWIN=true;;
    esac
    if $DARWIN; then
        echo "Install on Darwin OS..."
        if [ -n "${JSVC}" ];then
            echo $JSVC
        else
            JSVC=$HOME/bin/jsvc/1.0.15/macos/jsvc
        fi
        sed -i '' "s/JAVA_HOME_REPLACE/${JAVA_HOME//\//\\/}/g" $HOME/bin/sage-daemon-service
        sed -i '' "s/HOME_REPLACE/${HOME//\//\\/}\//g" $HOME/bin/sage-daemon-service
        sed -i '' "s/JSVC_REPLACE/${JSVC//\//\\/}/g" $HOME/bin/sage-daemon-service
        echo 'macos mock install is ok'
    else
        echo "Install on *nix OS..."
        if [ -n "${JSVC}" ];then
            echo $JSVC
        else
            JSVC=$HOME/bin/jsvc/1.0.15/unix/jsvc
        fi
        sed  -i "s/JAVA_HOME_REPLACE/${JAVA_HOME//\//\\/}/g" $HOME/bin/sage-daemon-service
        sed  -i "s/HOME_REPLACE/${HOME//\//\\/}\//g" $HOME/bin/sage-daemon-service
        sed  -i "s/JSVC_REPLACE/${JSVC//\//\\/}/g" $HOME/bin/sage-daemon-service
        if [ -f $HOME/bin/sage-daemon-service ]; then
            sudo mv $HOME/bin/sage-daemon-service /etc/init.d/
            echo 'install service[sage-daemon-service] done, enjoy!'
        else
            echo 'service[sage-daemon-service] had installed'
        fi
    fi




