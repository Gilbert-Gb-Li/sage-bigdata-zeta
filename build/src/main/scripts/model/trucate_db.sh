#!/bin/sh
home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

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

export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH

derby="${home}/db/bin/ij"
cd $home/bin;
$derby <trucate.sql;