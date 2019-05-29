#!/bin/bash
PATH="../data"
HOST_PORT="127.0.0.1:19090"
home="$( cd "$( /usr/bin/dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd ${home}/bin;


## -f <setting.env> file
## -U or -u, app upgrade
while [ -n "$1" ]
do
    case "$1" in
	-h) HOST_PORT=$2; shift 2;;
	-d) PATH=$2; shift 2;;
	*) echo "Error with unknown parameters!"; exit 1;;
    esac
done
if [ -d "$PATH" ]
then
	echo "upload data form[$PATH] to ${HOST_PORT} "
    if [ -f "$PATH/assettypes.json" ]
    then
        /usr/bin/curl -H 'Content-Encoding:UTF-8' -H 'Content-Type:application/json;charset=UTF-8' -H 'Accept-Charset:UTF-8' -XPOST ${HOST_PORT}/assettype/bulk -d @$PATH/assettypes.json;
    else
	    echo "$PATH/assettypes.json not found."
	    exit 1;
    fi
     if [ -f "$PATH/writers.json" ]
    then
        /usr/bin/curl -H 'Content-Encoding:UTF-8' -H 'Content-Type:application/json;charset=UTF-8' -H 'Accept-Charset:UTF-8' -XPOST ${HOST_PORT}/writer/bulk -d @$PATH/writers.json
      echo "";
      else
     	echo "$PATH/writers.json not found."
	    exit 1;

     fi
     if [ -f "$PATH/resolvers.json" ]
    then
        /usr/bin/curl -H 'Content-Encoding:UTF-8' -H 'Content-Type:application/json;charset=UTF-8' -H 'Accept-Charset:UTF-8' -XPOST ${HOST_PORT}/resolver/bulk -d @$PATH/resolvers.json
    echo "";
     else
	    echo "$PATH/resolvers.json not found."
	    exit 1;

     fi
     if [ -f "$PATH/datasources.json" ]
    then
        /usr/bin/curl -H 'Content-Encoding:UTF-8' -H 'Content-Type:application/json;charset=UTF-8' -H 'Accept-Charset:UTF-8' -XPOST ${HOST_PORT}/datasource/bulk -d @$PATH/datasources.json
    echo "";
    else
	    echo "$PATH/datasources.json not found."
	    exit 1;

    fi
    echo "upload finished";
else
	echo "$PATH not found."
fi
