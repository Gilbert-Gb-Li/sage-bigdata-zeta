#!/bin/bash
PATH="../data"
HOST_PORT="127.0.0.1:19090"

## -f <setting.env> file
## -U or -u, app upgrade
home="$( cd "$( /usr/bin/dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd ${home}/bin;
while [ -n "$1" ]
do
    case "$1" in
	-h) HOST_PORT=$2; shift 2;;
	-d) PATH=$2; shift 2;;
	*) echo "Error with unknown parameters!"; exit 1;;
    esac
done
if [ ! -d "$PATH" ]
then
    /bin/mkdir $PATH;
fi
/usr/bin/curl -XGET ${HOST_PORT}/assettype?pretty > $PATH/assettypes.json ;
/usr/bin/curl -XGET ${HOST_PORT}/writer?pretty > $PATH/writers.json ;
/usr/bin/curl -XGET ${HOST_PORT}/resolver?pretty > $PATH/resolvers.json ;
/usr/bin/curl -XGET ${HOST_PORT}/datasource?pretty > $PATH/datasources.json ;
echo "download finished";