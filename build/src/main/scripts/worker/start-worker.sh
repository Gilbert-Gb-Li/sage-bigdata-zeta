#!/bin/sh
# OS specific support.  $var _must_ be set to either true or false.
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

echo cygwin:$cygwin
echo darwin:$darwin
echo aix:$aix
echo hpux:$hpux
echo nix:$nix




if $darwin; then
    #!/bin/bash

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=${home//\//\\/}
    host_name=$(hostname)

    echo "Starting on Darwin OS..."
    sed -i '' "s/#workerid#/$host_name/g" $home/conf/worker.conf
    sed -i '' "s/\.\//$real\//g" $home/conf/worker.conf
    sed -i '' "s/>logs/>$real\/logs/g" $home/conf/logback-worker.xml

    $home/bin/sage-bigdata-zeta.sh start worker
elif $aix || $hpux; then
    #!/bin/ksh
    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=`echo $home | sed "s/\//\\\\\\\\\//g"`
    host_name=$(hostname)

    echo "Starting on AIX or HP-UX OS..."
    perl -pi -e "s/#workerid#/$host_name/g" $home/conf/worker.conf
    perl -pi -e "s/\.\//$real\//g" $home/conf/worker.conf
    perl -pi -e "s/>logs/>$real\/logs/g" $home/conf/logback-worker.xml

    ksh $home/bin/sage-bigdata-etl-kshell.sh start worker
else
  #!/bin/bash

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=${home//\//\\/}
    host_name=$(hostname)

    echo "Starting on *nix OS..."
    sed -i  "s/#workerid#/$host_name/g" $home/conf/worker.conf
    sed -i  "s/\.\//$real\//g" $home/conf/worker.conf
    sed -i  "s/>logs/>$real\/logs/g" $home/conf/logback-worker.xml

    $home/bin/sage-bigdata-zeta.sh start worker
fi

