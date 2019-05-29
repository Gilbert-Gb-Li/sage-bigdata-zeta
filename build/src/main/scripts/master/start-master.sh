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
    #!/bin/base

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=${home//\//\\/}
    grep -E "^\s*master\.web\.auth\.enable\s*=\s*(on)\s*" $home/conf/master.conf
    if [ $? == 0 ];
    then
      sed -i '' "s/var\s\+auth\s*=\s*\(\!1\|\!0\|1\|0\)/var auth=\!0/g" $home/conf/ui/scripts/app.min.js
    else
      sed -i '' "s/var\s\+auth\s*=\s*\(\!1\|\!0\|1\|0\)/var auth=\!1/g" $home/conf/ui/scripts/app.min.js
    fi
    echo "Starting on Darwin OS..."
    sed -i '' "s/\.\//$real\//g" $home/conf/master.conf
    sed -i '' "s/>logs/>$real\/logs/g" $home/conf/logback-master.xml

    $home/bin/sage-bigdata-zeta.sh start master
elif $aix || $hpux; then
    #!/bin/ksh

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=`echo $home | sed "s/\//\\\\\\\\\//g"`
    grep -E "^\s*master\.web\.auth\.enable\s*=\s*(on)\s*" $home/conf/master.conf
    if [ $? == 0 ];
    then
      perl -pi -e "s/var\s\+auth\s*=\s*\(\!1\|\!0\|1\|0\)/var auth=\!0/g" $home/conf/ui/scripts/app.min.js
    else
      perl -pi -e "s/var\s\+auth\s*=\s*\(\!1\|\!0\|1\|0\)/var auth=\!1/g" $home/conf/ui/scripts/app.min.js
    fi
    echo "Starting on AIX or HP-UX OS..."
    perl -pi -e "s/\.\//$real\//g" $home/conf/master.conf
    perl -pi -e "s/>logs/>$real\/logs/g" $home/conf/logback-master.xml

    ksh $home/bin/sage-bigdata-etl-kshell.sh start master
else
    #!/bin/bash

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=${home//\//\\/}
    grep -E "^\s*master\.web\.auth\.enable\s*=\s*(on)\s*" $home/conf/master.conf
    if [ $? == 0 ];
    then
      sed -i  "s/var\s\+auth\s*=\s*\(\!1\|\!0\|1\|0\)/var auth=\!0/g" $home/conf/ui/scripts/app.min.js
    else
      sed -i  "s/var\s\+auth\s*=\s*\(\!1\|\!0\|1\|0\)/var auth=\!1/g" $home/conf/ui/scripts/app.min.js
    fi
    echo "Starting on *nix OS..."
    sed -i  "s/\.\//$real\//g" $home/conf/master.conf
    sed -i  "s/>logs/>$real\/logs/g" $home/conf/logback-master.xml

    $home/bin/sage-bigdata-zeta.sh start master
fi


