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

    echo "Starting on Darwin OS..."
    sed -i '' "s/\.\//$real\//g" $home/conf/auth.conf
    sed -i '' "s/>logs/>$real\/logs/g" $home/conf/logback-auth.xml

    $home/bin/sage-bigdata-zeta.sh start auth
elif $aix || $hpux; then
    #!/bin/ksh

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=`echo $home | sed "s/\//\\\\\\\\\//g"`

    echo "Starting on AIX or HP-UX OS..."
    perl -pi -e "s/\.\//$real\//g" $home/conf/auth.conf
    perl -pi -e "s/>logs/>$real\/logs/g" $home/conf/logback-auth.xml

    ksh $home/bin/sage-bigdata-etl-kshell.sh start auth
else
    #!/bin/base

    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    real=${home//\//\\/}

    echo "Starting on *nix OS..."
    sed -i  "s/\.\//$real\//g" $home/conf/auth.conf
    sed -i  "s/>logs/>$real\/logs/g" $home/conf/logback-auth.xml

    $home/bin/sage-bigdata-zeta.sh start auth
fi
