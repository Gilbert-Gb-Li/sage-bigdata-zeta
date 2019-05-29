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
    BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    #echo $BASE_DIR
    SAGE_HOME="$(dirname $BASE_DIR)"
    echo copy all jars from $BASE_DIR/lib/ to $SAGE_HOME/lib/
    cp $BASE_DIR/lib/*.jar $SAGE_HOME/lib/
    echo copy confs from $BASE_DIR/conf/ to $SAGE_HOME/conf/

    cp $BASE_DIR/conf/* $SAGE_HOME/conf/
    echo install successed
elif $aix || $hpux; then
    #!/bin/ksh
    BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    #echo $BASE_DIR
    SAGE_HOME="$(dirname $BASE_DIR)"
    echo copy all jars from $BASE_DIR/lib/ to $SAGE_HOME/lib/
    cp $BASE_DIR/lib/*.jar $SAGE_HOME/lib/
    echo copy confs from $BASE_DIR/conf/ to $SAGE_HOME/conf/

    cp $BASE_DIR/conf/* $SAGE_HOME/conf/
    echo install successed
else
    #!/bin/base
    BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    #echo $BASE_DIR
    SAGE_HOME="$(dirname $BASE_DIR)"
    echo copy all jars from $BASE_DIR/lib/ to $SAGE_HOME/lib/
    cp $BASE_DIR/lib/*.jar $SAGE_HOME/lib/
    echo copy confs from $BASE_DIR/conf/ to $SAGE_HOME/conf/

    cp $BASE_DIR/conf/* $SAGE_HOME/conf/
    echo install successed
fi


