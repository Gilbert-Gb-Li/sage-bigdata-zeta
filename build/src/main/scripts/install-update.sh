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

    CWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
    echo $CWD_HOME

    $CWD_HOME/shell/preinstall-update.sh
elif $aix || $hpux; then
    #!/bin/ksh
    CWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
    echo $CWD_HOME

    ksh $CWD_HOME/shell/preinstall-kshell-update.sh
else
    #!/bin/base

    CWD_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/" && pwd )"
    echo $CWD_HOME

    $CWD_HOME/shell/preinstall-update.sh
fi

