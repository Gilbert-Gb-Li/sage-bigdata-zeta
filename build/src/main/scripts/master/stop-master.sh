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
    $home/bin/sage-bigdata-zeta.sh stop master
elif $aix || $hpux; then
    #!/bin/ksh
    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    ksh $home/bin/sage-bigdata-etl-kshell.sh stop master
else
    #!/bin/bash
    home="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
    $home/bin/sage-bigdata-zeta.sh stop master
fi
