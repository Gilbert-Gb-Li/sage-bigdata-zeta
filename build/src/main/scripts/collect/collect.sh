#!/bin/sh

HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
# OS specific support.  $var _must_ be set to either true or false.
echo "HOME: "$HOME

# create debug dir
if [ -d "$HOME/collect" ]
then
    echo "delete collect info..."
    rm -rf $HOME/collect
    echo "create collect info..."
    mkdir $HOME/collect
else
    echo "create collect info..."
    mkdir $HOME/collect
fi

# copy master db
if [ -d "$HOME/db/master" ]
then
    echo "copy master db..."
    cp -R $HOME/db/master $HOME/collect/
fi

# copy worker db
if [ -d "$HOME/db/worker" ]
then
    echo "copy worker db..."
    cp -R $HOME/db/worker $HOME/collect/
fi

# copy logs
if [ -d "$HOME/logs" ]
then
    echo "copy logs..."
    cp -R $HOME/logs $HOME/collect/
fi

# collect system info
echo "collect system info..."
echo "## hostname" >> $HOME/collect/system.log
hostname >> $HOME/collect/system.log
echo "## system version" >> $HOME/collect/system.log
uname -a >> $HOME/collect/system.log
head -n 1 /etc/issue >> $HOME/collect/system.log
echo "## 系统运行时间、用户数、负载" >> $HOME/collect/system.log
uptime >> $HOME/collect/system.log

# collect user info
echo "collect user info..."
echo "## 当前登录用户信息" >> $HOME/collect/user.log
users >> $HOME/collect/user.log
echo "## 全部活动用户信息" >> $HOME/collect/user.log
w >> $HOME/collect/user.log
echo "## 全部系统用户信息" >> $HOME/collect/user.log
cut -d: -f1 /etc/passwd >> $HOME/collect/user.log
echo "## 全部系统用户组信息" >> $HOME/collect/user.log
cut -d: -f1 /etc/group >> $HOME/collect/user.log


# collect cpu info
echo "collect cpu info..."
echo "## cpu统计信息" >> $HOME/collect/cpu.log
lscpu >> $HOME/collect/cpu.log
echo "## cpu详细信息" >> $HOME/collect/cpu.log
cat /proc/cpuinfo >> $HOME/collect/cpu.log

# collect mem info
echo "collect mem info..."
echo "## 空闲内存信息" >> $HOME/collect/mem.log
free -m >> $HOME/collect/mem.log
echo "## 内存详细信息" >> $HOME/collect/mem.log
cat /proc/meminfo >> $HOME/collect/mem.log

# collect disk info
echo "collect disk info..."
echo "## 磁盘分区使用率" >> $HOME/collect/disk.log
df -lh >> $HOME/collect/disk.log
echo "## 系统负载磁盘和分区" >> $HOME/collect/disk.log
cat /proc/loadavg >> $HOME/collect/disk.log

# collect Ethernet info
# echo "collect Ethernet info..."
# echo "## 网卡信息" >> $HOME/collect/ethernet.log
# ifconfig -a >> $HOME/collect/ethernet.log
# echo "## 路由表信息" >> $HOME/collect/ethernet.log
# route -n >> $HOME/collect/ethernet.log

# collect firewall info
# echo "collect firewall info..."
# echo "## iptables防火墙状态" >> $HOME/collect/firewall.log
# service iptables status >> $HOME/collect/firewall.log
# echo "## iptables防火墙策略" >> $HOME/collect/firewall.log
# iptables -L >> $HOME/collect/firewall.log
# echo "## firewalld防火墙状态" >> $HOME/collect/firewall.log
# service firewalld status >> $HOME/collect/firewall.log
# echo "## iptables防火墙策略" >> $HOME/collect/firewall.log
# firewall-cmd --list-all >> $HOME/collect/firewall.log

#collect hosts info
echo "collect hosts info..."
echo "## Hosts信息" >> $HOME/collect/hosts.log
cat /etc/hosts >> $HOME/collect/hosts.log

# collect dmi info
# echo "collect dmi info..."
# echo "## DMI详细信息" >> $HOME/collect/dmi.log
# dmidecode -q >> $HOME/collect/dmi.log

# collect env info
echo "collect env info..."
echo "## 环境变量信息" >> $HOME/collect/env.log
env >> $HOME/collect/env.log

# collect process info
echo "collect process info..."
echo "## Process信息" >> $HOME/collect/process.log
ps aux >> $HOME/collect/process.log

# collect netstat info
echo "collect netstat info..."
echo "## 所有监听端口" >> $HOME/collect/netstat.log
netstat -lntpue >> $HOME/collect/netstat.log
echo "## 所有已经建立的连接端口" >> $HOME/collect/netstat.log
netstat -antpue >> $HOME/collect/netstat.log
echo "## 按各个协议进行统计" >> $HOME/collect/netstat.log
netstat -s >> $HOME/collect/netstat.log

# collect ulimit info
echo "collect ulimit info..."
echo "## Limit信息" >> $HOME/collect/limit.log
ulimit -a >> $HOME/collect/limit.log

# collect chkconfig info
echo "collect chkconfig info..."
# echo "## 所有系统服务" >> $HOME/collect/chkconfig.log
# chkconfig --list >> $HOME/collect/chkconfig.log

# collect java info
echo "collect java info..."
echo "## JAVA_HOME" >> $HOME/collect/java.log
echo $JAVA_HOME >> $HOME/collect/java.log
echo "## java version" >> $HOME/collect/java.log
JAVA_VER=`java -version 2>&1 >/dev/null`
echo $JAVA_VER >> $HOME/collect/java.log

#collect top info
echo "collect top info..."
echo "## 实时进程状态" >> $HOME/collect/top.log
top -n 1 >> $HOME/collect/top.log

echo "create tar.gz file..."
tar cvf $HOME/collect.tar $HOME/collect --remove-files
gzip -f $HOME/collect.tar>$HOME/collect.tar.gz

echo
echo
echo
echo "collect.tar.gz file path: "$HOME/collect.tar.gz
echo "Information collection completed"