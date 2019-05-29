[TOC]
#1. 说明


### 1.1 编写目的

​	用户实施手册编写目的是明确本软件的安装、部署与配置帮助用户能完全独立的安装本软件。

### 1.2 sage-bigdata-etl简介

sage-bigdata-etl由master(采集器控制中心)、worker(采集器)。

- a.  master完成整个系统的监控、转发、配置等工作；
- b.  worker主要完成数据的采集、解析、收集系统的metrics信息等工作；

### 1.3环境要求

​	centos6.5系统、jdk1.8.0+、内存2G+、硬盘50G+

2.安装配置
========

### 2.1 安装简介

- 解压sage-bigdata-etl安装包到想要安装的目录下
- sage-bigdata-etl包括sage-bigdata-etl-master和sage-bigdata-etl-worker两部分
- sage-bigdata-etl-master:数据源采集器,以下简称master
- sage-bigdata-etl-worker:采集器控制器,以下简称worker
- sage-bigdata-etl-master和sage-bigdata-etl-worker可分开部署亦可部署在同一机器上,二者之间通过心跳通信

### 2.2  配置

####2.2.1. 修改master配置
```shell
~ cd $sage-bigdata-etl_MASTER/conf
~ vim master.conf
```

```yaml
#修改“127.0.0.1”为master部署节点的ip
akka.remote.netty {
    tcp.hostname = 127.0.0.1
    tcp.port = 19091
}
akka.cluster.seed-nodes = ["akka.tcp://master@127.0.0.1:19091"]
```


####2.2.2. 修改worker配置

```shell
~ cd $sage-bigdata-etl_WORKER/conf
~ vim worker.conf
```

```yaml
#修改“127.0.0.1”为worker部署节点的ip
akka.remote.netty {
	tcp.hostname = "127.0.0.1"
	tcp.port = 19093
}

```

> ​	直接连接master 节点

	> ``` yaml 
	> # sage-bigdata-etl worker连接master配置 #
	> worker.connect-to = cluster
	> worker.remote = ["akka.tcp://master@127.0.0.1:19091"]
	> 
	> 
	> ```
	>
	> ​    通过中转节点连接
	>
	> ```yaml 
	> worker.connect-to= transfer
	> sage-bigdata-etl worker连接master配置 #
	> worker.remote = ["akka.tcp://worker@127.0.0.1:19093/user/transfer"]
	> 
	> ```
	>
	> 

####2.2.3. 其他配置(看情况优化配置)

-   内存配置:

```bash
 ~ vi /dv-masterhome/bin/sage-bigdata-etl.sh
```
```yaml
java -Xms128M -Xmx512M //此处填写master的最小内存和最大内存
worker上的内存设置与master相同,建议比master的最大内存大1倍,最小配置128M,最大值和最小值最好一致,下面为默认配置	
java -Xms128M -Xmx1G
```

- 处理速度优化

```yaml 
process {

    #处理程序数据读取线程(针对数据源是文件(HDFS,FTP,本地目录),及一次读取几个文件)
  
    size = 1
  
    #处理程序最少等待缓存的数据条数,当数据缓存达到这个数值处理程序将等待,直到缓存数据低于这个值
  
    cache.size = 2000
  
    // #处理程序最多使用解析器的个数
  
    lexer.size = 1
  
    #每一个处理程序写writer的并发数
  
    writer.size = 1
  
    #每一个writer刷新缓存的时间 单位s
  
    writer.flush.times=60

    #处理程序一次等待的时间
  
    wait.times = 500

}

```

  > 注意:如果提高解析速度,要相对应的提高worker内存

- 指定日志位置

  到对应的worker,master下

```bash
~ vi conf/logback-master.xml
~ vi conf/logback-worker.xml
```

#3. 插件安装

以下以hdfs插件为例子,将所需插件的zip包解压到sage-bigdata-etl-worker的安装目录下,

```bash
~ cd plugin
~ cp sage-bigdata-etl-hdfs-*-SNAPSHOT.zip ../worker/
~ unzip sage-bigdata-etl-hdfs-*-SNAPSHOT.zip
```

到插件目录的bin目录下执行安装脚本

```bash
~ cd sage-bigdata-etl-hdfs-*-SNAPSHOT/bin
~ ./install.sh
```


如果安装成功,sage-bigdata-etl-worker 的conf目录下会多一个配置文件sage-bigdata-etl-hdfs.conf

# 4. 启动与停止

##4.1. 启动 master
```bash
~ cd $sage-bigdata-etl_MASTER/bin
~ export JAVA_HOME=/path/to/jre8
~ ./start.sh
```

##4.2. 启动 worker

```bash
~ cd $sage-bigdata-etl_WORKER/bin

~ export JAVA_HOME=/path/to/jre8

~ ./start.sh

```
##4.3. 停止时执行相应的stop脚本:
```bash
~ ./stop.sh
```


# 5. sage-bigdata-etl目录结构

>   在程序主目录中各个目录的说明

- bin //程序启动脚本

- conf //程序配置文件

- db //db文件存放目录

- lib //第三方lib库

- logs //日志文件

- pids //程序启动id

# 6. 启动成功

> 安装部署完成后访问:http://master_ip:19090
>
> 点击`采集器`:看到采集器状态为`RUNNING`,即为正常运行,就表示启动成功

# 7. 一键安装配置

> 上述主要是详细阐述如何一步步安装 sage-bigdata-etl-master 和sage-bigdata-etl-worker。安装包同时也提供了一键安装的功能,通过简单执行脚本即可安装并启动运行。

1. 解压安装包后,可发现两个文本文件,如下
  - install.sh

  - setting.env

  需要关注的文件是 setting.env,根据需要查看并配置系统相关安装参数

```bash 
~ cat setting.env
```

```bash
#!/bin/bash
## The master and worker will both be installed under the $TOP_INSTALL_FOLDER
## If JRE install is enabled, it will be also installed under the $TOP_INSTALL_FODER
TOP_INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
TOP_INSTALL_DIR=$TOP_INSTALL_DIR/sage-bigdata-etl

RUNNING_AFTER_INSTALL=1

RESET_DB=1

## IMPORTANT!
## Currently only support *nix system for customized Java runtime version, if your environment is MacOS or Windows, please use system Java version.
## We prefer using serverJre --> Jre --> JDK
## The server JRE is the first choice
## As per the second chapter of ‘The Java HotSpot Performance Engine Architecture‘: The JDK includes two flavors of the VM — a client-side offering, 
## and a VM tuned for server applications. These two solutions share the Java HotSpot runtime environment code base, but use different compilers 
## that are suited to the distinctly unique performance characteristics of clients and servers. These differences include the compilation policy, 
## heap defaults and inlining policy.
## To understand it in a nut-shell read the following descriptions:
## Server JRE: It is used to deploy long-running java applications on server. It provides the fastest possible operating speed. 
## It has been specifically fine tuned to maximize peak operating speed. It has highly aggressive algorithms to optimize the runtime performance 
## of java application. It also includes variety of monitoring tools.
## Client JRE: It is used to run java applications on the end-users systems. It contains everything to run the java applications. It can start up 
## faster and requires a smaller memory footprint.

MASTER_INSTALL=1
MASTER_WEB_HOST=0.0.0.0
MASTER_WEB_PORT=19090

MASTER_HOST_INTERNAL=127.0.0.1
MASTER_PORT_INTERNAL=19091

## The double quote " should be escaped
MASTER_CLUSTER_SEED_NODES=[\"akka.tcp://master@127.0.0.1:19091\"]

WORKER_INSTALL=1
WORKER_HOST=127.0.0.1
WORKER_PORT=19093
WORKER_ID=\#workerid\#

## enabled daemon, as a service
DAEMON_ENABLED=on
DAEMON_PORT=19095

## Enabled authentication
## We currently only support AUTH_ENABLED=1
AUTH_ENABLED=1

AUTH_HOST=127.0.0.1
AUTH_PORT=9000


## Currently plugins support: 
## db2
## hdfs
## kafka_0.10.x
## mysql
## oracle
## postgresql
## rabbitmq
## sqlite
## sqlserver
## sqlserver_jtds
## es5
## es2
## ftp
## sftp
## snmp
## winevt
## aws
## sybase
## netflow
## streaming
## excel
## hbase
## modeling
## analyzer-logaggs
## analyzer-sql
## analyzer-timeseries

## Using comma separate the required plugin, please be noticed, due to version incompatible,
## es2 and es5 can not both be enabled, so do sqlserver and sqlserver_jtds
## WORKER_PLUGINS=db2,hdfs,kafka_0.10.x,mysql,oracle,postgresql,rabbitmq,
## sqlite,sqlserver,sqlserver_jtds,es5,es2,ftp,sftp,snmp,winevt,aws,
## sybase,netflow,streaming,excel,hbase,modeling,analyzer-logaggs,analyzer-sql,analyzer-timeseries
WORKER_PLUGINS=mysql,kafka_0.10.x,es5,streaming,modeling,analyzer-logaggs,analyzer-sql,analyzer-timeseries

## ==============================================================================================================
## ==============================================================================================================
## ==============================================================================================================

## User defined settings

ES_CLUSTER=elk
ES_HOSTPORTS=[[\"127.0.0.1\",9300],[\"127.0.0.2\",9300]]

SYSLOG_HOST=127.0.0.1
SYSLOG_PORT=514

WRITER_KAFKA_HOSTPORTS=127.0.0.1:9092,127.0.0.2:9092
WRITER_KAFKA_TOPIC=writer_topic_name

DATASOURCE_KAFKA_HOSTPORTS=127.0.0.1:9092,127.0.0.2:9092
DATASOURCE_KAFKA_TOPIC=datasource_topic_name

OMDB_HOST=127.0.0.1
OMDB_PORT=3306
OMDB_SCHEMA=omdb
OMDB_USER=root
OMDB_PWD=123456

FLINK_HOSTPORTS=127.0.0.1:6123,127.0.0.2:6123
``` 
配置文件中参数说明: 
-	RUNNING_AFTER_INSTALL:安装完成后是否启动运行,1运行,0不运行
-	RESET_DB:是否进行数据初始化,1是,0否
-	MASTER_INSTALL:是否安装 Master,1安装,0不安装
-	MASTER_WEB_HOST:用来提供外部\(Web\)访问,默认为:0.0.0.0,没特殊需求不必修改
-	MASTER_WEB_PORT:用来提供外部\(Web\)访问使用的端口,默认为19090
-	MASTER_HOST_INTERNAL:Master 内部使用的IP,没有特殊需求可设置为 Master进程启动的机器的IP
-	MASTER_PORT_INTERNAL:Master 内部与 Worker 交互的端口,默认为19091
-	MASTER_CLUSTER_SEED_NODES:Master与Worker交互配置,里面的IP和port必须和MASTER_HOST_INTERNAL和MASTER_PORT_INTERNAL 保持一致
-	WORKER_INSTALL:是否安装 Worker,1安装,0不安装
-	WORKER_HOST:Worker 使用的 IP 地址,没有特殊需求可设置为Worker进程启动的机器的IP
-	WORKER_PORT:Worker与 Master 通信的内部端口,默认为19093
-	WORKER_ID:Worker(采集器)的名称,指定一个有意义的名称方便识别,不修改默认为Worker所在服务器hostname
-	DAEMON _ENABLED:是否启用Daemon,on-启用 off-不启用
-	DAEMON _PORT:Daemon与 Worker通信的内部端口,默认为19095
-	AUTH_ENABLED::是否启用 portal验证,1启用,0不启用
-	AUTH_HOST:portal服务IP地址
-	AUTH_PORT:portal服务端口号,默认9000
-	WORKER_PLUGINS:以逗号分隔的要安装的支持数据源插件列表,可选任意一个或多个组合使用,目前支持的有: db2,hdfs,kafka_0.10.x,mysql,oracle,postgresql,rabbitmq,sqlite, sqlserver,sqlserver_jtds,es5,es2,ftp,sftp,snmp,winevt,aws,sybase,netflow,streaming,excel,hbase,modeling,analyzer-logaggs,analyzer-sql,analyzer-timeseries
特别注意:以下配置信息说明为数据初始化模板配置信息:如没有相关信息,使用默认配置即可:
-	ES_CLUSTER:对接ES集群名称
-	ES_HOSTPORTS:对接ES集群IP地址:格式为:[[\"$IP\",$PORT],[\" $IP \",$PORT]]
-	SYSLOG_HOST:对接syslog地址
-	SYSLOG_PORT:对接syslog端口
-	WRITER_KAFKA_HOSTPORTS:对接输出kafka集群地址,格式为:$IP: $PORT, $IP: $PORT
-	WRITER_KAFKA_TOPIC:对接输出kafka的topic名称
-	DATASOURCE_KAFKA_HOSTPORTS:对接输入kafka集群地址,格式为:$IP: $PORT, $IP: $PORT
-	DATASOURCE_KAFKA_TOPIC:对接输入kafka的topic名称
-	OMDB_HOST:OMDB所在数据库服务IP地址
-	OMDB_PORT:OMDB所在数据库服务端口
-	OMDB_SCHEMA:OMDB数据库库名,默认omdb
-	OMDB_USER:OMDB所在数据库访问用户
-	OMDB_PWD:OMDB所在数据库访问密码
-	FLINK_HOSTPORTS:对接flink集群地址,格式为:$IP: $PORT, $IP: $PORT

特别注意:
-	es2与 es5插件不能同时安装和使用；
-	sqlserver 与 sqlserver_jtds 插件不能同时安装和使用； 
-	在不同服务器安装不同的服务时,必须保证setting.env文件配置一致

>```bash
> ~ ./install.sh
> ```
> 
> 
> 
> 或者在命令行中指定要用的配置文件,名称可任意指定,但要符合 BASH
>
> 脚本的写法:
>
> ```bash
> ~ ./install.sh -f  path/to/setting.env
> ```
>
> **注意:**升级安装时务必指定参数:-U 或-u
>
> ```bash
> ~ ./install.sh -f  path/to/setting.env -U
> ```
>
> 	需特别注意,在升级安装时,如果master 或 worker正在运行,会导致升级安装失败,所以在升级前,正确的步骤是:
> 1.  停止运行中的数据源 
> 2.  停止 Master 及 Worker,停止顺序没有要求
> 3.  如果要同时安装 JRE,需将 JRE 放置与安装包同级的目录,目前支持 Oracle JRE
>    1.8的版本,并可自动识别版本号。
> 4.  如果单独启动 Maste 或 Worker,而非通过脚本启动,请在执行启动 master 或
>     worker 脚本之前,务必设置环境变量:JAVA_HOME







#8. 检测安装是否成功

>有以下方法可检测脚本是否安装成功:
>
>1.  判断脚本的返回值,0成功,非0是安装失败
>
>2. 如果安装成功并且启动,可通过调用 Master 的 界面或API
>   来根据返回值判断是否安装成功并运行正常
>
>    a.  访问[http://master-ip:master-port/ui/index.html](http://master-ip:master-port/ui/index.html)来查看
>
>    b.  请求[http://master-ip:master-port/collector](http://master-ip:master-port/collector)来查看响应是否正常
