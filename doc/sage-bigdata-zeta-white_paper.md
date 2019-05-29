[TOC]

# 产品概述
========

​	分布式大数据采集器（简称 sage-bigdata-etl） 是新一代的大数据 ETL工具。

​	它支持广泛存在的各种类型数据源，通过灵活方便的自定义规则，进行数据抽取、识别或转换，并将部分或全量数据数据结果根据不同配置存入不同的大数据平台或进行二次转发、分发。

## 产品模块

-   sage-bigdata-etl-Master：采集器控制中心，可管理多个 sage-bigdata-etl-Worker
    进程，可监控其状态，配置下发，启停控制等；同时也提供 RestFul
    API，方便第三方对接。

-   sage-bigdata-etl-Worker：数据采集器，执行数据采集的最小单元，一个采集器可支持多个数据源的数据采集工作，一台机器可部署多个采集器，一个集群可以有多台机器组成；因此，在大规模
    ETL 的应用中，通常只需几台机器就可以完成。

## 数据源输入

当前采集器支持的数据源有以下几种（后续不断添加中）：

-   本地文件或目录

-   网络，支持syslog, SNMP, netflow等二进制报文, 支持任意端口的TCP/UDP
    协议发送字符

-   数据库，通过 JDBC 驱动支持 MySql，oracle, sqlserver, postgresql,
    db2, sybase, derby, sqlite 等

-   FTP， Hadoop HDFS 等

-   消息队列，如 Kafka，RabbitMQ 等

-   Elasticsearch

对于基于文件和消息队列等非易失的介质，数据源支持断点续传或跳行进行数据读取；为了对数据源进行解析规整，解析器支持读取样例日志并进行解析规则的预览操作。

## 数据源解析

当前采集器支持的解析规则有：

-   CEF（Common Event Format）

-   指定分隔符

-   键值对

-   XML，JSON

-   正则（grok pattern）

-   不解析，透传或转发

解析规则支持字段的重命名，字段合并及指定字段类型；解析规则由各个
sage-bigdata-etl-Worker 自行完成，与 sage-bigdata-etl-Master 无关。

### 输出到存储

当前采集器支持输出到以下存储介质或应用（后续不断添加中）：

-   Elasticsearch

-   消息对列，如 Kafka，RabbitMQ 等

-   Syslog

-   任意端口的 TCP

-   文件，HDFS等

-   数据库，通过 JDBC 驱动支持 MySql，oracle, sqlserver, postgresql,
    db2, sybase, derby, sqlite 等

-   数据转发，可作为通道进行跨网段的数据转发

根据选择的不同存储，在前端页面需要进行相应的设置；sage-bigdata-etl
可添加任意数量的存储类型，可以配置同一个数据源同时向多个存储并行写数据。

架构
====

sage-bigdata-etl 作为分布式可视化ETL 工具，采用 Extract，Transform，Load
标准作法；同时结合分布式部署，支持线性扩展。

#### sage-bigdata-etl架构图

![](assets/design.png)

-   数据源

> 支持基于TCP/UDP网络传输的文本；支持文件如 FTP，NFS，Local，HDFS；支持
> Netflow网络流；支持 Syslog、SNMP；支持大数据消息队列如
> Kafka，RabbitMQ；通过 JDBC 支持种类关系型数据库，以及 Elasticsearch
> 等。

-   采集集群

> 基于 Master-Slave 架构的采集集群，Slave（sage-bigdata-etl-Worker）
> 节点根据需要可任意扩充。

-   存储或转发

> sage-bigdata-etl-Worker
> 节点可配置输出到：HDFS，Elasticsearch，Kafka，RabbitMQ，Syslog，关系型数据库；也支持通过网络任意TCP/UDP端口转发；同时，可以配置一个输入的数据源，输出到一个或多个存储介质或转发出去。
>
> 管理员可完全操控采集集群，同时也提供完整的 HTTP Rest
> API，方便第三方应用或脚本等对接。

功能特性
========

-   #### 自动化脚本一键安装

> 通过设置配置参数，使用 Bash
> 一键安装脚本即可完成整个采集器环境的安装部署工作，可以方便第三方运维集成或基于云平台部署

-   #### 数据采集节点动态扩展

> 根据当前采集器压力（通过监控可以获知），这样可以根据实际情况平衡现有的数据源及新添加的数据源使用哪个采集器，或者对于新添加的数据源启用新的采集器来来分担。

-   #### 全WEB DOME配置

> 通过 UI 可配置输出的存储类型
>
> 通过 UI 可配置解析规则
>
> 通过UI可配置各种数据源

-   #### 完整的通用API接口

> RestFul API支持，标准
> JSON格式数据传输，完整的API操作，方便第三方调用集成。

-   #### 实时数据源状态监控

> 可查看每个数据源是否正常运行。

-   #### 采集进度实时计量

> 可查看每种数据源当前入库和总入库的条数和字节数。

-   #### 灵活的数据源管理 

> 可控制数据源启动，停止；可添加，删除，更新及查看数据源详情。

-   #### 支持扇入扇出 

> 支持数据从文件，网络，数据库等等汇入到同一个数据输出，或者从一个地方转发到不同的文件，网络，数据库等等。

-   #### 完善数据解析处理能力 

> 单条数据补充、删除、转化，多条数据过滤，转化、合并、分发。

系统要求
========

采集器可安装在 Windows 或 Linux 主机上，但通常推荐安装在 Linux 主机。

###  4.1 最小化安装

最小化安装仅需一台 Linux 服务器，配置如下：

CPU：双核

内存：2G

磁盘：至少1G 空闲

网络：百兆或千兆

以上配置通常可以满足一些测试和功能演示，此种场景下会将 sage-bigdata-etl-Master 和
sage-bigdata-etl-Worker 安装在同一台主机上，它们分别使用不同的端口。 sage-bigdata-etl-Master
占用内存小于256MB，sage-bigdata-etl-Worker
根据实际接数据情况（数据缓存大小，线程数等）会有不同，通常情况下4G
内存可满足大多数场景。

在同一个主机上也可以部署分布式的采集器节点，只需配置不同的端口通讯即可，但实际很少这样使用，因为同一台物理主机，多个
sage-bigdata-etl-Worker，并不能提升磁盘 IO 或网络 IO 的并发性能。

### 4.2 集群分布式安装

根据实际需要可以部署至少两个节点的服务器集群，这样 sage-bigdata-etl-Master 和
sage-bigdata-etl-Worker 可分别进行安装，它们都可以通过一键安装脚本来安装。

sage-bigdata-etl-Master 系统需求：

-   OS：CentOS/RHEL 6.x 64位

-   CPU: 双核

-   内存：2G 或以上

-   磁盘：至少1G 空闲

-   网络：全千兆

sage-bigdata-etl-Worker 系统需求：

-   OS：CentOS/RHEL 6.x 64位

-   CPU：四核或更多

-   内存：8G 或以上

-   磁盘：至少1G 空闲

-   网络：全千兆

建议以上节点不再承担其它角色或服务，以上配置单一
sage-bigdata-etl-Worker可以支撑10000EPS 的数据入库。

###  4.3 最佳实践

对于每天5种数据源，总量1T的原始数据，数据存储为Elasticsearch，保存最近一个月的数据，建议配置如下：

| 操作系统                | 角色             | 软硬件配置                     | 数量   |
| ------------------- | -------------- | ------------------------- | ---- |
| CentOS/RHEL 6.x 64位 | sage-bigdata-etl-Master    | 双核/4G/10G/千兆；JDK8         | 1    |
|                     | sage-bigdata-etl-Worker    | 四核/8G/20G/千兆；JDK8         | 5    |
|                     | ES Master Node | 四核/32G/100G SAS/千兆；JDK8   | 3    |
|                     | ES Data Node   | 四核/48G/1T\*6 SATA/千兆；JDK8 | 12   |

性能数据
========

单一数据源-单一采集器描述，网络千兆，数据源打满，单条1KB：

| 数据源           | 精确性  | 采集性能          | 错误处理机制     | 说明       |
| ------------- | ---- | ------------- | ---------- | -------- |
| Syslog        |      | 无             | \~10000EPS | 无        |
| Kafka         |      | at-least-once | \~30000EPS | 数据重复发送   |
| RabbitMQ      |      | 消息确认          | \~10000EPS | ACK 消息确认 |
| DB            |      | 检查点           | \~7000EPS  | 续读       |
| 本地文件          |      | 检查点           | \~30000EPS | 续读       |
| FTP 文件        |      | 检查点           | \~10000EPS | 续读\*     |
| HDFS 文件       |      | 检查点           | \~20000EPS | 续读       |
| Netflow       |      | N/A           | \~10000EPS | 无        |
| Elasticsearch |      | 检查点           | \~10000EPS | 续读       |

许可及支持
==========


