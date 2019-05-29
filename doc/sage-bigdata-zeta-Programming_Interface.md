[TOC]
#JSON数据格式说明
##	整体配置(Config)
```json
{
    "id": "uuidfsds",//String
    "dataSource": {},//DataSource
    "parser": {},//Parser
    "collector": {},//Collector
    "writers": [],//Array[Writer]
    "properties":{}//Map[Sting,Any]
}
```
| 字段         | 类型               | 说明             |
| ---------- | ---------------- | -------------- |
| id         | String           | config的 uuid   |
| dataSource | DataSource       | 数据源对象          |
| parser     | Parser           | 解析规则对象          |
| collector  | Collector        | 采集器对象          |
| writers    | \[Writer,,\]     | 输出JSON，可以有多个输出 |
| properties | Map\[Sting,Any\] | 配置公用的属性参数等     |
## 数据源（DataSource）
>目前包含文件类和网络类，HDFS，Kafka，MQ等
###  文件（目录方式）数据源(FileSource)
```json
    {
     "name":"directory",
     "contentType":"txt" ,//String
     "path": "a/.txt",//String 文件路径
     "encoding": "",//String 文件编码类型
     "codec": {},//Codec
     "position":"END" ,//String 文件读取开始位置,START-END
     "skipLine": 0 //Int 跳过开始的几行
    }
```
| 字段          | 类型     | 是否为空 | 说明                              |
| ----------- | ------ | ---- | ------------------------------- |
| name        | String | 否    | 表示数据源类型:FileSource传directory    |
| contentType | String | 是    | 内容类型（列表通过API获取）默认是TXT           |
| path        | String | 否    | 文件路径                            |
| encoding    | String | 是    | 编码格式                            |
| position    | String | 是    | 读取位置（START，END） 修改的时候需要指定默认是END |
###  网络数据源(NetSource)
```json
{ 
    "name":"net",
    "contentType":"syslog", // String
    "protocol":"",// String
    "host":"",// String
    "port":"",// Int
    "listens":[],// Array[(String, String)] = Array(),
    "codec":{},// Codec
    "properties" :{}, // {String:String,,},
    "metadata":{} // Metadata
 }
```
| 字段          | 类型                | 是否为空 | 说明                                      |
| ----------- | ----------------- | ---- | --------------------------------------- |
| name        | String            | 否    | 表示数据源类型:NetSource传net                   |
| contentType | String            | 否    | 内容类型（列表通过API获取 ,syslog,snmp,pipe）       |
| protocol    | String            | 否    | 协议（udp，tcp）                             |
| host        | String            | 否    | IP（采集器的）                                |
| port        | Int               | 否    | 端口                                      |
| cache       | Int               | 否    | 最大缓存数（当解析较慢时，缓存接受消息的最大数量，建议设大值，默认20000） |
| properties  | {String:String,,} | 是    | net连接时需要的属性，如:用户名，密码等                   |
| metadata    | Metadata          | 是    | Metadata对象                              |
>备注:
>当contentType 是syslog时:properties{}
>可能要指定编码如:
> ```json
>  { "properties":{"encoding":"GBK"}}
>  ```
>当时snmp时，可能需要指定版本，如:
>```json
>{"properties":{"version":"v3"}}
>```
>当版本是v3时可能需要指定如下中的几个properties:
>```json
>{
>  "version":"v3",
>  "user":"some ward",
>  "auth_protocol":"some ward",
>  "auth_key":"some ward",
>  "priv_protocol":"some ward",
>  "priv_key": "some ward"
>}
>```
>### 文件（HDFSSource）数据源
```json
{ "name":"hdfs",
 "host":"",// String
 "port":"",// Int
 "authorization":false,//Boolean
 "authentication":"kerberos", //String
 "path":{} // FileSource
}
```
| 字段             | 类型         | 是否为空   | 说明         |
| -------------- | ---------- | ------ | ---------- |
| name           | String     | 是      | 表示数据源类型    |
| host           |            | String | 否          |
| port           | Int        | 否      | 端口         |
| authorization  | Boolean    | 是      | 默认false    |
| authentication | String     | 是      | 默认kerberos |
| path           | FileSource | 否      | 包含的文件数据源   |
>备注:当使用authorization时，需要配置conf目录下的core-hdfs。xml
>文件，指定对应的
>keytab文件（使用绝对路径）和相应的krb5.conf文件（使用绝对路径），并制定使用安全账户。如下
>```xml
><property>
><name>dfs.namenode.keytab.file</name>
><value>/etc/kerberos.keytab</value>
></property>
>```
>```xml
><property>
>    <name>dfs.namenode.kerberos.principal</name>
>    <value>root/kunlun@HADOOP.COM</value>
></property>
>```
>```xml
><property>
>    <name>java.security.krb5.conf</name>
>    <value>/Users/zhhuiyan/workspace/dataviewer/src/main/resources/krb5.conf</value>
></property>
>```
### FTPSource文件（FTP）数据源
```json
{
      "name":"ftp",
      "host":"",// String\
      "port": 21,// Int
      "path":{},// FileSource
      "properties":{}// Properties
  }
```
| 字段   | 类型         | 是否为空 | 说明       |
| ---- | ---------- | ---- | -------- |
| name | String     | 是    | 表示数据源类型  |
| host | String     | 否    | IP（采集器的） |
| port | Int        | 否    | 端口       |
| path | FileSource | 否    | 包含的文件数据源 |
### SFTPSource文件（SFTP）数据源
```json
{
  "name":"ftp",
  "host":"",// String
  "port":"",// Int
  "path":{},// FileSource
  "properties":""// Properties
}
```
| 字段   | 类型         | 是否为空 | 说明       |
| ---- | ---------- | ---- | -------- |
| name | String     | 是    | 表示数据源类型  |
| host | String     | 否    | IP（采集器的） |
| port | Int        | 否    | 端口       |
| path | FileSource | 否    | 包含的文件数据源 |
### Kafka数据源
```json
{
    "name":"kafka",
    "hostPorts":"",// String "topic":"",// String
    "topic":"",// String
    "codec":{},// Codec
    "properties":{}// Properties
}
```
| 字段        | 类型     | 是否为空 | 说明          |
| --------- | ------ | ---- | ----------- |
| name      | String | 否    | 表示数据源类型     |
| hostPorts | String | 否    | Zk的地址列表逗号隔开 |
| topic     | String | 是    | 使用的topic    |
>备注:properties参考 kafka 官方配置
### RabbitMQ数据源
```json
{
  "name":" rabbitmq",
  "host":"",// String = "127.0.0.1",
   "port":"",// Option[Int] = None,
    "topic":"",// String\
	"virtualHost":"",// Option[String] = None,
	"endpoint":"",// String = "endpoint",
	"username":"",// Option[String] = None,
	"password":"",// Option[String] = None,
	"encoding":""// Option[String] = None
}
```
  |字段          |类型    | 是否为空  | 说明|
  |------------- |-------- |---------- |----------------|
  |name          |String   |否         |表示数据源类型 |
  |host          |String   |否         |主机地址      |
  |port          |String   |否         |主机端口      |
  |virtualHost   |String   |是         |队列虚拟地址    |
  |endpoint      |String   |是         |队列名称      |
  |username      |String   |是         |用户名       |
  |password      |String   |是         |密码        |
  |encoding      |String   |是         |数据的编码     |
>备注:properties参考 kafka 官方配置
### ESSource (ES数据源)
```json
{
    "name":" es",
"cluster":"",// String
"hostPorts":"",// Array[(String, Int)],
"index":"",// String = "logs_%{yyyyMMdd}",
"esType":"",// String = "logs",
"field":"",// String
"start":"",// Any
"step":0 // Long
}
```
| 字段        | 类型          | 是否为空 | 说明                               |
| --------- | ----------- | ---- | -------------------------------- |
| name      | String      | 否    | 表示数据源类型                          |
|           |             |      |                                  |
| cluster   | String      | 否    | 集群的名称                            |
| hostPorts | String      | 否    | 主机地址列表                           |
| index     | String      | 否    | 数据的索引地址                          |
| esTYpe    | String      | 否    | 数据的索引类型                          |
| field     | String      | 否    | 指定的排序列字段，为long，或者date类型          |
| start     | Date，或者long | 否    | 指定的起始位置,时间，或者日期缓存an'c台腏          |
| step      | Long        | 否    | 指定每次采集的指定的范围（从value 到value+step） |
|           |             |      |                                  |
### 数据库数据源
```json
{"name": "jdbc",
"protocol":"",// String
"host":"",// String = "0.0.0.0",
"port":"",// Int = 0,
"shame":"",// String
"table":"",// String
"column":"",// String
"value":"",// Any
"step":"",// Long
"properties":"",// Option[Props] = None,
"mateData":""// Option[MateData] = None
}
```

| 字段         | 类型          | 是否为空 | 说明                                       |
| ---------- | ----------- | ---- | ---------------------------------------- |
| name       | String      | 否    | 表示数据源类型                                  |
| protocol   | String      | 否    | 采集的数据库jdbc类型，暂时支持"sqlite","derby" , "mysql", "oracle","DB2","sybase","postgresql","sqlserver" |
| host       | String      | 否    | 数据库的IP地址                                 |
| port       | Int         | 否    | 数据库的端口                                   |
| shame      | String      | 否    | 数据库系统的那个数据库                              |
| column     | String      | 否    | 指定的排序列字段，为long，或者date类型                  |
| value      | Date，或者long | 否    | 指定的起始位置,时间，或者日期缓存an'c台腏                  |
| step       | Long        | 否    | 指定每次采集的指定的范围（从value 到value+step）         |
| properties | String      | 是    | 数据库的相关属性信息，用户名，密码等信息**user**,**password**及其他信息 |
| metadata   | Metadata    | 是    | Metadata对象                               |

## 输出Writer

###  Elasticsearch 

```json
{
  "name": "es",
"id":"",// String
"cluster":"",// String
"hostPorts": [],//[String->Int]
"index":"",// String
"esType":"",// String
"cache":"",// Int
"routing":"",//Sting,
"ids":""//Sting
}
```
| 字段        |          类型          | 是否为空 |                    说明                    |
| :-------- | :------------------: | :--: | :--------------------------------------: |
| name      |        String        |  否   |       表示writer类型:Elasticsearch传es        |
|           |                      |      |                                          |
| id        |        String        |  否   |               writer uuid                |
|           |                      |      |                                          |
| cluster   |        String        |  否   |            Elasticsearch集群名称             |
|           |                      |      |                                          |
| hostPorts | \[\[String,Int\],,\] |  否   |          Elasticsearch节点ip及port          |
|           |                      |      |                                          |
| index     |        String        |  否   | Elasticsearch的index名称, 例如:log\_%{yyyyMMdd} |
|           |                      |      |                                          |
|           |                      |      |                                          |
| esType    |        String        |  否   |           Elasticsearch的type名称           |
|           |                      |      |                                          |
| cache     |         Int          |  否   |      缓存数量:缓存指定数量的信息后发往Elasticsearch      |
|           |                      |      |                                          |
| routing   |        Sting         |  是   |       配置ES数据存储到的 routing 位置 用字段区分        |
|           |                      |      |                                          |
| Ids       |        Sting         |  是   |            用来生成 存储到es的 唯一标示符的            |
### 数据库
```json
{
  "name": "jdbc",
  "protocol":"",// String
  "host":"",// String = "0.0.0.0",
  "port":"",// Int = 0,
  "schema":"",// String
  "table":"",// String
  "column":"",// String
  "persisRef":"",// Option[OBject] = None,
  "metadata":"",// Option[List[(String, String, String)]] = None,
  "properties":""// Option[Props] = None
}
```
| 字段         | 类型                   | 是否为空 | 说明                    |
| ---------- | -------------------- | ---- | --------------------- |
| name       | String               | 否    | 表示writer类型:jdbc 标示数据库 |
| protocol   | String               | 否    | 数据库的类型                |
| host       | \[\[String,Int\],,\] | 否    | 数据库的主机地址              |
| Port       | \[\[String,Int\],,\] | 否    | 数据库的端口                |
| schema     | String               | 否    | 数据库                   |
| table      | String               | 否    | 数据表                   |
| column     | String               | 是    | 主键字段                  |
| persisRef  | REF                  | 是    | 数据表参考                 |
| metadata   | Sting                | 是    | 字段类型配置                |
| properties | Sting                | 是    | 数据库相关的属性信息（用户名，密码等）   |
### Forward 数据转发
```json
{ "name":" forward",
  "id":"",// String
  "host":"",// String
  "port":"",// Int
  "system":"",// String="worker",
  "app":"",// String="publisher",
  "cache":""// Int
}
```
| 字段    | 类型     | 说明                                       |
| ----- | ------ | ---------------------------------------- |
| id    | String | writer uuid                              |
| host  | String | forward的IP                               |
| port  | Int    | forward的端口                               |
| cache | Int    | 缓存数量:缓存指定数量的信息后发送（当Collector用作Writer时使用，用作采集器时不使用） |
>备注:当forward作为输出时，可以使用这个配置
### tcp 数据转发
```json
{"name":" tcp",
"host":"",// String
"port":"",// Int
"contentType":""// Option[ContentType] = None
}
```
| 字段          | 类型     | 说明                        |
| ----------- | ------ | ------------------------- |
| id          | String | writer uuid               |
| host        | String | 发送到的IP                    |
| port        | Int    | 发送到的端口                    |
| contentType | Int    | 输出的数据类型，可以是json,xml,分割符，等 |
>备注:当forward作为输出时，可以使用这个配置
### syslog数据转发
```json
{"name":" net",
"protocol":"syslog",// String
"host":"",// String
"port":"",// Int
"contentType":""// Option[ContentType] = None
}
```
  字段          类型     说明
------------- -------- ------------------------------------------- ------------
  id            String   writer uuid
  protocol      String   否
  host          String   发送到的IP
  port          Int      发送到的端口
  contentType   Int      输出的数据类型，可以是json,xml,分割符，等
备注:当forward作为输出时，可以使用这个配置
### kafka
```json
{"name": "kafka",
    "id":"",// String
    "contentType":"",// Option[ContentType] = None
    "metadataBrokerList":"",// String\
    "topic":"",// String
    "persisRef":"",// Option[OBject] = None,
    "cache":"",// Int\
    "properties":""// Option[Props] = None
}
```
 |字段|类型|说明|
 |-------------------|--------|-------------------------------------------------------|
  |id                   |String   |writer uuid                                         |
  |topic                |String   |Topic                                               |
  |metadataBrokerList   |Int      |主机列表                                                |
  |persisRef            |REF      |是                                                   |
  |contentType          |Int      |输出的数据类型，可以是json,xml,分割符，等                           |
  |cache                |Int      |缓存数量:缓存指定数量的信息后发送                                   |
>备注:参考kafka官方配置说明 

### File文件输出

```json
{   
    "name": "file",
    "id":"",// String
    "contentType":"",// Option[ContentType] = None
    "path":"",// String
    "persisRef":"",// Option[OBject] = None,
    "cache":""// Int\
}
```
  字段          类型     说明
------------- -------- ------------------------------------------- ----------
  id            String   writer uuid
  path          String   文件路径
  persisRef     REF      是
  contentType   Int      输出的数据类型，可以是json,xml,分割符，等
  cache         Int      缓存数量:缓存指定数量的信息后发送
### HDFS
```json
{
  "name": "hdfs",
  "id":"",// String
  "host":"",// String
  "port":"",// Int
  "authorization":false,//Boolean
  "authentication":"kerberos",//String
  "path":""// File
}
```
|  字段           | 类型     |说明  |
|----------------|---------|-------|
 | id              | String  |  writer uuid    |
 | host            | String  |  主机名称         |
 | port            | Int     |  主机端口          |
 | persisRef       | REF     |  是               | 
 | authorization   | Boolean |  是               |
 | authentication  | String  |  是               |
 | path            | Int     |  文件类型            |
>备注:参考kafka官方配置说明
### std 标准输出
```json
    {
    "name":"std",// std ",
    "uuid":"",// String
    "contentType":{},// Option[ContentType] = None
    "cache":""// Int
    }
```
 | 字段         |类型     | 说明                                      |
 |------------|-------- |---------------------------------------|
 | uuid       |  String |  writer uuid                          |
 | contentType|  Int    |  输出的数据类型，可以是json,xml,分割符，等            |
 | cache      |  Int    |  缓存数量:缓存指定数量的信息后发送（） 0                |
>备注:输出到标准控制台

### Ref 数据"分表"模式

#### 引用
```json
  {
  "name" : "ref",
  "field" : " datefield "
 }
```
  字段    类型     是否必填   说明
------- -------- ---------- ----------------
  field   Filter   否         引用的字段名称
#### 引用时间
```json
{
"name" : "date",
"field" : "datefield",
"format" : "yyyyMMdd"
}
```

#### 数据截取

```json
{
"name" : "truncate",
"field" : "ref",
"end" : true,
"from" : 0,
"length" : 5
}
```

#### HASH取模计算
```json
{
"name" : "mod",
"field" : "ref",
"mod" : 10
}
```
  字段    类型     是否必填   说明
------- -------- ---------- ------------------
  field   String   否         引用的字段名称
  mod     String   是         数据最多分成几份
###  ContentType 输出文档类型
#### JSON
```json
{"name": "json",
"encoding":""// Option\[String\]
}
```
  字段       类型     是否必填   说明
---------- -------- ---------- ----------
  encoding   String   是         数据编码
#### XML
```json
{"name": "xml",
"encoding":""// Option\[String\]
}
```

#### 分隔符

```json
{"name": "del",
"delimit":"",// Option\[String\] = Some(","),
"fields":"",// Option\[String\],
"encoding":""// Option\[String\]
}
```

#### 键值对
```json
{"name": "json",
"delimit":"",// Option\[String\] = Some(","),
"tab":"",// Option\[String\] = Some(","),,
"encoding":""// Option\[String\]
}
```

## 解析规则
### 正则
```json
{ "name":"regex",
"value":"",// String
"filter":""// Filter
}
```
  字段     类型     是否必填   说明
-------- -------- ---------- ------------
  value    String   是         正则表达式
  filter   Filter   否         Filter对象
### 分隔符
```json
{
"name":"",// delimit
"delimit":"",// String
"fields":[],// \[String,,\],
"filter":""// Filter
}
```
  字段      类型           是否必填   说明
--------- -------------- ---------- ------------------
  name      String         是         固定值delimit
  delimit   String         否         分隔符默认值"，"
  fields    \[String,,\]   是         字段名称列表
  filter    Filter         否         Filter对象
### 键值对分隔（特指键值对）
```json
{ "name":"",// delimitWithKeyMap
"delimit":"",// String
"tab":"",// String
"filter":""// Filter
}
```
  字段      类型     是否必填   说明
--------- -------- ---------- -------------------------
  name      String   是         固定值delimitWithKeyMap
  delimit   String   否         分隔符默认值"，"
  tab       String   否         键值分隔符默认是"="
  Filter    Filter   否         Filter对象
### cef解析规则
```json
{
"name":"cef",
"filter":{}// Filter
}
```
  字段     类型     是否必填   说明
-------- -------- ---------- ------------
  name     String   是         固定值cef
  Filter   Filter   否         Filter对象
### json解析规则
```json
    {
         "name":"json",// 
        "filter":{}// Filter
    }
```
  字段     类型     是否必填   说明
-------- -------- ---------- ------------
  name     String   是         固定值json
  Filter   Filter   否         Filter对象
备注:解析数据格式是标准json的数据到输出
### xml解析规则
```json
    {
         "name":"xml",// 
        "filter":{}// Filter
    }
```
  字段     类型     是否必填   说明
-------- -------- ---------- ------------
  name     String   是         固定值xml
  Filter   Filter   否         Filter对象
备注:解析数据格式是标准xml的数据到输出
### nothing解析规则
```json
    {
        "name":"nothing",
        "filter":{}// Filter
    }
```
  字段     类型     是否必填   说明
-------- -------- ---------- ---------------
  name     String   是         固定值nothing
  Filter   Filter   否         Filter对象
>**备注:不对数据做解析，把数据写到raw字段作为输出**
### transfer解析规则
```json
    { 
        "name":"transfer",// 
        "filter":{}// Filter
    }
```
  字段     类型     是否必填   说明
-------- -------- ---------- ----------------
  name     String   是         固定值transfer
  Filter   Filter   否         Filter对象
>备注:不对数据做解析，只传输数据到输出端
## 资产类型(AssetType)
```json
    {
        "name":"",// String = null,
        "parentId":""// Option\[String\] = None,
    }
```
  字段       类型     说明
---------- -------- ----------------
  Name       String   资产的中文名称
  parentId   String   关联的父类ID
### DataSourceWrapper
-----------------
>数据源信息的封装
```json
    {
        "id":"",// Option\[String\] = *Some*(UUID.*randomUUID*().toString),\
        "name":"",//String 数据源界面显示的名称
        "collectorId":"",// String=**""**,\
        "resolverId":"",// String=**""**,\
        "assetTypeId":"",// String\
        "assetTypeName":"",// Option\[String\]=None,\
        "writers":[],//List\[WriteWrapper\]=*List*(),\
        "data":{},// DataSource=**null**,\
        "status":""// Option\[String\]=None,
    }
```
  字段          类型                               说明
------------- ---------------------------------- ------------------
  Name          String                             数据源的中文名称
  collectorId   String                             采集器ID
  resolverId    String                             解析规则的ID
  data          \[\[String, String, String\],,\]   真正数据源的信息
  assetTypeId   String                             资产类型的ID
  writers       Object                             数据输出到的配置
  status        String                             数据源的
###Resolver
--------
>对解析规则的包装
```json
    {
        "name":"",// String
        "assetType":{},// AssetType
        "sample":"",// Option\[String\]=None,
        "parser":{},// Parser=null,
        "properties":{}// Option\[List\[ResolverProperties\]\]
    }
```
  字段         类型                 说明
------------ -------------------- --------------------------
  name         String               解析规则的中文名称
  assetType    Object               关联的资产类型
  sample       String               样例
  parser       Object               正真的解析规则
  properties   ResolverProperties   解析后的字段属性信息定义
###ResolverProperties
------------------
>解析后的字段属性信息定义
```json
    {
        "name":"",// Option\[String\]=None,
        "type":"",// String,
        "key":"",// String
        "format":"",// Option\[String\]=None,
        "sample":""// Option\[String\]=None
    }
```
  字段     类型     说明
-------- -------- --------------------
  name     String   解析规则的中文名称
  type     String   字段的类型
  format   String   字段的格式化方式
  sample   String   样例
###WriteWrapper
------------
>####数据输出的包装类
```json
{
"name":"",// String
"writeType":"",// Option\[String\],
"data":""// Option\[Writer\]
}
```
| 字段   | 类型     | 说明        |
| ---- | ------ | --------- |
| name | String | 解析规则的中文名称 |
| type | String | 数据输出的类型   |
| data | String | 真的的输出内容   |
##  Codec （解码器-断行规则）
### line
```json
{
"name":""// line
}
```
>备注:解析一行到一行（正常不需要配置）
| 字段   | 类型     | 可否为空 | 说明      |
| ---- | ------ | ---- | ------- |
| name | String | 是    | 固定值line |
### multi
```json
{ "name":"",// multi
"pattern":"",// String
"inStart":""// Boolean
}
```
| 字段      | 类型      | 是否为空    | 说明       |
| ------- | ------- | ------- | -------- |
| name    | String  | 是       | 固定值multi |
| inStart | Boolean | 行头，还是行尾 |          |
| pattern | String  | 正则      |          |
>备注:解析多行原始数据按照给定的开始或者结束符到一行rawdata
### json
```json
{
  "name":"json"
}
```
>备注:解析多行原始数据（json格式）到一行rawdata
### delimit
```json
{
"name":"delimit",// 
"delimit":""// String
}
```
>备注:按照指定的分隔符，解析一行原始数据到到多行rawdata
## 过滤规则(Filter and Rule) 
---------------
>Filter是一个Rule的Array\[Rule\]
>Rule有很多类型,具体如下
### Rule之addField
```json
{
"name":"addFields",
"fields":{}// key->values
}
```
  字段     类型                 说明
-------- -------------------- -----------------------------
  fields   {String:String ,,}   添加或映射的字段key-value对
addField: 在解析的时候，把配置的key-value直接加入到解析结果中
### Rule之mapping
```json
{"name":"mapping",
"fields":[]
}
```
  字段     类型                 说明
-------- -------------------- -----------------------------
  fields   {String:String ,,}   添加或映射的字段key-value对
mapping:在解析的时候，把配置的字段名（key:name1）改为（value:name2）
### Rule之addTags(添加标签)
```json
{
    "name":"addTags",
    "fields": []
}
```
| 字段     | 类型           | 说明        |
| ------ | ------------ | --------- |
| fields | \[String,,\] | 要处理的字段名称集 |
|        |              |           |
### Rule之removeFields
```json
{ "name":"removeFields",//
"fields": []
}
```
  字段     类型           说明
  fields   \[String,,\]   要处理的字段名称集
removeFields:去掉配置的字段
### Rule之removeTags 
```json
{
"name":"removeTags",
"fields":[]
}
```
  字段     类型           说明
  fields   \[String,,\]   要处理的字段名称集
### Rule之drop
```json
{ 
  "name":"drop" 
}
```
删除当前数据
### Rule之redirect
```json
{"name":"redirect",
"field":"",//String
"cases":[],//arraty[Case]
"defualt":{}//Case
}
```
  字段      类型         说明
--------- ------------ -----------------------
  field     String       匹配字段
  cases     \[Case,,\]   注:Case extends Rule
  defualt   Rule         解析方式
redirect:
匹配字段名（field），来执行cases。在Cases中，可以根据field的不同value匹配不同的Rule。例如:有个字段名为"pname"，我们可以设定当pname=zhangsan时，把pname改为zhangsan；当pname=lisi时，把pname改为lisi
### Rule之startWith
```json
{"name":"startWith",
"field":"",
"cases": [],//[Case,,]
"default":""// Rule
}
```
  字段      类型         说明
--------- ------------ -----------------------
  field     String       匹配字段
  cases     \[Case,,\]   注:Case extends Rule
  defualt   Rule         解析方式
>startWith:匹配以给定前缀开头的字段
### Rule之endWith
```json
{
"name":"endWith",
"field":"",
"cases": [],//[Case,,]
"default":""// Rule
}
```
  字段      类型         说明
--------- ------------ -----------------------
  field     String       匹配字段
  cases     \[Case,,\]   注:Case extends Rule
  defualt   Rule         解析方式
endWith:匹配以给定后缀结束的字段
### Rule之match
```json
{"name":"match",
"field":"",
"cases": [],//[Case,,]
"default":""// Rule
}
```
  字段      类型         说明
--------- ------------ -----------------------
  field     String       匹配字段
  cases     \[Case,,\]   注:Case extends Rule
  defualt   Rule         解析方式
match:匹配给定规则的字段
### Rule之contain
```json
{"name":"contain",
"field":"",
"cases": [],//[Case,,]
"default":""// Rule
}
```
  字段      类型         说明
--------- ------------ -----------------------
  field     String       匹配字段
  cases     \[Case,,\]   注:Case extends Rule
  defualt   Rule         解析方式
contain:匹配包含给定字符的字段
### Rule之merger
```json
{"name":"merger",
"fields":[],//\[String,,\]
"field":""// String
}
```
  字段     类型           说明
-------- -------------- --------------
  fields   \[String,,\]   要合并的字段
  field    String         合并后的字段
>merger:合并字段，合并后的字段值为原字段值拼接而成（以空格为间）
>如:有两个字段a=1，b=2,merger之后为c=1 2
>```json
>{
>"name":"contain",
>"fields":["a","b"],
>"field": "c"
>}
>```
### Rule之fieldCut
```json
{"name":"fieldCut",
"field":"",//String,
"from":0,// Int
"to":10 // Int
}
```
  字段    类型     说明
------- -------- ----------------
  field   String   要截取的字段名
  from    Int      截取的起始位置
  to      Int      截取的结束为止
fieldCut:截取字段名
### Rule之Case
```json
{"name":"case",
"value":"",// String
"rule":{}// Rule
}
```
  字段    类型     说明
------- -------- ----------------
  value   String   所匹配的字段值
  rule    Rule     Rule
### Rule之ReParser(再解析)
```json
{"name":"ReParser",
"field":"",// String
"parser":{}// Parser
}
```
  字段     类型     说明
-------- -------- -----------
  field    String   字段名称
  parser   Parser   解析规则1.4
## 分析规则
> 分析规则 继承自解析规则,是针对已经做完格式化,标准化的数据进行计算的配置规则;
 - 接口
    ```json
          {
           "name":"xxx",
           "filter":{},
        "metadata":[["字段名","类型","样例"],["字段名2","类型2","样例2"]]
          }
    ```
### SQL 分析    
```json
        {
          "name":"sql",
          "table": "定义的表名",
          "fromFields":[["name1","type1"],["name2","type2"]],
          "sql":"计算的语句",
          "filter":{},
          "metadata":[["字段名","类型","样例"],["字段名2","类型2","样例2"]]
        }
```
## 分析规则的过滤规则
  
### 再分析
### 分析后-解析
## metrics 查询返回结果
```json
    [{
        "collectorId":"",// String
        "id":"",// String
        "metricType":"",// String
        "metric":{},// {String:String,…},
        "lasttime":""// String
    }]
```
| 字段          | 类型                | 说明                 |
| ----------- | ----------------- | ------------------ |
| collectorId | String            | uuid               |
| id          | String            | 数据源或writer的 uuid   |
| metricType  | String            | metric类型           |
| metric      | {String:String,…} | metric JSON String |
| datetime    | String            | 时间                 |
## 所有状态结果
```json
{
"status":"",// String,
"message":"" // String
}
```
| 字段      | 类型     | 说明   |
| ------- | ------ | ---- |
| status  | String | 状态码  |
| message | String | 状态信息 |
## metrics 信息说明
>metric信息内置每隔1分钟记录一次
### timers
>计算时间花费，每个操作（es提交给定条数的数据）
>writer会含有该类信息
>```json
>{
>"count": "0(Long)",
>"99.9%": "0.0(Double)",
>"75%": "0.0(Double)",
>"mean rate": "0.0(Double/second)",
>"mean": "0.0(Double)",
>"98%": "0.0(Double)",
>"min": "0(Long)",
>"1-minute rate": "0.0(Double/second)",
>"max": "0(Long)",
>"median": "0.0(Double)",
>"stddev": "0.0(Double)",
>"15-minute rate": "0.0(Double/second)",
>"5-minute rate": "0.0(Double/second)",
>"95%": "0.0(Double)",
>"99%": "0.0(Double)"
>}
>```
| 字段            | 说明                                       |
| ------------- | ---------------------------------------- |
| count         | 输出次数总计（如果每次输出1000条日志，那么入库的日志总数为:输出次数\*1000）(类型) |
| mean          | [[]{#OLE_LINK46 .anchor}]{#OLE_LINK45 .anchor}处理每条花费时间的平均值（秒）[]{#OLE_LINK59 .anchor}(类型) |
| meanrate      | 每秒处理的条数(类型/单位)                           |
| min           | 处理一批数据花费时间的最小值(类型)                       |
| max           | 处理一批数据花费时间的最大值(类型)                       |
| median        | 处理每条花费时间的中值(类型)                          |
| stddev        | 处理每条花费时间的标准差(类型)                         |
| 75%           | 排序后，位于75%位置的值(类型)                        |
| 95%           | 排序后，位于95%位置的值(类型)                        |
| 98%           | 排序后，位于98%位置的值(类型)                        |
| 99%           | 排序后，位于99%位置的值(类型)                        |
| 99.9%         | 排序后，位于99.9%位置的值(类型)                      |
| 1-minuterate  | 一分钟内每秒处理条数(类型/单位)                        |
| 15-minuterate | 十五分钟内每秒处理条数(类型/单位)                       |
| 5-minuterate  | 五分钟内每秒处理条数(类型/单位)                        |
### meters
>Meters用来度量某个时间段的平均处理次数（request per
>second），每1、5、15分钟的[[]{#OLE_LINK58 .anchor}]{#OLE_LINK57
>.anchor}TPS
>统计一定时间段内的日志条数
>dataSource和writer会含有该类信息
>```json
>{
>  "count": "100000(Long)",
>  "mean rate": "3175.478994996841(Double/second)",
>  "1-minute rate": "2562.1463192162782(Double/second)",
>  "15-minute rate": "2043.9525621023888(Double/second)",
>  "5-minute rate": "2128.941469739676(Double/second)"
>}
>```
| 字段            | 说明                  |
| ------------- | ------------------- |
| count         | 总数(类型)              |
| meanrate      | 平均值（秒）(类型/单位)       |
| 1-minuterate  | 一分钟内每秒处理字节数(类型/单位)  |
| 15-minuterate | 十五分钟内每秒处理字节数(类型/单位) |
| 5-minuterate  | 五分钟内每秒处理字节数(类型/单位)  |
## 监控状态
### 采集器监控状态
>运行时:
>```json
>{
> "collectorStatus": "RUNNING"
>}
>```
>ERROR时会附上reason
>```json
>{
> "collectorStatus": "ERROR",
> "reason" :"****"
>}
>```
>采集器的状态有:RUNNING, ERROR
### 数据源监控状态
>```json
>{
>  "monitorStatus": "PENDING",
>  "streamStatus": {
>    "streampath1": "PENDING",
>      "streampath2": "PENDING"
>  },
>  "writerStatus": {
>    "writerpathI": "PENDING",
>      "writerpathII": "PENDING"
>  }
>}
>```
| 字段            | 说明                                       |
| ------------- | ---------------------------------------- |
| monitorStatus | 监控状态                                     |
|               |                                          |
| streamStatus  | 采集状态（例如监控一个目录，目录下有多个文件，就会出现多个streamStatus） |
|               |                                          |
| writerStatus  | 输出状态（多个输出时，会有多个writerStatus）             |
|               |                                          |
| streampath\*  | 类型为String，值为:           数据源的类型(ds.name)+ConfigID+数据源的路径+文件路径（如果需要） 例如:net\_configid\_udp://0.0.0.0:5140  net\_configid\_G:\\\\data\_G:\\\\data\\\\ex131217.log |
| writerpath\*  | 类型为String，值为:数据源的类型(ds.name)+ConfigID+数据源的路径+文件路径（如果需要）+ Writer类型(name) + WriterId,例如:net\_configid\_udp://0.0.0.0:5140\_es\_writerid1,net\_configid\_G:\\\\data\_G:\\\\data\\\\ex131217.log\_es\_writerid1​ |
>数据源的状态有:RUNNING, FAIL, ERROR, PENDING,STOPPED
## 知识库(Knowledge)
```json
{
    "id":"",//String
    "name":"knowledgename",//String
    "assetsType":"",//String
    "driver":"",//String
    "collector":"",//String
    "parser":""//String
}
```
| 字段         | 类型     | 说明      |
| ---------- | ------ | ------- |
| id         | String | 知识库的uid |
| name       | String | 知识库名称   |
| assetsType | String | 知识库类型ID |
| driver     | String | 数据源ID   |
| collector  | String | 采集器ID   |
| parser     | String | 解析规则ID   |
## RESTful API概览图
![REST  API](assets/rest_api.png)
----------------
### REST API详细说明
#### 数据通道配置
#####  新增数据源

>请求方式:`POST`
>请求URI:config
>请求参数:Application/JSON
>请求示例一（FileSource）:

   ```bash
   curl -XPOST localhost:19090/config -d  'data json'
   ```
   
   data
   ```json
   {
      "id": "123",
       "collector": {
            "name": "agent",
            "host": "127.0.0.1",
            "id": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
            "port": 5151
        },
       "parser": {
           "name":"regex",
           "value": "%{IIS_LOG}",
           "filter": [
               {"name":"addField",
                   "fields": {
                      "test": "124"
                   }
               }
           ]
       },
       "writers": [{
           "name":"es"  ,
           "hostPorts": [
              ["172.16.219.220",9300]
           ],
           "esType": "iis",
           "cache": 1000,
           "cluster": "es-master",
           "id": "293",
           "index": "logs_%{yyyyMMdd}"
       }],
       "dataSource": {
           "name":"directory",
           "path": "G:\\\\data\\\\ex131217.log",
           "encoding": "utf-8",
           "id": "3211",
           "contentType": "line",
           "category": "iis",
           "position": "END",
           "metadata": {
               "host": "localhost",
               "data": [["test","test","String"]],
               "port": 0,
               "app": "app1"
           }
       }
   }
   ```
>请求示例二（NetSource）:
   ```bash
   curl -XPOST localhost:19090/config -d '{
   "collector": {
   "system": "agent",
   "host": "localhost",
   "cache": 0,
   "id": "0",
   "port": 5151
   },
   "parser": {
   "regex": {
   "value": "%{IIS\_LOG}",
   "filter": {
   "rules": \[{
   "addFields": {
   "fields": {
   "test": "124"
   }
   }
   }\]
   }
   }
   },
   "writers": \[{
   "es": {
   "hostPorts": \[\["yzh", 9300\]\],
   "esType": "iis",
   "cache": 1000,
   "cluster": "yzh\_es",
   "id": "0",
   "index": "logs\_"
   }
   }\],
   "dataSource": {
   "directory": {
   "mateData": {
   "host": "localhost",
   "port": 0,
   "app": "app1",
   "data": \[\["test", "test", "String"\]\]
   },
   "path": "/Users/zhhuiyan/workspace/data/iislog/SZ\_test/",
   "encoding": "utf-8",
   "contentType": "txt",
   "category": "iis",
   "position": "END",
   "codec": null
   }
   },
   "id": "0"
   }'
   ```
#### 常见响应列举:
>响应一:成功
>```json
>{
>"status": "200",
>"message": "INSERT SUCCESS!"
>}
>```
>响应二:解析失败
>```json
>{
>"status": "406",
>"message": "JSON PARSE ERROR : key not \"found\"\\:\"cache(when parsing Collector)"
>}
>```
>响应三:APIService已经接受config信息，但是config中的collector不存在
>```json
>{
>"status": "202",
>"message": "CONFIG INSERT SUCCESS,BUT THE COLLECTOR[73618a27-4402-416e-9417-2ba2a0b6a0cb / (127.0.0.1:5151) ] NOT EXIST"
>}
>```
#### 更新数据源配置
> 请求方式:PUT
> 请求URI:config请求参数:json
> 请求示例一:
> 1.FileSource:
   ```bash 
   curl -PUT localhost:19090/config -d 'data'
   ```
  data
  ```json
   {
    "collector": {
      "system": "agent",
        "host": "localhost",
        "cache": 0,
        "id": "0",
        "port": 5151
    },
   "parser": {
      "name":"regex",
        "value": "%{IIS_LOG}",
        "filter": {
            "rules": [{
              "name":"addFields",
               "fields": {
                "test": "124"
                }
            
            
                }
            ]
        }
   },
   "writers": [{
      "name":"es",
        "hostPorts": [["yzh", 9300]],
        "esType": "iis",
        "cache": 1000,
        "cluster": "yzh_es",
        "id": "0",
        "index": "logs_"
        
        }],
   "dataSource": {
       "name":"file",
       "path": "/Users/zhhuiyan/workspace/data/iislog/SZ_test/",
       "encoding": "utf-8",
       "contentType": "txt",
       "category": "iis",
       "position": "END",
       "codec": null
   },
   "id": "123"
   }
   ```
> 请求示例二:
   ```bash
       curl -PUT localhost:19090/config -d 'data'
   ```
   data
   ```json
   {
       "collector": {
           "system": "agent",
           "host": "localhost",
           "id": "0",
           "port": 5151
       },
       "parser": {
       
             "value": "%{IIS_LOG}",
             "filter": {
             "rules": [{"fields": {   "test": "124"},
                        "name": "addFields"
                       }]
             },
           "name":"regex"
           },
       "writers": [{
           "name":"es",
           "hostPorts": [["yzh", 9300]],
           "esType": "iis",
           "cache": 1000,
           "cluster": "yzh_es",
           "id": "0",
           "index": "logs_"
        }],
   "dataSource": {
       "name":"net",
       "host": "192.168.10.24",
       "cache": 20000,
       "id": "0",
       "port": 5140,
       "contentType": "syslog",
       "protocol": "udp"
     },
   "id": "0"
   }
> ```
> 响应一:（更新成功）
> ```json
> {
> "status": "200",
> "message": " CONFIG_INFO UPDATEsuccess!"
> }
> ```
>响应二:（数据源正在工作的时候，执行更新，会出现以下返回信息。）
>```json
>{
>  "status": "304",
>   "message": "Not Modified : THE CONFIG HAVEN'T STOPPED,PLEASE STOP IT FIRST"
>}
>```
>响应三:（从json中解析出未知的collector）
>```json
>{
>"status": "404",
>"message": "Not Found: NOT FIND THE COLLECTOR"
>}
>```
####  查询数据源配置
>请求方式:`GET`
>请求URI:config/configid;
>请求示例:
    ```bash 
        curl -XGET localhost:19090/config/123   
    ```
>响应:
   ```json
   {
   "collector": {
   "host": "127.0.0.1",
   "id": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
   "port": 5151
   },
   "parser": {
   "name":"regex",
   "value": "%{IIS_LOG}",
      "filter": [{"name":"addField",
       "fields": {
             "test": "124"
             
        }
      }]
      
   },
   "writers": [{
       "name":"es",
         "hostPorts": [["172.16.219.220",
                        9300]],
        "esType": "iis",
        "cache": 1000,
        "cluster": "esmaster",
        "id": "293",
        "index": "logs_%{yyyyMMdd}"
                  
   }],
   "dataSource": {
    "name":"file",
       "path": "G:\\\\data\\\\ex131217.log",
       "encoding": "utf-8",
       "id": "3211",
       "contentType": "line",
       "category": "iis",
       "position": "END"
   
   },
   "id": "123"
   }
   ```
### 3.1.4 查询数据源配置ALL
> 请求方式:GET
> 请求URI:config/datasource
> 请求示例:
> ```bash 
> curl -XGET localhost:19090/config
> ```
> 响应:
> ```json 
> [{"collector": {
> "name": "agent",
> "host": "127.0.0.1",
> "cache": 0,
> "id": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
> "port": 5151
> },
> "parser": {
> "regex": {
> "value": "%{IIS_LOG}",
> "filter": [{
> "addField":{
> "fields": {
> "test": "124"
> }}}],
> }},
> "writers": [{"es": {
> "hostPorts": [["yzh", 9300]],
> "esType": "iis",
> "cache": 1000,
> "cluster": "yzh\_es",
> "id": "0",
> "index": "logs\_"
> } }\],
> "dataSource": {
> "directory": {
> "mateData": {
> "host": "localhost",
> "port": 0,
> "app": "app1",
> "data": \[\["test", "test", "String"\]\]
> },
> "path": "/Users/zhhuiyan/workspace/data/iislog/SZ\_test/",
> "encoding": "utf-8",
> "contentType": "txt",
> "category": "iis",
> "position": "END",
> "codec": null
> } },
> "id": "123"
> }]
> ```
#### 删除:
> 请求方式:DELETE
> 请求URI:config/<configid> 请求示例:
> ```shell
> curl -XDELETE localhost:19090/config/123
> ```
> 响应一:
> ```json
> {
> "status": "200",
> "message": "DELETE SUCCESS"
> }
> ```
> 响应二:（未停止数据源的时候，直接删除操作）
> ```json
> {
> "status": "304",
> "message": "Not Modified:THE CONFIG HAVEN'T STOPPED,PLEASE STOP IT FIRST"
> }
> ```
> 响应三:
> ```json
> {
> "status": "404",
> "message": "Not Found:CONFIG[123] NOT FOUND"
> }
> ```
3.2 数据源的配置-带UI
---------------------
3.3、采集器操作
---------------
### 3.3.1 查询
> 请求方式:GET
> 请求URI: collector/<collectorid> 请求示例:
> ```shell 
> curl -XGET localhost:19090/collector/73618a27-4402-416e-9417-2ba2a0b6a0cb
> ```
> 响应:
> ```json
> {
> "name": "pipe",
> "host": "127.0.0.1",
> "cache": 0,
> "id": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
> "port": 5151
> }
> ```
#### 批量查询
> 请求方式:GET
> 请求URI: collector
> 请求示例:
> ```shell
> curl -XGET localhost:19090/collector
> ```
> 响应:
> ```json
> [{
> "name": "pipe",
> "host": "127.0.0.1",
> "cache": 0,
> "id": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
> "port": 5151
> }]
> ```
3.4 解析规则、输出类型、读取方式的信息
------------------------------------
### 3.4.1 获取信息列表
> 请求方式:GET
> 请求URI:config/parser
> 请求示例:
> ```shell
> curl –XGET localhost:19090 /parser
> ```
> 响应:
> ```json
> {
> "lexer": ["regex", "json", "xml", "cef", "delimit"],
> "reader": {
> "directory": ["json", "line"],
> "net": ["snmp", "syslog", "kafka", "pipe"]
> },
> "writer": ["pipe", "kafka", "es"]
> }
> ```
###  Metric信息
--------------
>请求方式:GET
>请求URI:metrics/collectorId/dataSourceId(or writerId)(必需)?
>参数:
>mType查询类型（timers、meters、histogram），不传默认查询全部类型
>from开始时间（yyyy-MM-dd HH:mm:ss），不传默认为"结束时间"前10分钟
>to结束时间（yyyy-MM-dd HH:mm:ss），不传默认为当前时间
#### 3.5.1 histograms信息
>请求示例:
>```bash
>curl -XGET localhost:19090/metrics/73618a27-4402-416e-9417-2ba2a0b6a0cb/12345?pretty&mType=histograms&from=2015-03-16%2000:00:00&to=2015-03-16%2016:00:00
>```
>响应:
>```json
>[{
>"metricType": "HISTOGRAMS",
>"datetime": "2015-03-16 15:18:40.607",
>"id": "12345",
>"metric": {
>"count": "100000(Long)",
>"99.9%": "1651.0(Double)",
>"75%": "678.0(Double)",
>"mean": "581.871259027724(Double)",
>"98%": "970.0(Double)",
>"min": "189(Long)",
>"max": "1651(Long)",
>"median": "568.0(Double)",
>"stddev": "168.35924371725906(Double)",
>"95%": "860.0(Double)",
>"99%": "1053.0(Double)"
>},
>"collectorId": "73618a27-4402-416e-9417-2ba2a0b6a0cb"
>}]
>```
### 3.5.2 meters信息
>请求示例:
>```bash
>curl -XGET
>localhost:19090/metrics/73618a27-4402-416e-9417-2ba2a0b6a0cb/12345?pretty&mType=meters&from=2015-03-16%2000:00:00&to=2015-03-16%2016:00:00
>```
>响应:
>```json
>[{
>"metricType": "METERS",
>"datetime": "2015-03-16 15:18:40.615",
>"id": "12345",
>"metric": {
>"count": "100000(Long)",
>"mean rate": "2833.9460363866924(Double/second)",
>"1-minute rate": "4505.1873811327605(Double/second)",
>"15-minute rate": "5879.5180476113(Double/second)",
>"5-minute rate": "5648.662308353349(Double/second)"
>},
>"collectorId": "73618a27-4402-416e-9417-2ba2a0b6a0cb"
>}]
>```
#### 3.5.3 timers信息
>请求示例:
>```bash
>curl -XGET
>localhost:19090/metrics/73618a27-4402-416e-9417-2ba2a0b6a0cb/cf806c06-5e18-4b42-9e04-d58ea825a5ea?pretty&mType=timers&from=2015-03-16%2000:00:00&to=2015-03-16%2016:00:00
>```
>响应:
>```json
>[{
>"metricType": "TIMERS",
>"datetime": "2015-03-16 15:18:40.623",
>"id": "cf806c06-5e18-4b42-9e04-d58ea825a5ea",
>"metric": {
>"count": "0(Long)",
>"99.9%": "0.0(Double)",
>"75%": "0.0(Double)",
>"mean rate": "0.0(Double/second)",
>"mean": "0.0(Double)",
>"98%": "0.0(Double)",
>"min": "0(Long)",
>"1-minute rate": "0.0(Double/second)",
>"max": "0(Long)",
>"median": "0.0(Double)",
>"stddev": "0.0(Double)",
>"15-minute rate": "0.0(Double/second)",
>"5-minute rate": "0.0(Double/second)",
>"95%": "0.0(Double)",
>"99%": "0.0(Double)"
>},
>"collectorId": "73618a27-4402-416e-9417-2ba2a0b6a0cb"
>}]
>```
#### 3.5.4 all信息
>请求示例:
>```bash
>curl -XGET
>localhost:19090/metrics/73618a27-4402-416e-9417-2ba2a0b6a0cb/cf806c06-5e18-4b42-9e04-d58ea825a5ea?pretty&from=2015-03-16%2000:00:00&to=2015-03-16%2016:00:00
>```
>响应:
   ```json
   [{   
        "collectorId": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
        "metricType": "TIMERS",
        "datetime": "2015-03-16 15:18:40.623",
        "id": "cf806c06-5e18-4b42-9e04-d58ea825a5ea",
        "metric": {
            "count": "0(Long)",
            "99.9%": "0.0(Double)",
            "75%": "0.0(Double)",
            "mean rate": "0.0(Double/second)",
            "mean": "0.0(Double)",
            "98%": "0.0(Double)",
            "min": "0(Long)",
            "1-minute rate": "0.0(Double/second)",
            "max": "0(Long)",
            "median": "0.0(Double)",
            "stddev": "0.0(Double)",
            "15-minute rate": "0.0(Double/second)",
            "5-minute rate": "0.0(Double/second)",
            "95%": "0.0(Double)",
            "99%": "0.0(Double)"
        }
    
    }, {    
            "collectorId": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
            "metricType": "METERS",
            "datetime": "2015-03-16 15:18:40.617",
            "id": "cf806c06-5e18-4b42-9e04-d58ea825a5ea",
            "metric": {
                "count": "100000(Long)",
                "mean rate": "6022.807522510518(Double/second)",
                "1-minute rate": "10948.323252385751(Double/second)",
                "15-minute rate": "12168.551800469244(Double/second)",
                "5-minute rate": "11985.217171803984(Double/second)"
            }
        
    }
   ]
   ```
### 数据通道的启动、停止
> 注意: 启动、停止操作为异步操作。
###  启动
> 请求方式:PUT
> 请求URI:config/start/<configId> 请求示例:
   ```shell
        curl –XPUT localhost:19090/config/start/123
   ```
> 响应一:
   ```json
   {
   "status": "200",
   "message": "THE CONFIG[123] IS STARTING"
   }
   ```
> 响应二:（在数据源启动之后，继续发start操作，会出现如下返回结果）
   ```json
   {
   "status": "304",
   "message": "THE CONFIG[123] HAS STARTED"
   }
> ```
###  停止
> 请求方式:PUT
> 请求URI:operation/ stop/<configId> 请求示例:
> ```shell
> curl –XPUT localhost:19090/operation/stop/123
> ```
> 响应一:
> ```json
> {
> "status": "200",
> "message": "THE CONFIG[123] IS STOPING"
> }
> ```
> 响应二:（在数据源启动之后，继续发start操作，会出现如下返回结果）
> ```json
> {
> "status": "304",
> "message": "THE CONFIG[123] HAS STOPPED"
> }
> ```
## 3.7 监控状态

> 请求方式:`GET`
>
> 请求URI:health/<collectorId>/<configid>(当configId省略时，默认查询使用该collector的所有dataSource的状态)
>
> 请求参数:hType 监控状态类型（collector、datasource）
> 示例如下:
###   查询collector状态
> 请求示例:
> ```shell
> curl –XGET localhost:19090/health/73618a27-4402-416e-9417-2ba2a0b6a0cb/111?hType=collector
> ```
> 响应:
> collector运行:
> ```json
> {
>   "collectorStatus":"RUNNING"
> }
> ```
> collector error:在RESTfulAPI 服务在请求collector的时候超时，collector可能已经STOPPED
> ```json
> {
>   "collectorStatus": "ERROR" ,
>   "reason": "THE COLLECTOR[73618a27-4402-416e-9417-2ba2a0b6a0cb(127.0.0.1:5151)] REQUESTSTIMEOUT.IT MAY BE STOPPED"
> }
> ```
### 3.7.2 查询dataSource状态
> 请求示例:
> ```shell
> curl –XGET localhost:19090/health/73618a27-4402-416e-9417-2ba2a0b6a0cb/111?hType=dataSource
> ```
> 响应:
> ```json
> {
>  "collectorStatus": "RUNNING",
>   "dataSourceSource": {
>       "monitorStatus": "RUNNING",
>       "streamStatus": {
>       "net_configid_udp://0.0.0.0:5140": "RUNNING"
>       },
>       "writerStatus": {
>       "net_configid_udp://0.0.0.0:5140_es_writerid1": "RUNNING",
>       "net_configid_udp://0.0.0.0:5140_agent_writerid2": "PENDING"
> 	}
>   }
> }
> ```
####  查询Config整体状态
> 请求示例:
   ```sh
    curl –XGET localhost:19090/health/73618a27-4402-416e-9417-2ba2a0b6a0cb/111
   ```
> 响应:
> collector运行:
   ```json
    {
        "collectorStatus": "RUNNING",
        "dataSourceSource": {
            "monitorStatus": "RUNNING",
            "streamStatus": {
            "net_configid_udp://0.0.0.0:5140": "RUNNING"
            },
            "writerStatus": {
              "writerid1_net_configid_udp://0.0.0.0:5140": "RUNNING",
                "writerid2_net_configid_udp://0.0.0.0:5140": "PENDING"
            }
        }
    }
   ```
> collector error:
   ```json
    {
    "collectorStatus": "ERROR",
    "reason": " THE COLLECTOR[73618a27-4402-416e-9417-2ba2a0b6a0cb(127.0.0.1:5151)] REQUEST TIMEOUT.IT MAYBE STOPPED"
    }
   ```
#### 3.7.4 查询collector下的所有dataSource状态
> 请求示例:
> ```shell
> curl –XGET localhost:19090/health/73618a27-4402-416e-9417-2ba2a0b6a0cb?hType=datasource
> ```
> 响应:
   ```json
        {
           "123是configId）": {
             "monitorStatus": "RUNNING",
             "streamStatus": {
               "net_123_udp:\\/\\/0.0.0.0:5140": "RUNNING"
             },
             "writerStatus": {
               "net_123_udp:\\/\\/0.0.0.0:5140_es_writerid1": "RUNNING",
               "net_123_udp:\\/\\/0.0.0.0:5140_agent_writerid2": "PENDING"
             }
           },
           "12113（是configId）": {
             "monitorStatus": "RUNNING",
             "streamStatus": {
               "net_12113_udp:\/\/0.0.0.0:5140": "RUNNING"
             },
             "writerStatus": {
               "net_12113_udp:\/\/0.0.0.0:5140_es_writerid1": "RUNNING",
               "net_12113_udp:\/\/0.0.0.0:5140_agent_writerid2": "PENDING"
             }
           }
         }
   ```
####  查询collector下所有config状态
> 请求示例:
   ```sh
    curl –XGET localhost:19090/health/73618a27-4402-416e-9417-2ba2a0b6a0cb
   ```
> 响应:
   ```json
       {
          "collectorStatus": "RUNNING",
          "dataSourceStatus": {
            "123（是configId）": {
              "monitorStatus": "RUNNING",
              "streamStatus": {
                "net_123_udp:\\/\\/0.0.0.0:5140": "RUNNING"
              },
              "writerStatus": {
                "net_123_udp:\\/\\/0.0.0.0:5140_es_writerid1": "RUNNING",
                "net_123_udp:\\/\\/0.0.0.0:5140_agent_writerid2": "PENDING"
              }
            },
            "12113（是configId）": {
              "monitorStatus": "RUNNING",
              "streamStatus": {
                "net_12113_udp:\/\/0.0.0.0:5140": "RUNNING"
              },
              "writerStatus": {
                "net_12113_udp:\/\/0.0.0.0:5140_es_writerid1": "RUNNING",
                "net_12113_udp:\/\/0.0.0.0:5140_agent_writerid2": "PENDING"
              }
            }
          }
        }
   ```
3.8 知识库相关
----------------
##### 获取知识库
>请求方式：`GET`
>请求URl：`knowledge`
>请求示例：
   ```bash
      curl -XGET localhost:19090/knowledge
   ```
>响应一：
   ```json
       {
           "id":"cab0ceb3-1f52-4d5e-91fe-3012296c1c8d",
           "name":"knowledge1",
           "assetsType":"root",
           "driver":"3aad88ff-5ea9-46d8-b824-c87eb88b7d5d",
           "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
           "parser":"f272b9c1-6c71-4cc8-9a3e-b704256505c8"
       }
   ```
##### 新增知识库
>请求方式：`POST`
>请求URl：`knowledge`
>请求参数：`json`
>请求示例：
   ```bash
        curl -XPOST localhost:19090/knowledge -d '{
        "id":"123",
        "name":"knowledge1",
        "assetsType":"root",
        "driver":"3aad88ff-5ea9-46d8-b824-c87eb88b7d5d",
        "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
        "parser":"f272b9c1-6c71-4cc8-9a3e-b704256505c8"
   }'
   ```
##### 更新知识库 
>请求方式：`PUT`
>请求URl：`knowledge`
>请求参数：`json`
>请求示例：
   ```bash
      curl -PUT localhost:19090/knowledge -d '{
       "id":"123",
       "name":"knowledge1",
       "assetsType":"root",
       "driver":"3aad88ff-5ea9-46d8-b824-c87eb88b7d5d",
       "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
       "parser":"2725246f-ef97-4993-8a7f-0770c9883d83"
      }'
   ```
##### 查询知识库
>请求方式：GET
>请求URl：knowledge/uid
>请求示例：
   ```bash
        curl -XGET localhost:19090/knowledge/cab0ceb3-1f52-4d5e-91fe-3012296c1c8d
   ```
>响应一：
   ```json
     {
          "id":"cab0ceb3-1f52-4d5e-91fe-3012296c1c8d",
          "name":"knowledge1",
          "assetsType":"root",
          "driver":"3aad88ff-5ea9-46d8-b824-c87eb88b7d5d",
          "collector":"bb9c07be-f67d-445b-a76c-1028edc5885a",
          "parser":"f272b9c1-6c71-4cc8-9a3e-b704256505c8"
      }
   ```
>响应二：
   ```bash
       {
           "status":"404",
           "message":"NOT FOUND"
       }
   ```
##### 删除知识库
   ```bash
      curl -XDELETE localhost:19090/knowledge/123
   ```
>响应一：
   ```json
   {
       "status":"200",
       "message":"DELETE SUCCESS"
   }
   ```
>响应二：
   ```bash
   {
       "status":"404",
       "message":"NOT FOUND"
   }
   ```
##### 加载知识库
>请求方式：`GET`
>请求URl：`knowledge/load/#uid`
>请求示例：
   ```bash
      curl -XGET localhost:19090/knowledge/load/2e7d8e5d-3a30-48a8-b366-0bdbc5bcf6ca
   ```
>响应一：
   ```json
       {
           "status":"200",
           "message":"worker[akka://worker] load knowledge from driver[FileSource(D:\\D_sage-bigdata-etlDATA\\test.log,None,Some(FileType(txt)),None,Some(Codec(line)),Some(END),0)] is starting"
       }
   ```
>响应二：
   ```json
   {
       "status":"200",
       "message":"master[akka://master] load knowledge from driver[FileSource(D:\\D_sage-bigdata-etlDATA\\test.log,None,Some(FileType(txt)),None,Some(Codec(line)),Some(END),0)] is starting"
   }
   ```
>响应三：
   ```json
   {
       "status":"304",
       "message":"collector[bb9c07be-f67d-445b-a76c-1028edc5885a] maybe not running or is stopped"
   }
   ```
## 3.9  json pretty
> 在所有请求地址后添加pretty属性，返回结果将是格式化好的json，例如
> 查询collector信息:
> ```shell
> curl –XGET localhost:19090/config/collector?pretty
> ```
> 返回:
> ```json 
> [
>    {
>      "name": "pipe",
>      "host": "127.0.0.1",
>      "cache": 0,
>      "id": "73618a27-4402-416e-9417-2ba2a0b6a0cb",
>      "port": 5151
>    }
>  ]
> ```

## 特殊使用说明
 1. 数据上传,或json格式校验
   
   - 请求方法:`POST`
   - 请求URI: http://host/`需要校验的类型`/pre-check - d `data`
   - 返回结果:
     ```json
        { 
          "usable":true,//false
           "message":"是否可用的具体表述"
        }
     ```
    
附录:
--------------------
### 请求方式
>-   `GET`（SELECT）:从服务器取出资源（一项或多项）。
>-   `POST`（CREATE）:在服务器新建一个资源。
>-   `PUT`（UPDATE）:在服务器更新资源（客户端提供改变后的完整资源）。
>-   `DELETE`（DELETE）:从服务器删除资源。
### 状态码
>-   `200` OK 处理请求成功
>-   `202` Accepted 已经接受，但由于某种原因没有及时处理
>-   `400` Bad Request 错误的请求
>-   `404` Not Found 针对用户的请求不存在记录
>-   `406` Not Acceptable用户请求格式错误
>-   `408` Request Timeout 请求超时
>-   `500` Internal Server Error 服务器发生错误
>-   `304` Not Modified
### 运行状态
>- MonitorStatus:`STARTING`, `PENDING`, `RUNNING`, `STOPPING`, `STOPPED`, `ERROR`
>- StreamStatus:`RUNNING`, `ERROR`, `PENDING`, `STOPPED`
>- WriterStatus:`RUNNING`, `ERROR`, `PENDING`, `STOPPED`
>- CollectorStatus:`RUNNING`, `ERROR`, `PENDING`, `STOPPED`
