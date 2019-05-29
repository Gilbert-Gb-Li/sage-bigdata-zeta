[TOC]
sage-bigdata-etl FAQ
===================

version: 1.0

## 1. 说明 
  > sage-bigdata-etl使用过程中的常见文件解决思路

##  KAFKA 0.10版本 worker 无法访问 ping 可以,telnet可以
  > 配置worker 和kafka之间server 的/etc/hosts 能通过hostname 互相访问
 
## es2 与es5 插件同时使用会有问题
  > 解决方案是要使用不同版本的es时,不同的插件安装到不同的worker 实例上;
 
## @timestamp字段为ES中关键属性字段
  > 该字段为es2 与es5中的关键属性字段，请尽量保证该属性值中的数据准确性，数据在解析是无法对内容进行格式校验的;

## 匹配文件异常,或者会用编辑器开编辑文件时
  > 建议配置目录类型的数据源时,使用正则匹配文件的具体格式
  > 例如 `.*.csv`,`.*.xls` 等
 
## 启动worker/master时遇到磁盘空间满，造成程序启动失败时
  > 使用worker.conf.template/master.conf.template替换worker.conf/master.conf
  > 重新配置相关信息后再启动
  
## 当读取本地文件,ftp,sftp,hdfs等数据源时,对文件编码的处理
  > 1. 当确认知道文件编码时,请填上
  > 2. 如果文件比较小时建议,设置成UTF-8或者GBK尝试下,小文件自动编码识别可能不准确
  > 3. 文件比较大的时候,可以使用自动识别,成功率比较高
  
## 当原始数据中带有以下字段时的处理方式（通常是cli-collector再次处理cli-collector处理的数据会出现该问题）：
  > c@collector,raw,c@path,c@creationTime,c@数据源信息（c@lastAccessTime,c@lastModifiedTime....等）
  > 原因：以上字段时系统中默认带出来的，数据中如果存在相同的字段会产生冲突，并将系统中的字段覆盖掉
  > 解决方式：如果数据中出现以上字段，请对数据中的字段进行重命名处理，避免字段被覆盖
  
## 当作为数据`传输` `转发` 通道  数据输出格式是什么样的
  > 当使用解析规则为`传输` 或者 `不解析` 时,切没有任何其他的规则添加
  > 这个时候执行传输的动作,数据原样输出,不受 `数据存储` 的数据`输出格式` 影响
  
## 手动运行reset_db.sh（数据库初始化脚本）时，需要查看master进程是否停止
  > master进程未停止时，数据库无法连接。

## 个别win8服务器采集excel文件时，新增多excel文件，个别文件采集不到，同时需要观察其他类型文件是否有该问题。
  > 暂时 只有测试的一台机器出现此问题，其他win8、linux服务器没有复现
  
## 当数据存储类型是数据库时，数据表不存在的情况下，解析规则的配置,同时预览的字段列表字段的命名需符合数据库表字段的命名规范
   > 解析规则预览的字段列表需要配置,并且解析规则预览字段列表要包含数据源的字段列表。
   > 原因：数据表的生成会以解析规则字段列表作为数据表的字段；
   > 当数据源的字段在解析规则预览的字段列表中不存在的时候，写入数据的时候insert会异常。
   > 当字段命名不符合规范，如@timestamp,关键字等创建数据表的时候异常。这种情况需要使用""|``（具体根据数据库而定）标注。
   
## sage-bigdata-etl安装所在服务器基础支撑组件安装
   > netstat安装
   
## 使用文件续读，多行解码器解析时缺少最后一行数据
   > 多行解析需要在每行“开头”或“结尾”进行正则匹配，解码器在读取最后一行数据后，无法判断是否匹配结束
   
## 基于Flink的流式处理（分析通道、数据模型）在中途强制停止时，都会报出一下出错信息，该错误信息是流式处理强制提示，不是很友好，所以该错误提示是正常的，切不影响系统正常使用的。
  > 错误提示信息：
  > 2017-12-01 13:47:38,727 ERROR akka.remote.EndpointWriter (Slf4jLogger.scala:70) akka.tcp://worker@10.20.66.138:19093/system/endpointManager/reliableEndpointWriter-akka.tcp%3A%2F%2Fflink-source%4010.10.100.58%3A56214-2/endpointWriter AssociationError [akka.tcp://worker@10.20.66.138:19093] <- [akka.tcp://flink-source@10.10.100.58:56214]: Error [Shut down address: akka.tcp://flink-source@10.10.100.58:56214] [ akka.remote.ShutDownAssociation: Shut down address: akka.tcp://flink-source@10.10.100.58:56214 Caused by: akka.remote.transport.Transport$InvalidAssociationException: The remote system terminated the association because it is shutting down. ] 
  > 2017-12-01 13:47:38,735 DEBUG com.haima.sage.bigdata.etl.processor.Processor$Executor (Processor.scala:546) executor-2185fcfa-f052-4cc0-9c82-e85ce28d5e73 had wait[100 ms],lexers[1], batches[Map()] 
  > 2017-12-01 13:47:38,766 INFO akka.remote.RemoteActorRefProvider$RemotingTerminator (Slf4jLogger.scala:83) akka.tcp://flink@host138:52819/system/remoting-terminator Remoting shut down. 
  > 2017-12-01 13:47:38,767 DEBUG akka.event.EventStream (Slf4jLogger.scala:88) EventStream shutting down: StandardOutLogger started 
  > 2017-12-01 13:47:38,788 INFO com.haima.sage.bigdata.etl.streaming.flink.lexer.FlinkLexer (FlinkLexer.scala:173) flink lexer[flink-c06b2330-0d2e-48df-a3f8-7800bda437f0] closed 
  > 2017-12-01 13:47:38,793 ERROR com.haima.sage.bigdata.etl.streaming.flink.lexer.FlinkLexer (FlinkLexer.scala:130) Ask timed out on [Actor[akka://flink/user/$a#-667006151]] after [21474835000 ms]. Sender[null] sent message of type "org.apache.flink.runtime.messages.JobClientMessages$SubmitJobAndWait".

## 解析到数据存储
  > 对象转换成json字符串时会自动去除`null`值字段

## 读取ftp数据源
  > 个别redhat7.2环境对现有ftpapi不支持，导致不能正常读取ftp内容

## 关于文件路径
    > 文件目录深度超过一定限制，加文件名和其他固定的标识之后，超过500字符会出现问题，因为数据库中限制长度是500字节
    
## 解析规则、分析规则
    > 解析规则及分析规则中，对字段名称的命名方式，不允许使用"."符号命名

##关于知识库依赖的解析规则预览的字段
    > 首先知识库会以预览的字段列表作为建表的Schame信息，所以必须配置，如果不配置，会创建一个仅有raw字段的表
    > 预览字段的名称必须符合derby库列的命名规范，如果名称是关键字或是不符合规范的名称，使用英文的""括起来。

## 初始自动安装关闭auth后，重新配置master.conf中master.web.enable.auth配置，重启master。
    >目前这种操作，只支持Linux环境，auth配置修改生效。
    
## SQL分析时，嵌套字段不能使用含有特殊字符的字段，如“@、￥、%、&“
  > 比如：select t1.name,t2.id from t1 join t2 where t1.`@timestamp` = t2.`@timestamp` 