# 引言
&ensp; &ensp; sage-bigdata-etl管理手册（以下简称本手册）适用于sage-bigdata-etl2.1.X版本的产品。
sage-bigdata-etl在（以下简称本产品）在客户实际IT环境下部署，执行程序、配置文件、数据库文件和软件日志文件都可以配置（或者默认配置）在不同路径下。这里暂不考虑安装差异性来介绍sage-bigdata-etl的日常应用管理、运维操作建议和应急处置建议。
在这里，你将学习到如何管理 sage-bigdata-etl 自身。sage-bigdata-etl 是一个复杂的软件，有许多可移动组件，大量的 API 设计用来帮助管理你的 sage-bigdata-etl 部署。
在这个章节，我们涵盖三个主题：

根据监控你的集群重要数据的统计，去了解哪些行为是正常的，哪些应该引起警告，并解释 sage-bigdata-etl 提供的各种统计信息。
部署你的集群到生产环境，包括最佳实践和应该（或不应该！）修改的重要配置。
部署后的维护，如数据源、解析规则的复制、备份和导入等操作。
# 产品管理事项
&ensp; &ensp; 在安装部署本产品后，可详细阅读《用户手册》来了解产品操作使用流程。为了更好的利用本产品进行日常的ETL操作和管理，需要深入了解本产品的管理。
sage-bigdata-etl 经常以多节点集群的方式部署。 有多种 API 让你可以管理和监控集群本身，而不用和集群里存储的数据打交道。
## 集群健康
一个 sage-bigdata-etl 集群至少包括一个master节点和一个worker节点。或者它可能有数百个worker节点、三个单独的master节点，这些共同操作上千个数据源和解析规则。
不管集群扩展到多大规模，你都会想要一个快速获取集群状态的途径。Cluster Health API 充当的就是这个角色。你可以把它想象成是在一万英尺的高度鸟瞰集群。它可以告诉你安心吧一切都好，或者警告你集群某个地方有问题。

让我们执行一下 cluster-health API 然后看看响应体是什么样子的:

`GET /health/
`
## 第三方监控
sage-bigdata-etl可以从API层面为第三方监控软件提供监控接口，用于监控集群健康状态并进行告警；也可以在系统层面对服务器上的java进程进行监控，从而更加精细地对sage-bigdata-etl进行监控。

# 产品运维建议
产品安装部署（详见《安装部署手册》）完成之后，还需要日常系统运维。
登录系统之后，点击“采集器”菜单，查看当前集群下的采集器列表和每个采集器的运行状态。可以点击右侧的"开始"/"停止"按钮对采集器worker进行启停控制。
![sage-bigdata-etl采集器运行状态](/uploads/33d4c1ba71e41071bb0fd26ec1e29220/屏幕快照_2017-05-31_15.19.43.png)
还可以通过在“数据源”页面，点击“趋势图表📈” 来查看当前采集任务的性能数据。
![数据采集性能图表](/uploads/fcca3e34ecb1656a370b3101bba7739b/屏幕快照_2017-05-31_15.42.03.png)


### 运维建议
当数据读取和处理过程较慢的时候，sage-bigdata-etl的worker节点通常不会异常宕机。通常情况下，不建议管理员通过修改sage-bigdata-etl的配置方式来优化sage-bigdata-etl的性能。
当一个worker节点不能及时处理数据流的时候，可以增加一个worker节点来分阶段处理这部分数据。
当数据解析或者处理逻辑较复杂的时候，数据处理的性能会下降，可以通过按照业务逻辑字段分流的的机制来分配数据流到不同的worker上进行处理。
为了更好的管理集群节点和数据处理流程，建议在配置采集节点（修改worker的配置文件）的时候，将worker的ID配置成为有业务意义的编号，否则系统将会采用随机编码方式进行设定。同理，在设定系统中其他组件命名的时候，尽量使用统一的有意义的命名方式来命名，可以便于运维管理和未知系统问题定位。

# 产品应急处置建议
产品在日常使用中出现异常可能性较小，但还是存在的。即便产品在使用过程中突然停止，可以更加不同场景进行紧急处置。如果在数据源配置中，选择了“断点续传”，
![断点续传](/uploads/cfd57812c3813e01c912611d5bcd6d01/屏幕快照_2017-05-31_16.56.39.png)
可以在重新启动数据源采集任务之后，数据不丢失继续执行采集任务。
如果数据源无法通过UI或者API进行重启操作，就需要重新启动采集器进程。重启采集器操作可以在“采集器”界面，使用启停按钮进行。
如果重启采集器操作无法通过界面操作完成，就只能通过登录到worker所在的服务器上进行重启操作了。详见《安装部署手册》、《运维手册》