package com.haima.sage.bigdata.etl.server.hander

import akka.actor.Props
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store._


/**
  * Created by zhhuiyan on 16/7/18.
  */
class DataSourceServer extends StoreServer[DataSourceWrapper, String] with PreviewHandler {

  // private val configServer = context.actorOf(Props[ConfigServer])
  //private val configServer = context.actorOf(Props[ConfigServer])
  lazy val modelingStore: ModelingStore = Stores.modelingStore

  override val store: DataSourceStore = Stores.dataSourceStore

  lazy val configServer = context.actorOf(Props[ConfigServer])


  def fromChannel: List[DataSourceWrapper] = {
    val configs = configStore.all()

    val rt = configs.map(wrapper => {
      wrapper.datasource match {
        case Some(id) =>

          configs.find(_.id == id) match {
            case Some(config) =>
              DataSourceWrapper(wrapper.id, wrapper.name, getChannel(wrapper.id), collector = wrapper.collector, createtime = wrapper.createtime, lasttime = wrapper.lasttime)
            case _ =>
              val source: Option[DataSource] = store.get(id) match {
                case Some(ds) =>
                  ds.data match {
                    case s: KafkaSource =>
                      Some(s.withGroupId(id))
                    case s: DataSource => Some(s)
                  }
                case None =>
                  None
              }
              source match {
                case Some(s) =>
                  DataSourceWrapper(wrapper.id, wrapper.name, SingleChannel(s,
                    wrapper.parser.map(id => parserStore.get(id).map(_.parser.orNull).orNull) match {
                      case Some(null) =>
                        None
                      case obj =>
                        obj
                    },
                    Option(wrapper.id),
                    wrapper.collector.map(collectorStore.get).orNull,
                    wrapper.`type`
                  ), collector = wrapper.collector, createtime = wrapper.createtime, lasttime = wrapper.lasttime)
                case None =>
                  null
              }
          }

        case None =>
          null
      }

    }).filter(_ != null)
    logger.debug(s"configs[${configs.size}] as channel[${rt.size}]")
    rt
  }

  def getChannels: List[DataSourceWrapper] = {
    fromChannel ++ store.byType(DataSourceType.Channel)
  }

  def all: List[DataSourceWrapper] = {
    fromChannel ++ store.all()
  }

  override def receive: Receive = {
    case (Opt.GET, _type: DataSourceType.Type) =>

      logger.debug(s" get datasource by type[${_type}] ")
      /**
        * DataSourceType.Channel : 包括解析通道、分析通道、数据表（单表+组合）、通道组合
        * DataSourceType.NotTable : 通道组合
        * DataSourceType.Table : 数据表（单表+组合）
        * DataSourceType.Default: 普通数据源（解析通道使用的数据源）
        * DataSourceType.ChannelWithoutTable ： 包括解析通道、分析通道、通道组合
        */
      val list = _type match {
        case DataSourceType.Channel =>
          getChannels
        case DataSourceType.ChannelWithoutTable =>
          store.byType(DataSourceType.NotTable) ::: fromChannel
        case _ =>
          store.byType(_type)
      }
      sender() ! list.sortWith((v1, v2) => v2.createtime.get.before(v1.createtime.get))

    case (Opt.SYNC, (Opt.CREATE, datasource: DataSourceWrapper)) =>
      if (store.all().exists(_.name == datasource.name)) {
        sender() ! BuildResult("304", "111", datasource.name, datasource.name)
        //sender() ! Result("304", message = s"YOUR SET DATASOURCE[${datasource.name}] HAS EXIST IN SYSTEM PLEASE RENAME IT!")
      } else {
        /*val channel = {
          richChannel(datasource) match {
            case Some(d) =>
              datasource.copy(data = d)
            case None => datasource
          }
        }*/
        if (store.set(datasource)) {
          //val config = toConfig(datasource)
          // configServer.forward((Opt.CREATE, toConfig(datasource)))
          /*verifyDataSource(config.dataSource.get) match {
            case Some(rt) =>
              sender() ! rt
            case _ =>


          }*/
          sender() ! BuildResult("200", "100", datasource.name)
          //sender() ! Result("200", message = s"add datasource[${datasource.id}] success!")
        } else {
          sender() ! BuildResult("304", "101", datasource.name)
          //sender() ! Result("304", message = s"add datasource[${datasource.id}] failure!")
        }
      }

    case (Opt.SYNC, (Opt.UPDATE, datasource: DataSourceWrapper)) =>
      logger.debug(s"save[$datasource]")

      if (store.all().exists(ds => ds.name == datasource.name && !ds.id.contains(datasource.id))) {
        sender() ! BuildResult("304", "111", datasource.name, datasource.name)
        //sender() ! Result("304", message = s"your set datasource[${datasource.name}] has exist in system please rename it!")
      } else {
        /*val channel = {
          richChannel(datasource) match {
            case Some(d) =>
              datasource.copy(data = d)
            case None => datasource
          }
        }*/
        if (store.set(datasource)) {
          //val config = toConfig(datasource)
          //  configServer ! (Opt.RESTART, datasource)
          if (datasource.id != null && !"".equals(datasource.id) && datasource.data == null) {
            if (collectorStore.get(store.get(datasource.id).get.collector.get).isEmpty)
              sender() ! BuildResult("304", "121", datasource.name)
            else
              sender() ! BuildResult("200", "102", datasource.name)
          } else {
            if (collectorStore.get(datasource.collector.get).isEmpty)
              sender() ! BuildResult("304", "121", datasource.name)
            else
              sender() ! BuildResult("200", "102", datasource.name)
          }

          //sender() ! Result("200", message = s"update datasource[${datasource.id}] success, effective after after restart channel that dependence this!")
          //logger.debug(s"update config $config")
          //  configServer.forward((Opt.UPDATE, config))
        } else {
          sender() ! BuildResult("304", "103", datasource.name)
          //sender() ! Result("304", message = "update datasource[${datasource.id}] failure!")
        }
      }
    /*预览数据源的数据*/
    /*TODO FIXME When is channel */
    case (Opt.PREVIEW, id: String) =>

      self forward(Opt.PREVIEW, all.find(_.id == id).get)

    /*预览数据源的数据*/
    case (Opt.PREVIEW, data: DataSourceWrapper) =>
      val send = sender()
      val (source, collector) = if (data.id != null && !"".equals(data.id) && data.data == null) {
        val wrapper = store.get(data.id)
        val getcollector = collectorStore.get(wrapper.get.collector.get)
        if (getcollector.isEmpty) {
          (null, null)
        } else {
          (wrapper.get.data, getcollector.get)
        }
      } else {
        val getcollector = collectorStore.get(data.collector.get)
        if (getcollector.isEmpty) {
          (null, null)
        } else {
          (data.data, getcollector.get)
        }
      }
      source match {
        case null =>
          sender() ! List(Map("error" -> s"The collector is not exist."))
        case SingleChannel(s, p, _,_, Some(_: ParserChannel)) =>
          preview(send, collector, (s, p.get))
        case sql: SingleTableChannel =>
          // 单数据表预览时，构造一个查询所有字段的SQL分析规则
          preview(send, collector, (getChannel(data.id), SQLAnalyzer(sql = s"select * from ${sql.table.tableName}", table = Some(sql.table))))
        /*case tc: SingleTableChannel =>
          tc.channel match {
            case c: SingleChannel =>
              val config = toConfig(configStore.get(c.id.get).get)
              preview(send, collector, (config.channel.dataSource, config.channel.parser.get))
            case _ =>
              sender() ! List(Map("error" -> s"The channel preview is not supported until now."))
          }*/
        case _: TupleTableChannel =>
          /*TODO add channel all*/
          sender() ! List(Map("error" -> s"The channel preview is not supported until now."))

        case _: TupleChannel =>
          // 构造一个不作为的分析规则 NoneAnalyzer
          preview(send, collector, (getChannel(data.id), NoneAnalyzer()))
        //          sender() ! List(Map("error" -> s"The channel preview is not supported until now."))?
        case d =>
          preview(send, collector, d)
      }


    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      val datasource = store.get(id).orNull
      sender() ! (configStore.byDatasource(id) match {
          //被数据通道使用
        case head :: _ =>
          BuildResult("304", "115", datasource.name, head.name)
        case Nil =>
          knowledgeStore.byDataSource(id) match {
            //被知识库使用
            case head :: _ =>
              BuildResult("304", "117", datasource.name, head.name.orNull)
            case Nil =>
              modelingStore.byDataSource(id) match {
                //被数据建模使用
                case head :: _ =>
                  BuildResult("304", "118", datasource.name, head.name)
                case Nil =>
                  if (store.delete(id)) {
                    BuildResult("200", "104", datasource.name)
                    //sender() ! Result("200", message = s"delete datasource[$id] success!")
                  } else {
                    BuildResult("304", "105", datasource.name)
                    //sender() ! Result("304", message = s"delete datasource[$id] failure!")
                  }
              }
          }
      })

    case (Opt.GET, id: String, _type: DataSourceType.Type) =>
      logger.debug(s" get datasource by type[${_type}] and exclude_id[$id]")
      val list = _type match {
        case DataSourceType.Channel =>
          getChannels
        case DataSourceType.ChannelWithoutTable =>
          store.byType(DataSourceType.NotTable) ::: fromChannel
        case _ =>
          store.byType(_type)
      }
      val value = list.filter(ds => recursiveCheck(id, ds))
      sender() ! value.sortWith((v1, v2) => v2.createtime.get.before(v1.createtime.get))


    /* TODO val timers = Await.result(timerServer ? (Opt.GET, "byConf", id), timeout.duration).asInstanceOf[List[TimerWrapper]]
    if (timers.nonEmpty) {
      sender() ! Result("304", message = s"DELETE DATASOURCE[$id] FAILED,SOME timer is use it")
    } else {
      Await.result(configServer ? (Opt.DELETE, id), timeout.duration).asInstanceOf[Result] match {
        case Result("200", _) =>
          if (store.delete(id)) {
            sender() ! Result("200", message = s"DELETE DATASOURCE[$id] SUCCEED !")
          } else {
            sender() ! Result("304", message = s"DELETE DATASOURCE[$id] FAILED !")
          }
        case r@Result("404", _) =>
          if (store.delete(id)) {
            sender() ! Result("200", message = s"DELETE DATASOURCE[$id] SUCCEED !")
          } else {
            sender() ! Result("304", message = s"DELETE DATASOURCE[$id] FAILED!")
          }
        case r@Result("304", msg) =>
          if (msg.contains("don`t exist") && store.delete(id)) {
            sender() ! Result("200", message = s"DELETE DATASOURCE[$id] SUCCEED !")
          } else {
            sender() ! r
          }
        case rt =>
          sender() ! rt
      }
    }*/
    case obj =>
      super.receive(obj)


  }


  /*  def toConfig(dataSource: DataSourceWrapper): Config = {
      val collector = collectorStore.get(dataSource.collectorId)
      val parser = parserStore.get(dataSource.parserId)
      /*val timer = dataSource.timerId match {
        case Some(_id) => Stores.timerStore.get(_id)
        case _ => None
      }*/
      val assetMap = dataSource.assetId match {
        case Some(aid) => assetsStore.get(aid) match {
          case Some(asset) =>
            Map("assetId" -> asset.id, "assetName" -> asset.name)
          case _ =>
            Map[String, String]()
        }
        case None => Map[String, String]()
      }
      val parser = ParserServer.addData(ParserServer.toParser(parser.get), assetMap)
      val id = UUID.randomUUID().toString
      Config(dataSource.id.getOrElse(id), Some(dataSource.data), Some(parser), collector, dataSource.writers.map(wrapper => {

        wrapper.id match {
          case Some(_id) =>
            writeStore.get(_id).map(_.data)
          case _ =>
            None
        }
      }
      ).filter(_.nonEmpty).map {
        case Some(rl: JDBCWriter) =>

          rl.copy(metadata = parser.metadata)
        case Some(rl: ESWriter) =>
          val data = rl.copy(metadata = parser.metadata)
          logger.debug(s"rl:${rl.name}")
          logger.debug(s"data:${data.name}")
          data
        case w =>
          w.get
      }, dataSource.properties)

    }*/

  /*private def verifyDataSource(dataSource: DataSource): Option[Result] = {
    if (DataSource.monitorNamesInfo.contains(dataSource.name)) {
      dataSource match {
        case x: JDBCSource if !JDBCSource.protocolNamesInfo.contains(x.protocol) =>
          logger.error(s"API SERVICE ERROR : DBSOURCE ${x.name} PROTOCOL ${x.protocol} NOT SUPPORTED.")
          Some(Result(StatusCodes.InternalServerError.intValue.toString, s"API SERVICE ERROR : DBSOURCE ${x.name} PROTOCOL ${x.protocol} NOT SUPPORTED."))
        case _ => None
      }
    } else {
      logger.error(s"API SERVICE ERROR : DATA TYPE ${dataSource.name} NOT SUPPORTED.")
      Some(Result(StatusCodes.InternalServerError.intValue.toString, s"API SERVICE ERROR : DATA TYPE ${dataSource.name} NOT SUPPORTED."))
    }
  }*/

  def richChannel(datasource: DataSourceWrapper): Option[DataSource] = {
    def toChannel(ref: String): Option[Channel] = {
      getChannels.find(_.id == ref).map(_.data.asInstanceOf[Channel]).map {
        case s@SingleChannel(channel: Channel, _, id,_, _) =>
          channel.withID(ref)
        case o =>
          o.withID(ref)
      }

    }

    datasource match {
      case null =>
        None
      case ds if ds.data != null =>
        ds.data match {
          /*组合通道*/
          case tuple@TupleChannel(first: SingleChannel, second: SingleChannel, _, _) =>
            Some(tuple.copy(
              first = first.id.map(ref => toChannel(ref).orNull).getOrElse(first),
              second = second.id.map(ref => toChannel(ref).orNull).getOrElse(second)
            ))
          /*组合通道*/
          case tuple@TupleTableChannel(first, second, _) =>
            Some(tuple.copy(
              first = first.id.map(ref => toChannel(ref).orNull).getOrElse(first).asInstanceOf[TableChannel],
              second = second.id.map(ref => toChannel(ref).orNull).getOrElse(second).asInstanceOf[TableChannel]
            ))
          /*数据表通道*/
          case table@SingleTableChannel(channel: SingleChannel, _, _) =>
            Some(table.copy(
              channel = channel.id.map(ref => toChannel(ref).orNull).getOrElse(channel)
            ))
          /*数据表通道*/
          case s@SingleChannel(_, _, ref,_, _) =>
            Some(ref.map(ref => toChannel(ref).orNull).getOrElse(s))

          case channel: Channel =>
            Some(channel)
          case _ =>
            None
        }
      case _ =>
        None
    }
  }

  def recursiveCheck(id: String,ds: DataSourceWrapper): Boolean ={
      channelCheck(id, ds.id) match {
        case null => false
        case _ => true
      }
  }

}
