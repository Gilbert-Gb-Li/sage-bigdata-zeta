package com.haima.sage.bigdata.etl.server.hander

import com.haima.sage.bigdata.etl.common.model.{BuildResult, _}
import com.haima.sage.bigdata.etl.store._


class AnalyzerServer extends StoreServer[AnalyzerWrapper, String] with PreviewHandler {

  override val store: AnalyzerStore = Stores.analyzerStore

  lazy val modelingStore: ModelingStore = Stores.modelingStore
  lazy val datasourceStore: DataSourceStore = Stores.dataSourceStore

  def proc(data: AnalyzerWrapper): AnalyzerWrapper = {
    data.data match {
      case Some(analyzer: SQLAnalyzer) if data.channel.isDefined =>

        /**
          * 只有单表的分析可以注册表到数据分析中
          **/
        datasourceStore.get(data.channel.get) match {
          case Some(DataSourceWrapper(_, _, SingleTableChannel(_, table, _), _, _, _, _)) =>
            data.copy(data = Option(analyzer.copy(table = Option(table))))
          case _ =>
            data

        }
      case Some(x) =>
        data
    }
  }


  override def receive: Receive = {
    case (Opt.SYNC, (Opt.UPDATE, _data: AnalyzerWrapper)) =>
      val data = proc(_data)
      logger.debug(s"server update analyzer is ${data}")
      if (store.all().exists(ds => ds.name == data.name && !ds.id.contains(data.id.orNull))) {
        sender() ! BuildResult("304", "311", data.name, data.name)

      } else {
        if (store.set(data)) {
          sender() ! BuildResult("200", "309", data.name)

        } else {
          sender() ! BuildResult("304", "310", data.name)

        }
      }
    case (Opt.SYNC, (Opt.CREATE, _data: AnalyzerWrapper)) =>
      val data = proc(_data)
      logger.debug(s"server update analyzer is ${data}")
      if (store.all().exists(_.name == data.name)) {
        sender() ! BuildResult("304", "311", data.name, data.name)
        //sender() ! Result("304", message = s"your set analyzer[${data.name}] has exist in system please rename it!")
      } else {
        if (store.set(data)) {
          sender() ! BuildResult("200", "300", data.name)
          //sender() ! Result("200", message = s"add analyzer[${data.id.get}] success!")
        } else {
          sender() ! BuildResult("304", "301", data.name)
          //sender() ! Result("304", message = s"add analyzer[${data.id.get}] failure!")
        }
      }
    case (Opt.GET, analyzerType: AnalyzerType.Type) =>
      sender() ! store.queryByType(analyzerType)
    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      val analyzer = store.get(id).orNull
      sender() ! (configStore.byParser(id) match {
        case head :: _ =>
          BuildResult("304", "307", analyzer.name, head.name)
        case Nil =>
          modelingStore.byAnalyzer(id) match {
            case head :: _ =>
              BuildResult("304", "317", analyzer.name, head.name)
            case Nil =>
              if (store.delete(id)) {
                BuildResult("200", "304", analyzer.name)
              } else {
                BuildResult("304", "305", analyzer.name)

              }
          }
      })

    case (Opt.SYNC, (Opt.GET, id: String)) =>
      sender() ! store.get(id)
    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[AnalyzerWrapper@unchecked]) =>
      logger.debug(s"query:$sample")
      sender() ! store.queryByPage(start, limit, orderBy, order, sample)
    //TODO FIXME flink 分析预览
    case (Opt.PREVIEW, collector: Collector, aw: AnalyzerWrapper) =>
      preview(collector, aw)
    case obj =>
      super.receive(obj)

  }

  def preview(collector: Collector, aw: AnalyzerWrapper): Unit = {
    val send = sender()
    val channel: Channel = aw.channel match {
      case Some(channelId) =>
        configStore.get(channelId) match {
          case Some(cw) =>
            toConfig(cw).channel
          case None =>
            dataSourceStore.get(channelId) match {
              case Some(ds) =>
                ds.data match {
                  case c: SingleTableChannel =>
                    c.copy(channel = configStore.get(c.channel.id.get) match {
                      case Some(cw) =>
                        toConfig(cw).channel
                      case None =>
                        null
                    })
                  case c: TupleChannel =>
                    c.copy(first = configStore.get(c.first.id.get) match {
                      case Some(cw) =>
                        toConfig(cw).channel
                      case None =>
                        null
                    }, second = configStore.get(c.second.id.get) match {
                      case Some(cw) =>
                        toConfig(cw).channel
                      case None =>
                        null
                    })
                  case _ =>
                    null
                }
              case None =>
                null
            }
        }

      case None =>
        logger.error("The channel are not set.");
        null
    }

    if (channel == null) {
      send ! (List(Map("error" -> "The channel are not set.")), List())
    } else {
      preview(send, collector, (channel, aw.data.get))
    }
  }
}