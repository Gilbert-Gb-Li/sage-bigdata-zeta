package com.haima.sage.bigdata.etl.server.hander

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.Rule
import com.haima.sage.bigdata.etl.store._
import com.haima.sage.bigdata.etl.store.resolver.ParserStore

trait StoreServer[T >: Null, ID] extends BroadcastServer {

  protected lazy val dataSourceStore: DataSourceStore = Stores.dataSourceStore
  protected lazy val parserStore: ParserStore = Stores.parserStore
  protected lazy val analyzerStore: AnalyzerStore = Stores.analyzerStore
  protected lazy val collectorStore: CollectorStore = Stores.collectorStore
  protected lazy val writeStore: WriteWrapperStore = Stores.writeStore
  protected lazy val configStore: ConfigStore = Stores.configStore
  protected lazy val knowledgeStore: KnowledgeStore = Stores.knowledgeStore
  protected lazy val statusStore: StatusStore = Stores.statusStore

  protected def store: BaseStore[T, ID]

  override def receive: Receive = {
    case (Opt.SYNC, (Opt.UPDATE, data: T@unchecked)) =>
      sender() ! store.set(data)
    case (Opt.SYNC, (Opt.CREATE, data: T@unchecked)) =>
      sender() ! store.set(data)
    case (Opt.SYNC, (Opt.DELETE, id: ID@unchecked)) =>
      sender() ! store.delete(id)
    case (Opt.GET, id: ID@unchecked) =>
      sender() ! store.get(id)
    case Opt.GET =>
      sender() ! store.all()
    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[T@unchecked]) =>
      sender() ! store.queryByPage(start, limit, orderBy, order, sample)
    case obj =>
      super.receive(obj)
  }

  def toConfig(wrapper: ConfigWrapper): Config = {

    wrapper.`type` match {
      case Some(_: AnalyzerChannel) =>
        val _collector = wrapper.collector.map(id => collectorStore.get(id).orNull)
        val collector = _collector.get
        val source = SingleChannel(
          getChannel(wrapper.datasource.get),
          wrapper.parser.map(id => analyzerStore.get(id).map(_.data.orNull).orNull),
          Some(wrapper.id),
          _collector,
          `type` = wrapper.`type`
        )
        Config(wrapper.id, wrapper.name,
          source,
          _collector,
          wrapper.writers.map(id => writeStore.get(id).map(_.data.setId(id)).orNull).filter(_ != null) :+ ForwardWriter(host = collector.host, port = collector.port, identifier = Option(wrapper.id)),
          wrapper.properties.map(data => Properties(data).properties.orNull)
        )
      case _ =>
        val _collector = wrapper.collector.map(id => collectorStore.get(id).orNull)
        val collector = _collector.get
        val parserWrapper: ParserWrapper = wrapper.parser.map(id => parserStore.get(id).get).get
        val meta = parserWrapper.properties match {
          case Some(data) if data.nonEmpty =>
            Some(data.map(prop => (prop.key, prop.`type`, prop.format.getOrElse(""))))
          case _ =>
            None
        }
        val writer = wrapper.writers.map(id => writeStore.get(id).map(_.data.setId(id))).filter(_.nonEmpty).map {
          /*case Some(rl: JDBCWriter) =>
            rl.copy(metadata = meta)*/
          case Some(rl: ES2Writer) =>
            val data = rl.copy(metadata = meta)
            logger.debug(s"rl:${rl.name}")
            logger.debug(s"data:${data.name}")
            data
          case Some(rl: ES5Writer) =>
            val data = rl.copy(metadata = meta)
            logger.debug(s"rl:${rl.name}")
            logger.debug(s"data:${data.name}")
            data
          case w =>
            w.get
        }

        val source: Option[DataSource] = wrapper.datasource.map(id => dataSourceStore.get(id).map(_.data).orNull match {
          case s: KafkaSource =>
            s.withGroupId(id)
          case s: DataSource => s
        })
        Config(wrapper.id, wrapper.name,
          SingleChannel(source.get,
            wrapper.parser.map(id => parserStore.get(id).map(_.parser.orNull).orNull), Some(wrapper.id)),
          _collector,
          writer :+ ForwardWriter(host = collector.host, port = collector.port, identifier = Option(wrapper.id), cache = writer.map(_.cache).max),
          wrapper.properties.map(data => Properties(data).properties.orNull)
        )


    }
  }

  def getChannel(id: String): Channel = {

    configStore.get(id) match {
      /*分析通道*/
      case Some(configWrapper) if configWrapper.`type`.exists(_.isInstanceOf[AnalyzerChannel]) =>

        val _collector = configWrapper.collector.map(id => collectorStore.get(id).orNull)
        SingleChannel(
          getChannel(configWrapper.datasource.get),
          configWrapper.parser.map(id => analyzerStore.get(id).map(_.data.orNull).orNull),
          Some(configWrapper.id),
          _collector,
          `type` = configWrapper.`type`
        )
      /*解析通道*/
      case Some(configWrapper) =>
        val _collector = configWrapper.collector.map(id => collectorStore.get(id).orNull)
        SingleChannel(dataSourceStore.get(configWrapper.datasource.get).get.data,
          parserStore.get(configWrapper.parser.get)
            .map(_.parser.get.asInstanceOf[Parser[_ <: Rule]]),Option(id),_collector)
      case None =>
        // TupleChannel or TableChannel or datasource
        dataSourceStore.get(id).get.data match {
          case tuple@TupleChannel(first, second, _, _) =>
            tuple.copy(
              first = first.id.map(ref => getChannel(ref)).getOrElse(first),
              second = second.id.map(ref => getChannel(ref)).getOrElse(second)
            ).withID(id)
          case tuple@TupleTableChannel(first, second, _) =>
            tuple.copy(
              first = first.id.map(ref => getChannel(ref)).getOrElse(first).asInstanceOf[TableChannel],
              second = second.id.map(ref => getChannel(ref)).getOrElse(second).asInstanceOf[TableChannel]
            ).withID(id)
          case table@SingleTableChannel(channel, _, _) =>
            table.copy(
              channel = channel.id.map(ref => getChannel(ref)).getOrElse(channel)
            ).withID(id)
          case source: DataSource =>
            SingleChannel(source).withID(id)
        }

    }
  }

  def channelCheck(id: String, sourceId: String): Channel = {
    id match {
      case x if x == sourceId => null
      case _ =>
        configStore.get(sourceId) match {
          case Some(configWrapper) if configWrapper.`type`.exists(_.isInstanceOf[AnalyzerChannel]) =>
            channelCheck(id, configWrapper.datasource.get)
          case Some(configWrapper) =>
            SingleChannel(dataSourceStore.get(configWrapper.datasource.get).get.data)
          case None =>
            dataSourceStore.get(sourceId).get.data match {
              case tuple: TupleChannel =>
                channelCheck(id, tuple.first.id.getOrElse(id))
                channelCheck(id, tuple.second.id.getOrElse(id))
              case table: SingleTableChannel =>
                channelCheck(id, table.channel.id.get)
              case source: DataSource =>
                SingleChannel(source)
            }
        }
    }
  }

}