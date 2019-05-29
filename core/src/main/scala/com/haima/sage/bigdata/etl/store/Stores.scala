package com.haima.sage.bigdata.etl.store

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.MetricTable
import com.haima.sage.bigdata.etl.store.position.ReadPositionStore
import com.haima.sage.bigdata.etl.store.resolver.ParserStore
import com.haima.sage.bigdata.etl.store.rules.{DBRuleGroupStore, DBRuleUnitStore, RulesStore}


/**
  * Created by zhhuiyan on 15/11/23.
  */
object Stores {
  lazy val statusStore: StatusStore = constructor[StatusStore](Constants.getApiServerConf(Constants.STORE_STATUS_CLASS))
  lazy val positionStore: ReadPositionStore = constructor[ReadPositionStore](Constants.getApiServerConf(Constants.STORE_POSITION_CLASS))
  lazy val configStore: ConfigStore = constructor[ConfigStore](Constants.getApiServerConf(Constants.STORE_CONFIG_CLASS))
  lazy val collectorStore: CollectorStore = constructor[CollectorStore](Constants.getApiServerConf(Constants.STORE_COLLECTOR_CLASS))

  lazy val analyzerStore: AnalyzerStore = constructor[AnalyzerStore](Constants.getApiServerConf(Constants.STORE_ANALYZER_CLASS))
  lazy val knowledgeStore: KnowledgeStore = constructor[KnowledgeStore](Constants.getApiServerConf(Constants.KNOWLEDGE_CLASS))
  lazy val knowledgeInfoStore: KnowledgeInfoStore = constructor[KnowledgeInfoStore](Constants.getApiServerConf(Constants.KNOWLEDGE_INFO_CLASS))
  lazy val modelingStore: ModelingStore = constructor[ModelingStore](Constants.getApiServerConf(Constants.STORE_MODELING_CLASS))

  lazy val dataSourceStore: DataSourceStore = constructor[DataSourceStore](Constants.getApiServerConf(Constants.STORE_DATASOURCE_CLASS))
  lazy val metricInfoStores =
    MetricTable.values.toList.map(table => (table, new MetricInfoStore(table))).toMap
  lazy val metricInfoStore: MetricInfoStore = metricInfoStores(MetricTable.METRIC_INFO_SECOND)
  lazy val metricHistoryStore: MetricHistoryStore = constructor[MetricHistoryStore](Constants.getApiServerConf(Constants.STORE_METRIC_HISTORY_CLASS))
  lazy val parserStore: ParserStore = constructor[ParserStore](Constants.getApiServerConf(Constants.STORE_PARSER_CLASS))
  lazy val writeStore: WriteWrapperStore = constructor[WriteWrapperStore](Constants.getApiServerConf(Constants.WRITER_CLASS))
  lazy val taskStore: TaskWrapperStore = constructor[TaskWrapperStore](Constants.getApiServerConf(Constants.TIMER_CLASS))
  lazy val taskLogInfoStore: TaskLogInfoStore = constructor[TaskLogInfoStore](Constants.getApiServerConf(Constants.TIMER_MSG_CLASS))
  lazy val dictionaryStore: DictionaryStore = constructor[DictionaryStore](Constants.getApiServerConf(Constants.DICTIONARY_CLASS))

  lazy val rulesStore: RulesStore = new RulesStore(
    constructor[DBRuleUnitStore](Constants.getApiServerConf(Constants.STORE_RULE_UNIT_CLASS)),
    constructor[DBRuleGroupStore](Constants.getApiServerConf(Constants.STORE_RULE_GROUP_CLASS))
  )


  private def constructor[T](name: String): T = try {
    Class.forName(name).asInstanceOf[Class[T]].newInstance()
  } catch {
    case e: ClassNotFoundException =>
      throw new ClassNotFoundException(s"Can not initialization $name ,cause by:${e.getCause}")
    case e: NoSuchMethodException =>
      throw new NoSuchMethodException(s"Can not initialization $name ,cause by:${e.getCause}")
    case e: Exception =>
      throw new Exception(s"Can not initialization $name ,cause by:${e.getMessage}")
  }

}