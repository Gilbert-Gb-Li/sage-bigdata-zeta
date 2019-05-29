package com.haima.sage.bigdata.etl.server.swagger

import com.github.swagger.akka._
import com.haima.sage.bigdata.etl.server.route._

object SwaggerDocService extends SwaggerHttpService {

  // override val host = "localhost:12345"
  //override val info = Info(version = "1.0")
  //override val externalDocs = None
  //Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
  //override val securitySchemeDefinitions =  Map("basicAuth" -> new BasicAuthDefinition())

  override def apiClasses: Set[Class[_]] = Set(classOf[ConfigRouter],
    classOf[AnalyzerRouter],classOf[DataSourceRouter],
    classOf[ModelingRouter],classOf[ParserRouter],
    classOf[WriterRouter],classOf[CollectorRouter],
    classOf[MetricsRouter],classOf[PreviewRouter],
    classOf[KnowledgeInfoRouter],classOf[KnowledgeRouter],
    classOf[ConfigUsabilityRouter],classOf[TaskManagerRouter]
   )
}