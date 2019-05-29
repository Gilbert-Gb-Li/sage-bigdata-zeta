package com.haima.sage.bigdata.etl.store

import java.sql.ResultSet
import java.util.{Date, UUID}

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.store.resolver.ParserStore
import com.haima.sage.bigdata.etl.utils.{Mapper, ParserHelper}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created: 2016-05-16 18:33.
  * Author:zhhuiyan
  * Created: 2016-05-16 18:33.
  *
  *
  */
class DBParserStore extends ParserStore with DBStore[ParserWrapper, String] with Mapper {

  override val logger: Logger = LoggerFactory.getLogger("DBParserStore")

  override val TABLE_NAME = "PARSER"
  val CREATE_TABLE: String =
    s"""CREATE TABLE $TABLE_NAME(
       |ID VARCHAR(64) PRIMARY KEY NOT NULL,
       |NAME VARCHAR(64),
       |SAMPLE CLOB,
       |PARSER CLOB,
       |DATASOURCE VARCHAR(64),
       |PROPERTIES CLOB,
       |LAST_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |CREATE_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
       |IS_SAMPLE INT NOT NULL DEFAULT 0)""".stripMargin

  override val INSERT = s"INSERT INTO $TABLE_NAME(ID, NAME, SAMPLE, PARSER,DATASOURCE, PROPERTIES) VALUES (?, ?, ?, ?,?, ?)"
  override val UPDATE = s"UPDATE $TABLE_NAME SET NAME=?, SAMPLE=?, PARSER=?,DATASOURCE=?, PROPERTIES=?, LAST_TIME=? WHERE ID=?"
  override val DELETE = s"DELETE FROM $TABLE_NAME WHERE ID=?"
  init

  override def entry(set: ResultSet): ParserWrapper = {
    val name = set.getString("NAME")
    val parser = try {
      mapper.readValue[Option[Parser[MapRule]]](set.getString("PARSER"))
    } catch {
      case e: Exception =>
        logger.warn(s"parse error, parser name = $name", e)
        None
    }
    val properties = try {
      mapper.readValue[Option[List[ParserProperties]]](set.getString("PROPERTIES"))
    } catch {
      case e: Exception =>
        logger.warn(s"parse error, parser name = $name", e)
        None
    }
    val thiz = ParserWrapper(
      Option(set.getString("ID")),
      name,
      Option(set.getString("SAMPLE")),
      parser,
      Option(set.getString("DATASOURCE")),
      properties,
      Option(set.getTimestamp("LAST_TIME")),
      Option(set.getTimestamp("CREATE_TIME")),
      set.getInt("IS_SAMPLE"))
    thiz.copy(parser = ParserHelper.toParser(thiz))
  }

  /*
  * id 是必选字段 可以为空
  *   并且id应该为最后一个字段
  *
  * */
  override def from(entity: ParserWrapper): Array[Any] = {
    logger.debug(s"datasource[${entity.datasource.orNull}]")
    Array(entity.name,
      entity.sample.orNull,
      mapper.writeValueAsString(entity.parser),
      entity.datasource.orNull,
      mapper.writeValueAsString(entity.properties),
      new Date(),
      entity.id.orNull)
  }


  override def where(entity: ParserWrapper): String = {
    val name: String = if (entity.name != null && entity.name != "") {
      s" name like '%${entity.name}%'"
    } else {
      ""
    }

    if (name == "") {
      ""
    } else {
      s" where $name"
    }
  }

  override def set(parser: ParserWrapper): Boolean = {
    if (exist(SELECT_BY_ID)(Array(parser.id.getOrElse(""))))
      updateParser(parser)
    else
      insertParser(parser)
  }


  def insertParser(parser: ParserWrapper): Boolean = {
    execute(INSERT)(Array(parser.id.getOrElse(UUID.randomUUID().toString), parser.name, parser.sample.getOrElse(""),
      mapper.writeValueAsString(parser.parser),
      parser.datasource.orNull,
      mapper.writeValueAsString(parser.properties)))
  }

  def updateParser(parser: ParserWrapper): Boolean = {
    execute(UPDATE)(from(parser))
  }

  def bySub(id: String): List[ParserWrapper] = {
    query(s"SELECT * from $TABLE_NAME where PARSER like '%$id%' ")().toList
  }


  def toListParser(cs: Array[MapCase], default: Option[MapRule]): List[ParserWrapper] = {
    ((default match {
      case Some(ReParser(_, _, _ref)) =>
        _ref match {
          case Some(id) =>
            get(id).get
          case None =>
            null
        }
      case _ =>
        null
    }) :: cs.map(_case =>
      shiftRule(_case.rule)
    ).reduce[List[ParserWrapper]] {
      case (f, s) =>
        f match {
          case Nil =>
            s
          case _ =>
            f ::: s
        }
    }).filterNot(_ == null)
  }

  def shiftRule(rule: MapRule): List[ParserWrapper] = rule match {
    case ReParser(field, parser, ref) =>
      ref match {
        case Some(id) =>
          List(get(id).get)
        case None =>
          Nil
      }
    case r@MapRedirect(field, cs, default) =>
      toListParser(cs, default)
    case r@MapStartWith(field, cs, default) =>
      toListParser(cs, default)
    case r@MapEndWith(field, cs, default) =>
      toListParser(cs, default)
    case r@MapMatch(field, cs, default) =>
      toListParser(cs, default)
    case r@MapContain(field, cs, default) =>
      toListParser(cs, default)
    case _ =>
      Nil
  }

  def byKnowledge(knowledgeId: String): List[ParserWrapper] = {
    query(s"SELECT * FROM $TABLE_NAME WHERE PARSER LIKE '%$knowledgeId%'")().toList
  }
}
