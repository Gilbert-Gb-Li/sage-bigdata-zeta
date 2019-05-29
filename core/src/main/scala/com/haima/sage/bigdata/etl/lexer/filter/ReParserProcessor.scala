package com.haima.sage.bigdata.etl.lexer.filter

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.filter.{Filter, MapRule, ReParser}
import com.haima.sage.bigdata.etl.common.model.{Parser, RichMap}
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import com.haima.sage.bigdata.etl.normalization.Normalizer
import com.haima.sage.bigdata.etl.utils.{Mapper, lexerFactory}


case class ReParserProcessor(override val filter: ReParser) extends RuleProcessor[RichMap, List[RichMap], ReParser] {

  final val RAW: String = "c@raw"
  private lazy val lexer = lexerFactory.instance(filter.parser)
  private lazy val _filter = filter

  protected lazy val r_filter: Option[Filter] = Option(_filter.parser.filter) match {
    case Some(f) if f.nonEmpty =>
      Some(Filter(f))
    case _ =>
      None
  }

  private lazy val normalizer: Normalizer = new Normalizer() {
    override def parser: Parser[MapRule] = _filter.parser

    private lazy val mapper = new Mapper {}.mapper

    override def toJson(value: Any): String = {
      mapper.writeValueAsString(value)
    }
  }


  override def process(event: RichMap): List[RichMap] = {
    this.parse(event)
  }

  def parse(event: RichMap): List[RichMap] = {

    val fld = filter.field match {
      case Some(f: String) if f.trim != "" =>
        f
      case _ =>
        RAW
    }


    val result: RichMap = event.get(fld) match {
      case Some(data: String) if data != "" =>
        lexer match {
          case Some(l) =>
            val parsed = l.parse(data)
            //                /* 追加父级属性名称，中间用“.”间隔 */
            //                val _redata: java.util.Map[String, Any] = {
            //                  val _data = new util.HashMap[String, Any]()
            //                  _data.putAll(parsed)
            //                  _data
            //                }
            //                parsed = Map(fld -> _redata)
            if (parsed.contains("error")) {
              //event
              parsed
            } else {
              //                  event - fld ++ parsed


              val notFieldEvent = event - fld

              notFieldEvent ++ (if (parsed.size == 1 && parsed.get(RAW).isDefined) {
                parsed.map(prop => {
                  fld -> prop._2
                })
              } else {
                parsed.map(prop => {
                  if (notFieldEvent.contains(prop._1)) {
                    (fld + "_" + prop._1) -> prop._2
                  } else {
                    prop._1 -> prop._2
                  }
                })
              })
              //              val notFieldEvent = event - fid
              //              val _event: Map[String, Any] = parsed.map(prop => {
              //                /*对于特殊字段raw 不做补充前缀*/
              //                if (notFieldEvent.contains(prop._1)) {
              //                  (fld + "_" + prop._1, prop._2)
              //                } else {
              //                  (prop._1, prop._2)
              //                }
              //              })
              //              notFieldEvent ++ _event
            }

          case None =>
            event
        }
      case _ =>
        event
    }
    r_filter.map(_.filter(result)).getOrElse(List(result)).map(t => {
      if (t.forall(_._1.startsWith("c@"))) {
        t
      } else {
        normalizer.normalize(t - "c@raw")
      }


    }

    )


  }
}