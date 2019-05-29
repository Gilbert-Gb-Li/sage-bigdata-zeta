package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.store.Stores

import scala.util.Try

object ParserHelper {

  private final val ADD_ASSET: Boolean = Try(Constants.MASTER.getBoolean(Constants.ADD_ASSET_DATA)).getOrElse(false)

  def shiftRule(rule: MapRule): MapRule = {
    rule match {
      case r@ReParser(field, parser, ref) =>
        ref match {
          case Some(id) =>
            Stores.parserStore.get(id) match {
              case Some(parser) =>
                ReParser(field, toParser(parser).orNull, parser.id)
              case None =>
                ReParser(field, parser)
            }
          case None =>
            r
        }

      case r@MapRedirect(field, cases, default) =>
        r.copy(cases = cases.map(_case =>
          _case.copy(rule = shiftRule(_case.rule))), default = default match {
          case Some(d) =>
            Some(shiftRule(d))
          case _ =>
            None
        })
      case r@MapStartWith(field, cases, default) =>
        r.copy(cases = cases.map(_case =>
          _case.copy(rule = shiftRule(_case.rule))), default = default match {
          case Some(d) =>
            Some(shiftRule(d))
          case _ =>
            None
        })
      case r@MapEndWith(field, cases, default) =>
        r.copy(cases = cases.map(_case =>
          _case.copy(rule = shiftRule(_case.rule))), default = default match {
          case Some(d) =>
            Some(shiftRule(d))
          case _ =>
            None
        })
      case r@MapMatch(field, cases, default) =>
        r.copy(cases = cases.map(_case =>
          _case.copy(rule = shiftRule(_case.rule))), default = default match {
          case Some(d) =>
            Some(shiftRule(d))
          case _ =>
            None
        })
      case r@MapContain(field, cases, default) =>
        r.copy(cases = cases.map(_case =>
          _case.copy(rule = shiftRule(_case.rule))), default = default match {
          case Some(d) =>
            Some(shiftRule(d))
          case _ =>
            None
        })
      case _ =>
        rule
    }
  }

  def addData(parser: Parser[MapRule], map: Map[String, String]): Parser[MapRule] = {

    if (ADD_ASSET) {
      val addFields =
        AddFields(map)
      parser match {
        case p: Regex =>
          p.filter match {
            case rules if rules != null && rules.nonEmpty =>
              p.copy(filter = addFields +: rules)

            case _ =>
              p.copy(filter = Array(addFields))
          }
        case p: Delimit =>
          p.filter match {
            case rules if rules != null && rules.nonEmpty =>
              p.copy(filter = addFields +: rules)

            case _ =>
              p.copy(filter = Array(addFields))
          }
        case p: DelimitWithKeyMap =>
          p.filter match {
            case rules if rules != null && rules.nonEmpty =>
              p.copy(filter = addFields +: rules)

            case _ =>
              p.copy(filter = Array(addFields))
          }
        case p: Parser[MapRule] =>
          p.filter match {
            case rules if rules != null && rules.nonEmpty =>
              Parser(p.name, metadata = p.metadata, filter = addFields +: rules)
            case _ =>
              Parser(p.name, metadata = p.metadata, filter = Array(addFields))
          }

      }
    } else {
      parser
    }

  }

  def toParser(parser: ParserWrapper): Option[Parser[MapRule]] = {
    val metadata = parser.properties match {
      case Some(data) if data.nonEmpty =>
        data.map(prop => (prop.key, prop.`type`, prop.format.getOrElse("")))

      case _ =>
        List[(String, String, String)]()
    }
    parser.parser.map {
      case p: TransferParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: NothingParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: CefParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: XmlParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: AvroParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: ProtobufParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: JsonParser =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: Regex =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: Delimit =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: DelimitWithKeyMap =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            p.copy(metadata = Some(metadata), filter = rules.map(shiftRule))
          case _ =>
            p.copy(metadata = Some(metadata))
        }
      case p: Parser[MapRule] =>
        p.filter match {
          case rules if rules != null && rules.nonEmpty =>
            Parser(p.name, metadata = Some(metadata), filter = rules.map(shiftRule))

          case _ =>
            Parser(p.name, metadata = Some(metadata))

        }
    }


  }
}