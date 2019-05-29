package com.haima.sage.bigdata.etl.performance.test

import com.haima.sage.bigdata.etl.common.model.RichMap

object GuajiInfoTest {

  def run(event: RichMap): RichMap ={

    import scala.collection.mutable

    class ConditionService(conditions: Seq[ConditionConfig]) {

      def process (event: mutable.Map[String, Any]): mutable.Map[String, Any] = {
        conditions.foreach( c =>
          c.process(event)
        )
        event
      }

    }

    object ConditionService {
      def apply(conditions: ConditionConfig*): ConditionService = {
        new ConditionService(conditions)
      }
    }

    case class ConditionConfig(
                                conditionIndex: String = "",
                                condition: Any = "default",
                                indices: Seq[String] = Array.empty[String],
                                index: String,
                                value: Any = ""
                              ){
      def process (event: mutable.Map[String, Any]): mutable.Map[String, Any] ={
        condition match {
          case s: String if s=="default" =>
            event.getOrElseUpdate(index, value)
            event
          case r: String =>
            val regex = r.r
            regex.findFirstIn(event.getOrElse(conditionIndex, "").toString) match {
              case Some(_) => event += (index -> value)
              case None => event
            }
        }
      }

    }



    import java.text.SimpleDateFormat
    import java.util.Date

    import com.haima.sage.bigdata.etl.common.model.RichMap

    import scala.collection.mutable
    import scala.util.Try

    case class MergeService(configs: MergeConfig*) {
      private val services = MergeServiceFactory.factory(configs)

      def valid(): Boolean = services.nonEmpty

      def process(event: RichMap): RichMap = Try {
        if (services.length == 1) {
          services.head.process(event)
        } else {
          val map = new mutable.HashMap[String, Any]()
          map ++= event
          val dst = process(map)
          event ++ dst
        }

      }.getOrElse(event)

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        if (services.length == 1) {
          services.head.process(event)
        } else {
          services.foreach(service => {
            service.process(event)
          })
          event
        }
      }.getOrElse(event)
    }

    object MergeServiceFactory {
      def factory(configs: Seq[MergeConfig]): Seq[MergeItemService] = Try {
        configs.map(new MergeItemService(_))
      }.getOrElse(Nil)
    }

    case class MergeConfig(dstField: String,
                           fromFields: Seq[String],
                           allowItemEmpty: Boolean = false,
                           separator: String = "",
                           trim: Boolean = true
                          )

    case class MergeItemService(config: MergeConfig) {
      val dstField = Try {
        config.dstField.trim
      }.getOrElse(null)
      val fromFields = Try {
        config.fromFields.filter(_ != null).map(_.trim).filter(_.nonEmpty)
      }.getOrElse(Nil)
      val separator = Try {
        config.separator.trim
      }.getOrElse("")
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      def valid(): Boolean = {
        if (config == null) {
          return false
        }
        if (config.dstField == null || dstField.isEmpty) {
          return false
        }
        if (config.fromFields == null || config.fromFields.isEmpty || fromFields.isEmpty) {
          return false
        }
        true
      }

      private def getFromValues(event: mutable.Map[String, Any]): Seq[String] = {
        fromFields.indices.map(i => {
          val field = fromFields(i)
          val value = event.getOrElse(field, null)
          if (value == null) {
            ""
          } else {
            val dstValue = format(value)
            if (config.trim) {
              dstValue.trim
            } else {
              dstValue
            }
          }
        })
      }

      private def getFromValues(event: RichMap): Seq[String] = {
        fromFields.indices.map(i => {
          val field = fromFields(i)
          val value = event.getOrElse(field, null)
          if (value == null) {
            ""
          } else {
            val dstValue = format(value)
            if (config.trim) {
              dstValue.trim
            } else {
              dstValue
            }
          }
        })
      }

      def format(value: Any): String = {
        value match {
          case x: Date =>
            sdf.format(x)
          case x: Any =>
            x.toString
        }
      }

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        val values = getFromValues(event)
        if (!config.allowItemEmpty) {
          if (values.exists(_.isEmpty)) {
            return event
          }
        }
        val dstValue = values.mkString(separator)
        event += (dstField -> dstValue)
      }.getOrElse(event)

      def process(event: RichMap): RichMap = Try {
        val values = getFromValues(event)
        if (!config.allowItemEmpty) {
          if (values.exists(_.isEmpty)) {
            return event
          }
        }
        val dstValue = values.mkString(separator)
        event + (dstField -> dstValue)
      }.getOrElse(event)
    }


    import java.math.BigDecimal

    import com.haima.sage.bigdata.etl.common.model.RichMap

    import scala.collection.mutable
    import scala.util.{Failure, Success, Try}

    class UnknownNumberItemService(override val config: NumberItemConfig)
      extends NumberItemService(config) {


      override def valid(): Boolean = {
        false
      }

      override def process(value: String): Number = {
        null
      }
    }

    class NumberService(val configs: NumberItemConfig*) {
      private val services = NumberServiceFactory.factory(configs)

      def valid(): Boolean = services.nonEmpty

      def process(event: RichMap): RichMap = Try {
        if (services.size == 1) {
          return services.head.process(event)
        }
        val map = new mutable.HashMap[String, Any]()
        map ++= event
        val dst = process(map)
        RichMap(dst.toMap)
      }.getOrElse(event)

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        services.foreach(service => {
          service.process(event)
        })
        event
      }.getOrElse(event)
    }

    abstract class NumberItemService(val config: NumberItemConfig) {
      def valid(): Boolean

      def process(event: RichMap): RichMap = Try {
        val map: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
        map ++= event
        val value: String = event.get(config.field) match {
          case Some(x: Any) =>
            x.toString.trim
          case None => null
        }
        val dstValue = process(value)
        map += (config.field -> dstValue)
        RichMap(map.toMap)
      }.getOrElse(event - config.field)

      def process(map: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        val value: String = map.get(config.field) match {
          case Some(x: Any) =>
            x.toString.trim
          case None => null
        }
        val dstValue = process(value)
        map += (config.field -> dstValue)
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          map -= config.field
      }

      def process(value: String): Number
    }

    object NumberServiceFactory {
      def factory(configs: Seq[NumberItemConfig]): Seq[NumberItemService] = Try {
        configs.map(config => {
          newNumberItemService(config)
        }).filter(_.valid())
      }.getOrElse(Nil)

      def newNumberItemService(config: NumberItemConfig): NumberItemService = Try {
        config.cleanType match {
          case "english-w-upper" | "W" =>
            new EndOnyNumberItemService(config, "W", 10000)
          case "english-w-lower" | "w" =>
            new EndOnyNumberItemService(config, "w", 10000)
          case "english-w-upper" | "K" =>
            new EndOnyNumberItemService(config, "K", 1000)
          case "english-w-lower" | "k" =>
            new EndOnyNumberItemService(config, "k", 1000)
          case "chinese-shi" | "shi" =>
            new EndOnyNumberItemService(config, "十", 10)
          case "chinese-bai" | "bai" =>
            new EndOnyNumberItemService(config, "百", 100)
          case "chinese-qian" | "qian" =>
            new EndOnyNumberItemService(config, "千", 1000)
          case "chinese-wan" | "wan" =>
            new EndOnyNumberItemService(config, "万", 10000)
          case "chinese-shiwan" | "shiwan" =>
            new EndOnyNumberItemService(config, "十万", 100000)
          case "chinese-baiwan" | "baiwan" =>
            new EndOnyNumberItemService(config, "百万", 1000000)
          case "chinese-qianwan" | "qianwan" =>
            new EndOnyNumberItemService(config, "千万", 10000000)
          case "chinese-yi" | "yi" =>
            new EndOnyNumberItemService(config, "亿", 100000000)
          case "none-or-single" | "either" =>
            new EitherNumberItemService(config)
          case "none-or-single" | "none" =>
            new NoneNumberItemService(config)
          case _ =>
            null
        }
      }.getOrElse(null)
    }

    case class NumberItemConfig(field: String,
                                cleanType: String,
                                needReplace: Boolean = true,
                                dotNumber: Boolean = false)

    class EitherNumberItemService(override val config: NumberItemConfig)
      extends NumberItemService(config) {
      private val kService = {
        NumberServiceFactory.newNumberItemService(
          NumberItemConfig(config.field, "k", needReplace = false, config.dotNumber))
      }
      private val wanService = {
        NumberServiceFactory.newNumberItemService(
          NumberItemConfig(config.field, "w", needReplace = false, config.dotNumber))
      }
      private val yiService = {
        NumberServiceFactory.newNumberItemService(
          NumberItemConfig(config.field, "yi", needReplace = false, config.dotNumber))
      }
      private val noneService = {
        NumberServiceFactory.newNumberItemService(
          NumberItemConfig(config.field, "none", needReplace = false, config.dotNumber))
      }

      override def valid(): Boolean = {
        true
      }

      override def process(raw: String): Number = {
        val lastChar: Char = raw.charAt(raw.length - 1)
        if (lastChar == 'w' || lastChar == 'W' || lastChar == '万') {
          wanService.process(raw.substring(0, raw.length - 1).trim)
        } else if (lastChar == '亿') {
          yiService.process(raw.substring(0, raw.length - 1).trim)
        } else {
          noneService.process(raw)
        }
      }
    }


    class NoneNumberItemService(override val config: NumberItemConfig)
      extends NumberItemService(config) {


      override def valid(): Boolean = {
        true
      }

      override def process(raw: String): Number = {
        val dst: BigDecimal = new BigDecimal(raw)
        if (config.dotNumber) {
          dst.doubleValue()
        } else {
          dst.longValue()
        }
      }
    }

    class EndOnyNumberItemService(override val config: NumberItemConfig,
                                  val from: String,
                                  val unit: Int)
      extends NumberItemService(config) {

      override def valid(): Boolean = {
        true
      }

      override def process(raw: String): Number = {
        val value: String = if (config.needReplace) {
          raw.replace(from, "").trim
        } else {
          raw.trim
        }
        val dst: BigDecimal = new BigDecimal(value).multiply(new BigDecimal(unit))
        if (config.dotNumber) {
          dst.doubleValue()
        } else {
          dst.longValue()
        }
      }
    }



    import java.util.regex.{Matcher, Pattern}

    import com.haima.sage.bigdata.etl.common.model.RichMap

    import scala.collection.mutable
    import scala.util.Try

    class RegexService(val configs: RegexConfig*) {
      private val services = RegexServiceFactory.factory(configs)

      def valid(): Boolean = services.nonEmpty

      def process(event: RichMap): RichMap = Try {
        val map = new mutable.HashMap[String, Any]()
        map ++= event
        val dst = process(map)
        RichMap(dst.toMap)
      }.getOrElse(event)

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        services.foreach(service => {
          service.process(event)
        })
        event
      }.getOrElse(event)
    }


    class SingleRegexItemService(override val config: RegexConfig)
      extends RegexItemService(config) {
      private val findConfig = Try(config.findConfigs.head).getOrElse(null)
      private val findService = new RegexFindService(findConfig)

      override def valid(): Boolean = {
        if (config == null || findService == null) {
          return false
        }
        if (!findService.valid()) {
          return false
        }
        true
      }

      override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        if (findService == null) {
          return event
        }
        val value: String = event.get(config.field) match {
          case Some(x: Any) =>
            x.toString
          case None => null
        }
        if (value == null) {
          return event
        }
        findService.process(value, event)._2
      }.getOrElse(event)
    }

    object RegexLogicType extends Enumeration {
      val AND: RegexLogicType.Value = Value(1)
      val OR: RegexLogicType.Value = Value(2)
    }


    abstract class RegexItemService(val config: RegexConfig) {
      def valid(): Boolean

      def process(event: RichMap): RichMap = Try {
        val map = new mutable.HashMap[String, Any]()
        map ++= event
        val dst = process(map)
        RichMap(dst.toMap)
      }.getOrElse(event)

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any]
    }

    object RegexServiceFactory {
      def factory(configs: Seq[RegexConfig]): Seq[RegexItemService] = Try {
        configs.map(config => {
          newRegexItemService(config)
        }).filter(_.valid())
      }.getOrElse(Nil)

      def newRegexItemService(config: RegexConfig): RegexItemService = Try {
        if (config.findConfigs.size == 1) {
          return new SingleRegexItemService(config)
        }
        if (config.logic == RegexLogicType.AND) {
          return new AndRegexItemService(config)
        }
        new OrRegexItemService(config)
      }.getOrElse(null)
    }

    case class RegexConfig(field: String,
                           logic: RegexLogicType.Value,
                           findConfigs: Seq[RegexFindConfig]) {
      def this(field: String,
               pattern: String,
               fields: Map[Int, String],
               matchType: Boolean = true,
               trim: Boolean = true) {
        this(field, RegexLogicType.AND,
          Seq(RegexFindConfig(pattern, fields, matchType, trim)))
      }
    }


    class RegexFindService(val config: RegexFindConfig) {
      private val pattern = Try(Pattern.compile(config.pattern)).getOrElse(null)

      def valid(): Boolean = {
        if (pattern == null) {
          return false
        }
        true
      }

      /**
        *
        * @param value 字段值
        * @param event 匹配结果放入map
        * @return (是否匹配成功， )
        */
      def process(value: String, event: mutable.Map[String, Any]): (Boolean, mutable.Map[String, Any]) = {
        if (pattern == null) {
          return null
        }
        val matcher = pattern.matcher(value)
        val matchRes: Boolean = matchResult(config.matchType, matcher)

        if (matchRes) {
          config.fields.foreach(entry => {
            val index = entry._1
            val field = entry._2
            val text = matcher.group(index) match {
              case null =>
                null
              case x =>
                if (config.trim) {
                  x.trim
                } else {
                  x
                }
            }
            if (text != null && text.length > 0) {
              event += (field -> text)
            }
          })
          (true, event)
        } else {
          (false, event)
        }
      }

      private def matchResult(matchType: Boolean, matcher: Matcher): Boolean = {
        if (matchType) {
          matcher.find()
        } else {
          matcher.matches()
        }
      }
    }


    case class RegexFindConfig(pattern: String,
                               fields: Map[Int, String],
                               matchType: Boolean = true,
                               trim: Boolean = true)

    class OrRegexItemService(override val config: RegexConfig)
      extends RegexItemService(config) {
      private val findServices = Try {
        config.findConfigs.map(findConfig => {
          new RegexFindService(findConfig)
        }).filter(_.valid())
      }.getOrElse(Nil)

      override def valid(): Boolean = {
        if (findServices == null || findServices.isEmpty) {
          return false
        }
        true
      }

      override def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        val value: String = event.get(config.field) match {
          case Some(x: Any) =>
            x.toString
          case None => null
        }
        if (value == null) {
          return event
        }
        for (findService <- findServices) Try {
          val res = findService.process(value, event)
          if (res != null && res._1) { // 正则匹配了，返回查找到的结果
            return event
          }
        }
        // 未匹配到，原始返回
        event
      }.getOrElse(event)
    }


    class AndRegexItemService(override val config: RegexConfig)
      extends RegexItemService(config) {
      private val findServices = Try {
        config.findConfigs.map(findConfig => {
          new RegexFindService(findConfig)
        }).filter(_.valid())
      }.getOrElse(Nil)

      override def valid(): Boolean = {
        if (findServices == null || findServices.isEmpty) {
          return false
        }
        true
      }

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        val value: String = event.get(config.field) match {
          case Some(x: Any) =>
            x.toString
          case None => null
        }
        if (value == null) {
          return event
        }
        for (findService <- findServices) Try {
          findService.process(value, event)
        }
        event
      }.getOrElse(event)
    }


    import com.haima.sage.bigdata.etl.common.model.RichMap

    import scala.collection.mutable
    import scala.util.Try


    case class ReplaceService(val configs: ReplaceConfig*) {
      private val services = ReplaceServiceFactory.factory(configs)

      def valid(): Boolean = services.nonEmpty

      def process(event: RichMap): RichMap = Try {
        if (services.length == 1) {
          services.head.process(event)
        } else {
          val map = new mutable.HashMap[String, Any]()
          map ++= event
          val dst = process(map)
          event ++ dst
        }

      }.getOrElse(event)

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        services.foreach(service => {
          service.process(event)
        })
        event
      }.getOrElse(event)
    }

    object ReplaceServiceFactory {
      def factory(configs: Seq[ReplaceConfig]): Seq[ReplaceItemService] = Try {
        configs.map(new ReplaceItemService(_))
      }.getOrElse(Nil)
    }

    class ReplaceItemService(val config: ReplaceConfig) {
      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        val value: String = event.get(config.field) match {
          case Some(x: Any) =>
            x.toString
          case None =>
            return event
        }

        event += (config.field -> value.replace(config.from, config.to))
      }.getOrElse(event)

      def process(event: RichMap): RichMap = Try {
        val value: String = event.get(config.field) match {
          case Some(x: Any) =>
            x.toString
          case None =>
            return event
        }
        event + (config.field -> value.replace(config.from, config.to))
      }.getOrElse(event)
    }

    case class ReplaceConfig(field: String, from: String, to: String = "")


    import java.util.regex.{Matcher, Pattern}

    import com.haima.sage.bigdata.etl.common.model.RichMap

    import scala.collection.mutable
    import scala.collection.mutable.ListBuffer
    import scala.util.Try

    /**
      *
      * @param field   数据字段
      * @param configs 文本查找配置
      * @param attrs   文本替换配置
      * @param cleans  文本清理配置
      */
    class TextMatchService(val field: String,
                           val configs: Seq[TextConfig],
                           val attrs: Seq[XmlToTextConfig] = Nil,
                           val cleans: Seq[String] = Nil,
                           val trim: Boolean = true) {
      private val separator = "\n"
      private val indices: Seq[TextProcess] = Try {
        configs.map(_.service(separator))
      }.getOrElse(Nil)

      private val pattern = Pattern.compile(separator)
      private val needXmlToText = attrs != null && attrs.nonEmpty
      private val xmlToTextService = if (needXmlToText) {
        new XmlToTextService(field, attrs, separator)
      } else {
        null
      }

      def process(event: RichMap): RichMap = Try {
        val map = new mutable.HashMap[String, Any]()
        map ++= event
        val dst = process(map)
        RichMap(dst.toMap)
      }.getOrElse(event)

      def process(event: mutable.Map[String, Any]): mutable.Map[String, Any] = Try {
        if (indices.isEmpty) {
          return event
        }
        var value: String = event.get(field) match {
          case Some(x: Any) =>
            x.toString
          case None =>
            return event
        }
        if (needXmlToText) {
          value = xmlToTextService.process(value)
        }
        if (cleans.nonEmpty) {
          cleans.foreach(clean => {
            value = value.replace(clean, "")
          })
        }
        val lines: Seq[String] = split(value)
        var dst = new mutable.HashMap[String, Any]()
        for (item <- indices) {
          Try {
            val res = item.process(lines)
            if (res != null) {
              dst += (item.config.index -> res)
            }
          }
        }
        event ++ dst
      }.getOrElse(event)

      /**
        * 忽略 空白行
        *
        * @param raw 欲拆分的文本
        * @return
        */
      def split(raw: String): Seq[String] = {
        val res = new ListBuffer[String]
        val m = pattern.matcher(raw)
        var index = 0
        var key = 1
        while (m.find()) {
          val value = raw.substring(index, m.start)
          append(res, value)
          index = m.end
          key += 1
        }
        if (index > 0) {
          if (index != raw.length - 1) {
            val value = raw.substring(index, raw.length)
            append(res, value)
          }
        }
        res
      }

      /**
        * 忽略空白行
        *
        * @param res   结果集合
        * @param value 单行数据
        * @return
        */
      def append(res: ListBuffer[String], value: String): ListBuffer[String] = {
        if (value == null) {
          return res
        }
        var dst = value
        if (trim) {
          dst = value.trim
        }
        if (dst.isEmpty) {
          return res
        }
        res += dst
      }
    }

    /**
      *
      * @param anchor     锚点, 正则
      * @param offset     偏移, 0 自身, >0 向后, <0 向前
      * @param matchType  正则匹配类型：  true 全部匹配（默认），false 查找
      * @param firstGroup offset=0时, 是否返回第一个匹配的项目
      */
    case class TextFindConfig(anchor: String,
                              offset: Int = 0,
                              matchType: Boolean = true,
                              firstGroup: Boolean = true
                             )

    /**
      * 根据锚点和位移 查找数据
      *
      * @param config 配置数据
      */
    class TextFindProcess(val config: TextFindConfig) {
      private val regex = Pattern.compile(config.anchor)

      /**
        * 向后查找最近锚点
        *
        * @param lines           行数据
        * @param startLineNumber 行号
        * @param stopLineNumber  -1不限制
        * @return (Int pattern行号, Int offset行号)未找到，返回 null
        */
      def anchorLineNumberAfter(lines: Seq[String], startLineNumber: Int, stopLineNumber: Int = -1): (Int, Int) = {
        anchorLineNumber(lines, startLineNumber, stopLineNumber, after = true)
      }

      /**
        * 向前/后查找最近锚点
        *
        * @param lines           行数据
        * @param startLineNumber 行号
        * @param stopLineNumber  -1不限制
        * @return (Int pattern行号, Int offset行号)未找到，返回 null
        */
      def anchorLineNumberBefore(lines: Seq[String], startLineNumber: Int, stopLineNumber: Int = -1): (Int, Int) = {
        anchorLineNumber(lines, startLineNumber, stopLineNumber, after = false)
      }

      /**
        * 向前查找最近锚点
        *
        * @param lines           行数据
        * @param startLineNumber 行号
        * @return (Int pattern行号, Int offset行号)未找到，返回 null
        */
      def anchorLineNumber(lines: Seq[String], startLineNumber: Int, stopLineNumber: Int, after: Boolean): (Int, Int) = {
        val range = if (after) {
          val limit = if (stopLineNumber < 0) lines.length else 0
          startLineNumber until limit
        } else {
          val limit = if (stopLineNumber < 0) 0 else stopLineNumber
          (limit until startLineNumber).reverse
        }
        for (i <- range) {
          val line = lines(i)
          val matcher = regex.matcher(line)
          if (isMatch(matcher)) {
            return (i, i + config.offset)
          }
        }
        null
      }

      def isMatch(matcher: Matcher): Boolean = {
        if (config.matchType) {
          matcher.matches()
        } else {
          matcher.find()
        }
      }

      def doMatch(lines: Seq[String], line: String, lineNumber: Int): (Boolean, String) = {
        val matcher = regex.matcher(line)
        if (isMatch(matcher)) {
          if (config.offset == 0) {
            val value = if (config.firstGroup) {
              matcher.group(1)
            } else {
              line
            }
            return (true, value)
          } else {
            val dstIndex = lineNumber + config.offset
            if (dstIndex < -1 || dstIndex > lines.size) {
              return (true, null)
            } else {
              return (true, lines(dstIndex))
            }
          }
        }
        (false, null)
      }

      /**
        * @param lines 行数据
        * @return 未找到，返回null
        */
      def process(lines: Seq[String], startLineNumber: Int): (String, Int) = {
        for (i <- startLineNumber until lines.length) {
          val line = lines(i)
          val res = doMatch(lines, line, i)
          if (res._1) {
            return (res._2, i)
          }
        }
        null
      }

    }

    object TextFindType extends Enumeration {
      // 单锚点
      val Single: TextFindType.Value = Value("s")
      // 两锚点之间
      val Between: TextFindType.Value = Value("b")
      val Up: TextFindType.Value = Value("u")
      val Down: TextFindType.Value = Value("d")

      def checkExists(day: String): Boolean = this.values.exists(_.toString == day) //检测是否存在此枚举值
      def showAll(): Unit = this.values.foreach(println) // 打印所有的枚举值
    }


    /**
      *
      * @param index    字段名称
      * @param findType 查找方式
      * @param trim     清除空白
      */
    abstract class TextConfig(val index: String,
                              val findType: TextFindType.Value,
                              val trim: Boolean = true,
                              val findTimes: Int = 10) {
      def service(separator: String): TextProcess
    }

    case class TextSingleConfig(override val index: String,
                                anchor: TextFindConfig,
                                pattern: Option[String] = None,
                                override val trim: Boolean = true,
                                override val findTimes: Int = 10
                               )
      extends TextConfig(index, TextFindType.Single, trim, findTimes) {
      override def service(separator: String): TextProcess = Try {
        TextSingleProcess(this, separator)
      }.getOrElse(null)
    }

    case class TextBetweenConfig(override val index: String,
                                 firstAnchor: TextFindConfig,
                                 secondAnchor: TextFindConfig,
                                 override val trim: Boolean = true,
                                 override val findTimes: Int = 10,
                                 singleNode: Boolean = true
                                )
      extends TextConfig(index, TextFindType.Single, trim, findTimes) {
      override def service(separator: String): TextProcess = Try {
        if (singleNode) {
          new TextBetweenSingleProcess(this, separator)
        } else {
          new TextBetweenMultiProcess(this, separator)
        }
      }.getOrElse(null)
    }

    case class TextUpConfig(override val index: String,
                            firstAnchor: TextFindConfig,
                            secondAnchor: TextFindConfig,
                            override val trim: Boolean = true,
                            override val findTimes: Int = 10
                           )
      extends TextConfig(index, TextFindType.Single, trim, findTimes) {
      override def service(separator: String): TextProcess = Try {
        TextUpProcess(this, separator)
      }.getOrElse(null)
    }

    case class TextDownConfig(override val index: String,
                              firstAnchor: TextFindConfig,
                              secondAnchor: TextFindConfig,
                              override val trim: Boolean = true,
                              override val findTimes: Int = 10
                             )
      extends TextConfig(index, TextFindType.Single, trim, findTimes) {
      override def service(separator: String): TextProcess = Try {
        DownTextProcess(this, separator)
      }.getOrElse(null)
    }

    abstract class TextProcess(val config: TextConfig, val separator: String) {
      protected val findTimes: Int = if (config.findTimes > 0) config.findTimes else 10

      def process(lines: Seq[String]): Any
    }

    case class TextSingleProcess(override val config: TextSingleConfig,
                                 override val separator: String)
      extends TextProcess(config, separator) {
      private val textFindProcess = new TextFindProcess(config.anchor)
      private val pattern = Try {
        config.pattern match {
          case Some(x) =>
            Pattern.compile(x)
          case None =>
            null
        }
      }.getOrElse(null)

      def process(lines: Seq[String]): String = {
        var start = 0
        for (_ <- 0 to findTimes) {
          val res = find(lines, start)
          if (res == null) {
            return null
          }
          if (res._2 == -1) {
            return res._1
          }
          if (res._1 != null) {
            if (pattern == null) {
              return res._1
            }
            if (pattern.matcher(res._1).matches()) {
              return res._1
            }
          }
          start = res._2 + 1
        }
        null
      }

      def find(lines: Seq[String], start: Int): (String, Int) = {
        textFindProcess.process(lines, start)
      }
    }

    /**
      * 最近的两个锚点
      *
      * @param config 锚点配置
      */
    class TextBetweenSingleProcess(override val config: TextBetweenConfig,
                                   override val separator: String)
      extends TextProcess(config, separator) {
      private val firstTextFindProcess = new TextFindProcess(config.firstAnchor)
      private val secondTextFindProcess = new TextFindProcess(config.secondAnchor)

      def process(lines: Seq[String]): Any = {
        var start = 0
        for (_ <- 0 to findTimes) {
          val res = find(lines, start)
          if (res == null) {
            return null
          }
          if (res._2 == -1) {
            return res._1
          }
          if (res._1 != null) {
            return res._1
          }
          start = res._2 + 1
        }
        null
      }

      /**
        * 查找两个锚点之间的数据，必须：起始锚点的位置>=结束锚点的位置
        *
        * @param lines 行数据
        * @param start 起始点
        * @return (String 结果, Int pattern位置,  Int offset位置)
        */
      def find(lines: Seq[String], start: Int): (String, Int, Int) = {
        val line1 = firstTextFindProcess.anchorLineNumberAfter(lines, start)
        if (line1 == null) {
          return null
        }
        val line2 = secondTextFindProcess.anchorLineNumberAfter(lines, line1._2)
        if (line2 == null) {
          return null
        }
        if (line1._1 > line2._1) { // 必须: 开始锚点的位置 <= 结束锚点的位置 (混乱数据)
          return (null, line2._1, line2._2)
        }
        if (line1._2 > line2._2) { // 必须：开始锚点offset <= 结束锚点offset (空数据)
          return (null, line2._1, line2._2)
        }
        if (line1._2 == line2._2) { // 单行：开始锚点offset = 结束锚点offset
          // 最后一个锚点
          return (between(lines, line1._2, line2._2), line2._2, line2._1)
        }
        val line3 = firstTextFindProcess.anchorLineNumberBefore(lines, line2._1, line1._1)
        val from = if (line3 != null && line3._2 > line1._2) {
          line3._2
        } else {
          line1._2
        }
        (between(lines, from, line2._2), line2._1, line2._2)
      }

      /**
        *
        * @param lines 行数据集
        * @param start 开始, 包括本行
        * @param stop  结束, 包括本行
        * @return
        */
      private def between(lines: Seq[String], start: Int, stop: Int): String = {
        (start to stop).map(lines(_)).mkString(separator)
      }
    }

    /**
      * 最近的两个锚点得到一条数据
      * 得到多条数据
      *
      * @param config 锚点配置
      */
    class TextBetweenMultiProcess(override val config: TextBetweenConfig,
                                  override val separator: String)
      extends TextBetweenSingleProcess(config, separator) {
      override def process(lines: Seq[String]): Any = {
        var list = new ListBuffer[String]()

        var start = 0
        var flag = true
        while (flag) {
          val res = find(lines, start)
          if (res == null) {
            return list
          }
          if (res._2 != -1 && res._1 != null) {
            list += res._1
          }
          start = res._2 + 1
          flag = if (start >= lines.length) {
            false
          } else {
            true
          }
        }
        list
      }
    }

    case class TextUpProcess(override val config: TextUpConfig,
                             override val separator: String)
      extends TextProcess(config, separator) {
      private val firstTextFindProcess = new TextFindProcess(config.firstAnchor)
      private val secondTextFindProcess = new TextFindProcess(config.secondAnchor)

      def process(lines: Seq[String]): String = {
        val line1 = firstTextFindProcess.anchorLineNumberAfter(lines, 0)
        if (line1 == null) {
          return null
        }
        val base = line1._2
        for (i <- (0 to base).reverse) {
          val line = lines(i)
          val res = secondTextFindProcess.doMatch(lines, line, i)
          if (res._1) {
            return res._2
          }
        }
        null
      }
    }

    case class DownTextProcess(override val config: TextDownConfig,
                               override val separator: String)
      extends TextProcess(config, separator) {
      private val firstTextFindProcess = new TextFindProcess(config.firstAnchor)
      private val secondTextFindProcess = new TextFindProcess(config.secondAnchor)

      def process(lines: Seq[String]): String = {
        val line1 = firstTextFindProcess.anchorLineNumberAfter(lines, 0)
        if (line1 == null) {
          return null
        }
        val start = line1._2
        val stop = lines.size - 1
        for (i <- start to stop) {
          val line = lines(i)
          val res = secondTextFindProcess.doMatch(lines, line, i)
          if (res._1) {
            return res._2
          }
        }
        null
      }
    }


    import java.io.ByteArrayInputStream

    import com.ctc.wstx.api.WstxInputProperties
    import com.ctc.wstx.stax.WstxInputFactory
    import com.fasterxml.jackson.core.JsonToken
    import com.fasterxml.jackson.dataformat.xml.XmlFactory
    import com.fasterxml.jackson.dataformat.xml.deser.FromXmlParser
    import com.haima.sage.bigdata.etl.common.model.RichMap
    import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

    import scala.collection.mutable.ListBuffer
    import scala.util.{Failure, Success, Try}

    class XmlToTextService(val field: String,
                           val attrs: Seq[XmlToTextConfig],
                           separator: String = "\n") {


      def process(event: RichMap): RichMap = Try {
        val value: String = event.get(field) match {
          case Some(x: Any) =>
            x.toString
          case None => null
        }
        if (value == null) {
          return event
        }
        val data = process(value)
        event + (field -> data)
      }.getOrElse(event)

      def process(value: String): String = {
        val parser = readXml(value)
        parse(parser)
      }

      private def parse(parser: FromXmlParser): String = Try {
        val dst = new ListBuffer[String]
        var flag = true
        while (flag) {
          val token: JsonToken = parser.nextToken()
          token match {
            case null | JsonToken.NOT_AVAILABLE =>
              flag = false
            case JsonToken.VALUE_STRING =>
              val name: String = parser.getCurrentName
              attrs.find(_.attr == name) match {
                case Some(item) =>
                  var value = parser.getValueAsString
                  if (value != null && value.nonEmpty) {
                    value = value.replace("\n", "\\n")
                    value = value.replace("\t", "\\t")
                    if (item.withName) {
                      dst += item.attr + item.separator + value
                    } else {
                      dst += value
                    }
                  }
                case _ =>
              }
            case _ =>
          }
        }
        dst.mkString(separator)
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          exception.printStackTrace()
          null
      }

      val factory: XmlFactory = {
        val factory = new XmlFactory()
        factory
      }
      val inputFactory: XMLInputFactory = {
        val wstxInputFactory = new WstxInputFactory()
        wstxInputFactory.setProperty(WstxInputProperties.P_MAX_ATTRIBUTE_SIZE, 32000)
        wstxInputFactory.setProperty(WstxInputProperties.P_MAX_ELEMENT_DEPTH, 32000)
        wstxInputFactory.setProperty(WstxInputProperties.P_MAX_ENTITY_DEPTH, 32000)
        wstxInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false)
        wstxInputFactory
      }

      private def readXml(value: String): FromXmlParser = {
        val reader: XMLStreamReader = inputFactory.createXMLStreamReader(new ByteArrayInputStream(value.getBytes))
        implicit val parser: FromXmlParser = factory.createParser(reader)
        parser
      }
    }

    case class XmlToTextConfig(attr: String,
                               withName: Boolean = false,
                               separator: String = "=")


    import java.text.SimpleDateFormat
    import java.util.Date
    import java.util.regex.Pattern

    import com.haima.sage.bigdata.etl.common.model.RichMap

    import scala.collection.mutable
    import scala.util.{Failure, Success, Try}

    object GuajiCarInfoUtils {
      private val field = "data"
      private val textService = new TextMatchService(field, Seq(
        TextDownConfig("car_name",
          TextFindConfig("车源号\\s*(.+)"), // 车主报价，详情
          TextFindConfig("(^\\s?.*\\d{4}款.*)", matchType=false),
          trim = false
        ),
        TextSingleConfig("source_id", TextFindConfig("车源号\\s*(.+)")),
        TextSingleConfig("state_tmp", TextFindConfig(".*(没有找到相关数据).*")),
        TextSingleConfig("state_tmp", TextFindConfig(".*(全部车源信息).*")),
        TextSingleConfig("service_subtitle", TextFindConfig("服务保障", 1)),
        TextSingleConfig("registration_date", TextFindConfig("上牌时间", 1)),
        TextSingleConfig("apparent_mileage", TextFindConfig("表显里程", 1)),
        TextSingleConfig("plate_address", TextFindConfig("车牌所在地", 1)),
        TextSingleConfig("car_area", TextFindConfig("看车地点", 1))
        // TextSingleConfig("car_gear", TextFindConfig("变速箱", 1)),
        // TextSingleConfig("emission_standards", TextFindConfig("迁入地为准", 1)), // 排放标准
        // TextSingleConfig("engine_exhaust", TextFindConfig("排放量", 1)),
        // TextSingleConfig("change_times", TextFindConfig("登记证为准", 1)),  // 过户次数
        // TextSingleConfig("annual_inspection_expiry", TextFindConfig("年检到期", 1)), // 年检
        // TextSingleConfig("jqx_expiry", TextFindConfig("交强险到期", 1)),  // 交强险
        // TextSingleConfig("commercial_insurance", TextFindConfig("商业险到期", 1))  // 交强险
      ),
        attrs = Seq(XmlToTextConfig("text")),
        cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n"),
        trim = false
      )
      private val replaceService = ReplaceService(
        ReplaceConfig("apparent_mileage", "公里")
        // ReplaceConfig("change_times", "次过户"),
        // ReplaceConfig("engine_exhaust", "L")
        // ReplaceConfig("commercial_insurance", "已过期", "1970.1")
      )

      private val regexService = new RegexService(
        new RegexConfig("service_subtitle",
          "服务费约{0,1}(\\d+)元.*",
          Map[Int, String](1 -> "service_charge")),
        new RegexConfig("service_subtitle",
          "服务费不超过(\\d+)%.*",
          Map[Int, String](1 -> "service_percentage")),
        new RegexConfig("schema",
          ".*;link_uid=(.+)/.*",
          Map[Int, String](1 -> "car_uid")),
        new RegexConfig("schema",
          ".*S.puid=(.+);.*",
          Map[Int, String](1 -> "car_uid"))
      )
      private val numberService = new NumberService(
        NumberItemConfig("apparent_mileage", "wan")
        // NumberItemConfig("price", "wan"),
        // NumberItemConfig("down_payment", "wan")
      )

      private val conditionService = ConditionService(
        ConditionConfig("car_name", "^\\s.*",index="is_strict", value=1),
        ConditionConfig("state_tmp","没有找到相关数据", index="state", value=1),
        ConditionConfig("state_tmp", "全部车源信息", index="state", value=2),
        ConditionConfig(index="service_percentage", value=0),
        ConditionConfig(index="state", value=0), //
        ConditionConfig(index="is_strict", value=0),
        ConditionConfig(index="is_new", value=0)
      )

      private val mergeService = MergeService(
        MergeConfig("@es_id", Seq("meta_app_name", "meta_table_name", "car_uid"))
      )

      def process(event: RichMap): RichMap = Try {
        var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
        dst ++= event
        dst += ("meta_app_name" -> "guaji")
        dst += ("meta_table_name" -> "car_info")
        dst = textService.process(dst)
        dst = replaceService.process(dst)
        dst = regexService.process(dst)
        setCarUid(dst)
        dst = numberService.process(dst)
        dst = conditionService.process(dst)
        dst = mergeService.process(dst)
        setChargeValue(dst)
        setHdfsPath(dst)
        RichMap(dst.toMap)
      } match {
        case Success(value) =>
          value
        case Failure(exception) =>
          exception.printStackTrace()
          event
      }

      // 某些服务类里无法完成的任务
      def setChargeValue(event: mutable.Map[String, Any]): Unit = {
        val value: String = event.getOrElse("service_subtitle", "").toString
        val pattern = Pattern.compile("服务费不超过(\\d+)%\\.*")
        val m = pattern.matcher(value)
        if (m.find()){
          val p = m.group(1).toInt
          val v = event.getOrElse("price", 0).toString.toInt * p / 100
          event += ("service_charge" -> v)
        }
      }

      def setHdfsPath(event: mutable.Map[String, Any]): Unit = {
        var data_generate_time = event.getOrElse("timestamp",
          new Date().getTime.toString)
        val hdfsTime = new SimpleDateFormat("yyyy-MM-dd/HH")
          .format(new Date(data_generate_time.toString.toLong))
        val hdfsPath = s"/data/guaji/origin/car_info/${hdfsTime}/data"
        event += ("@hdfs_path" -> hdfsPath)
      }

      val rd = new util.Random
      def setCarUid(event: mutable.Map[String, Any]): Unit = {
        val value: String = event.getOrElse("car_uid", "").toString
        if (value.nonEmpty) {
          event += ("car_uid" -> value.replaceAll("/.*",""))
        } else {
          event += ("car_uid" -> s"${rd.nextInt(10000)}unknown")
        }
      }

    }
    GuajiCarInfoUtils.process(event)

  }
}
