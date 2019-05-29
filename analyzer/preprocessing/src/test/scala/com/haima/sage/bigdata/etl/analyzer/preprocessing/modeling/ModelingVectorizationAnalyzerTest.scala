package com.haima.sage.bigdata.analyzer.preprocessing.modeling

import java.util.{Calendar, Date}

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.SplitTokenizer
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.junit.Test

/**
  * Created by lenovo on 2017/11/16.
  */
class ModelingVectorizationAnalyzerTest {
  val env = ExecutionEnvironment.getExecutionEnvironment
  Constants.init("sage-analyzer-preproc.conf")

  @Test
  def testSimHash(): Unit = {

    val simHashVectorization = SimHashVectorization(32)
    val wordSegmentation = WordSegmentationAnalyzer("cs-uri-stem", SplitTokenizer("/"))
    val vectorization = VectorizationAnalyzer("cs-uri-stem_terms", simHashVectorization)
    val scalar = ScalarAnalyzer(Array(("vector", null), ("b", Atan())))

    val wordSegmentationProcessor = new ModelingWordSegmentationAnalyzer(wordSegmentation)
    val vectorizationProcessor = new ModelingVectorizationAnalyzer(vectorization)
    val scalarProcessor = new ModelingScalarAnalyzer(scalar)
    val ds: DataSet[RichMap] = env.fromElements(
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/UI/Base/doc/Scripts/BaseFunc1.js",
        "b" -> 0.48,
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/UI/Base/doc/Scripts/Scripts/BaseFunc2.js",
        "s-computername" -> "PBSZ-A",
        "b" -> 0.48,
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/UI/Base/Base/doc/Scripts/BaseFunc3.js",
        "s-computername" -> "PBSZ-A",
        "b" -> 0.48,
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/CreditCardV2/UI/Base/doc/Scripts/BaseFunc4.js",
        "s-computername" -> "PBSZ-A",
        "b" -> 0.48,
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map("a" -> 0.2, "h" -> 0.48, "c" -> 0.49, "d" -> 101),
      Map("a" -> 0.1, "b" -> -18, "c" -> 0.19, "f" -> 101),
      Map("a" -> 0.4, "b" -> 28, "g" -> 0.29, "d" -> 102),
      Map("h" -> 0.56, "b" -> -38, "c" -> 0.39, "d" -> 103))
    //wordSegmentationProcessor.action(ds).print()
    vectorizationProcessor.action(wordSegmentationProcessor.action(ds)).print()
    // scalarProcessor.action(vectorizationProcessor.action(ds)).print()

  }

  @Test
  def binary(): Unit = {
    println(500.toBinaryString)
    println(1521230000.toBinaryString)
  }

  @Test
  def testTFIDF(): Unit = {


    val wordSegmentation = WordSegmentationAnalyzer("cs-uri-stem", SplitTokenizer("/"))

    val tfidf = TFIDFVectorization()

    val vectorization = VectorizationAnalyzer("cs-uri-stem", tfidf)
    val scalar = ScalarAnalyzer(Array(("vector", null), ("b", Atan())))
    val wordSegmentationProcessor = new ModelingWordSegmentationAnalyzer(wordSegmentation)
    val vectorizationProcessor = new ModelingVectorizationAnalyzer(vectorization)
    val scalarProcessor = new ModelingScalarAnalyzer(scalar)
    val ds: DataSet[RichMap] = env.fromElements(
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/UI1/Base/doc/Scripts/BaseFunc3.js",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/UI/Base/doc1/Scripts3/BaseFunc.js",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV21/UI/Base/doc/Scripts/BaseFunc1.js",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "/CmbBank_CreditCardV2/UI/Base2/doc/Scripts3/BaseFunc.js",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map("a" -> 0.2, "h" -> 0.48, "c" -> 0.49, "d" -> 101),
      Map("a" -> 0.1, "b" -> -18, "c" -> 0.19, "f" -> 101),
      Map("a" -> 0.4, "b" -> 28, "g" -> 0.29, "d" -> 102),
      Map("h" -> 0.56, "b" -> -38, "c" -> 0.39, "d" -> 103))
    //vectorizationProcessor.action(wordSegmentationProcessor.action(ds)).writeAsText("./target/tmp.txt",WriteMode.OVERWRITE)
    scalarProcessor.action(vectorizationProcessor.action(wordSegmentationProcessor.action(ds))).writeAsText("./target/tmp.txt", WriteMode.OVERWRITE)
    env.execute()
  }

  @Test
  def testSomeWeight(): Unit = {


    val wordSegmentation = WordSegmentationAnalyzer("cs-uri-stem", SplitTokenizer("/"))

    val tfidf = SameWeightVectorization()

    val vectorization = VectorizationAnalyzer("cs-uri-stem", tfidf)
    val scalar = ScalarAnalyzer(Array(("vector", null), ("b", Atan())))
    val wordSegmentationProcessor = new ModelingWordSegmentationAnalyzer(wordSegmentation)
    val vectorizationProcessor = new ModelingVectorizationAnalyzer(vectorization)
    val scalarProcessor = new ModelingScalarAnalyzer(scalar)
    val ds: DataSet[RichMap] = env.fromElements(
      Map(
        "cs-uri-stem" -> "A",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "B",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "A",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map(
        "cs-uri-stem" -> "C",
        "s-computername" -> "PBSZ-A",
        "cs-host" -> "pbsz.ebank.cmbchina.com"
      ),
      Map("a" -> 0.2, "h" -> 0.48, "c" -> 0.49, "d" -> 101),
      Map("a" -> 0.1, "b" -> -18, "c" -> 0.19, "f" -> 101),
      Map("a" -> 0.4, "b" -> 28, "g" -> 0.29, "d" -> 102),
      Map("h" -> 0.56, "b" -> -38, "c" -> 0.39, "d" -> 103))
    //vectorizationProcessor.action(wordSegmentationProcessor.action(ds)).writeAsText("./target/tmp.txt",WriteMode.OVERWRITE)
    // scalarProcessor.action(vectorizationProcessor.action(ds)).print()
    vectorizationProcessor.action(ds).print()

  }

  @Test
  def dateTest(): Unit = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    val data: Seq[RichMap] = (0 to 10).map(i => {
      cal.add(Calendar.DAY_OF_MONTH, 1)
      RichMap(Map("field" -> cal.getTime))
    }).toSeq
    val tfidf = SameWeightVectorization()

    val vectorization = VectorizationAnalyzer("field", tfidf)
    val vectorizationProcessor = new ModelingVectorizationAnalyzer(vectorization)
    vectorizationProcessor.action(env.fromElements(data: _*)).print()

  }


  @Test
  def scalarTest() {
    val vectorization = VectorizationAnalyzer("primary_key", SameWeightVectorization())
    val scalar = ScalarAnalyzer(Array("ids_number" -> Atan(), "ods_number" -> Atan()))

    val regression = RegressionAnalyzer(1, 0.002, 100, 0.001, "usedtime", "vector")


    val vectorizationProcessor = new ModelingVectorizationAnalyzer(vectorization)
    val scalarProcessor = new ModelingScalarAnalyzer(scalar)

    val data:DataSet[RichMap] = env.readTextFile("/Users/zhhuiyan/Downloads/local-json-sql.json").map(new Mapper() {}.mapper.readValue[Map[String, Any]](_))
    //vectorizationProcessor.action(data).print()
    scalarProcessor.action(vectorizationProcessor.action(data)).print()
  }

  @Test
  def testSplit(): Unit = {
    val s = "10102030405060710100"
    s.split("").foreach(println)
  }


}
