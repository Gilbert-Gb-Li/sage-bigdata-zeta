package com.haima.sage.bigdata.analyzer.preprocessing.modeling

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.WordSegmentationAnalyzer
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.ModelingAnalyzerProcessor
import org.apache.flink.api.scala._
import org.junit.Test

/**
  * Created by ASUS on 2018/2/9.
  */
class ModelingWordSegmentationAnalyzerTest {
    val env = ExecutionEnvironment.getExecutionEnvironment
    Constants.init("sage-analyzer-preproc.conf")

    @Test
    def test(): Unit = {

      val conf = ReAnalyzer(Some(WordSegmentationAnalyzer("cs-uri-stem", SplitTokenizer("[/\\.]"))))
        val processor = ModelingAnalyzerProcessor(conf)
        val ds: DataSet[RichMap] = env.fromElements(
            Map(
                "cs-uri-stem" ->"/CmbBank_CreditCardV2/UI/Base/doc/Scripts/BaseFunc.js",
                "s-computername"->"PBSZ-A",
                "cs-host"->"pbsz.ebank.cmbchina.com"
            ),
            Map("h" -> 0.56, "b" -> -38, "c" -> 0.39, "d" -> 103))
      assert(  processor.process(ds).head.collect()(0).get("cs-uri-stem_terms").orNull.isInstanceOf[Vector[_]])
        assert(  processor.process(ds).head.collect()(0).get("cs-uri-stem_terms").orNull.asInstanceOf[Vector[_]].length==7)
    }

}
