package com.haima.sage.bigdata.analyzer.model.preprocessing



import com.haima.sage.bigdata.etl.common.model.filter._





class AnsjTokenizerProcessor(override val tokenizer:AnsjTokenizer) extends TokenizerProcessor[AnsjTokenizer]{
    import org.ansj.splitWord.analysis.ToAnalysis

    import scala.collection.JavaConversions._

    override def process(value: String): Array[String] = {

        ToAnalysis.parse(value).getTerms.map(item=>{
            item.score()
            item.getName

        }).toArray
    }
}
