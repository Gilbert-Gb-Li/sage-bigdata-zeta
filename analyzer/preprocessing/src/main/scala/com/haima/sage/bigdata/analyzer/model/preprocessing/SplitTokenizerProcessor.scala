package com.haima.sage.bigdata.analyzer.model.preprocessing

import com.haima.sage.bigdata.etl.common.model.filter.SplitTokenizer

class SplitTokenizerProcessor(override val tokenizer:SplitTokenizer) extends TokenizerProcessor[SplitTokenizer]{
    override def process(value: String): Array[String] = value.split(tokenizer.separator).filter(_!="")
}
