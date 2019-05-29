package com.haima.sage.bigdata.analyzer.model.preprocessing

import com.haima.sage.bigdata.etl.common.model.filter.Tokenizer

/**
  * Created by ASUS on 2018/2/9.
  */


trait TokenizerProcessor[T<:Tokenizer]{  //具体的实现  遵从API接口
    val tokenizer:T

    def process(value: String): Array[String]
}
