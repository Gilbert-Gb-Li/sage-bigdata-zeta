package com.haima.sage.bigdata.etl.base

import org.junit.Test

/**
 * Created by zhhuiyan on 15/3/20.
 */
class System {
   @Test
    def name(): Unit ={
     println(sys.props("os.name"))
     assert(!sys.props("os.name").contains("Windows"))
    }


}
