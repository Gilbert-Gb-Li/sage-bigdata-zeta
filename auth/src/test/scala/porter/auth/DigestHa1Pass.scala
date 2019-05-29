package porter.auth

import org.junit.Test
import porter.model.{DigestHa1, Ident}
import porter.util.Hash

/**
  * Created by zhhuiyan on 2017/5/5.
  */
class DigestHa1Pass {

  @Test
  def mkPass(): Unit ={

   println("digestmd5.0:"+Hash.md5String("sage:sage:test"))
    println(DigestHa1(Ident("sage"),"sage","test"))
  }

}
