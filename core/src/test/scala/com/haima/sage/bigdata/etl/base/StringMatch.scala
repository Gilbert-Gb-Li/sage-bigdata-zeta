package com.haima.sage.bigdata.etl.base

object Email {
  def unapply(str: String) = """(.*)@(.*)""".r
    .unapplySeq(str).get match {
    case user :: domain :: Nil => Some(user, domain)
    case _ => None
  }
}



object Brackets {
  def unapply(str: String) =
    """(.*)\%\{(.*)\}(.*)""".r.unapplySeq(str) match {
      case Some(start :: brackets :: end :: Nil) => Some(start, brackets, end)
      case _ => None
    }
}

object StringMatch extends App {

  import scala.collection.mutable.WeakHashMap

  val cache = new WeakHashMap[Int, Int]

  def memo(f: Int => Int) = (x: Int) => {
    cache.get(x) match {
      case None =>
        f(x)
      case Some(v) =>
        print(v)
        v
    }

    cache.getOrElseUpdate(x, f(x))
  }


  def fibonacci_(in: Int): Int = in match {
    case 0 => 0;
    case 1 => 1;
    case n: Int => fibonacci_(n - 2) + fibonacci_(n - 1)
  }

  def fibonacci = memo(fibonacci_)

  "user@domain.com" match {
    case Email(user, domain) => println(user + "@" + domain)
  }
  "test" match {
    case Brackets(start, brackets, end) => println(brackets)
    case _ =>
  }
  /*

    val t1 = System.currentTimeMillis()
    println(fibonacci_(42))
    println("fibonacci_ takes " + (System.currentTimeMillis() - t1) + "ms")

    val t2 = System.currentTimeMillis()
    println(fibonacci(42))
    println("fibonacci takes " + (System.currentTimeMillis() - t2) + "ms")
  */

  val regex = "%\\{(?<name>(?<pattern>[A-z0-9]+)(?::(?<subname>[A-z0-9_:@]+))?)(?:=(?<definition>(?:(?:[^{}]+|\\.+)+)+))?\\}".r


  regex.findAllMatchIn("APACHE %{IPORHOST:c_ip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:@timestamp}\\] \\\"(?:%{WORD:method} %{NOTSPACE:request}(?: HTTP/%{NUMBER:httpversion})?|%{DATA:rawrequest})\\\" %{NUMBER:response} (?:%{NUMBER:bytes}|-)( %{QS:referrer} %{QS:agent} %{NUMBER:read} %{NUMBER:write})?").foreach(m => {
    println(m.groupCount)

    (0 until m.groupCount).foreach(i =>
      println(m.group(i))
    )

  })


}


