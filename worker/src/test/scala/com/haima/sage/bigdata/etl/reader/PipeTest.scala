package com.haima.sage.bigdata.etl.reader

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, DeadLetter}
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model._
import org.junit.Test

import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 15/2/6.
  */
class PipeTest {
  implicit val timeout = Timeout(50 seconds)
  final lazy val system = ActorSystem("worker", CONF.resolve())
  lazy val reader: AkkaLogReader = new AkkaLogReader(NetSource(Some(Akka()), Some("127.0.0.1"), 19093), system)

  new Thread() {
    override def run(): Unit = {
      reader.foreach(println)
    }
  }.start()


  @Test
  def test(): Unit = {

    def send(): Unit = {
      (1 to 10).foreach {
        i =>
          ActorSystem().actorSelection("/user/publisher") ! Map[String, Any](
            "test" -> i,
            "test2" -> "2"
          )
          TimeUnit.SECONDS.sleep(1)
      }
    }

    send()
    reader.close()
  }


}

object PipeBox {
  var flag = true
}

class PipeDeadLetterListener extends Actor {
  def receive = {
    case DeadLetter(msg: Status.Status, _, _) =>
      PipeBox.flag = false
    case DeadLetter(msg, _, _) =>
      println(s"FROM CUSTOM LISTENER $msg")

  }
}

