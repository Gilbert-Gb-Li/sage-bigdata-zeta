package com.haima.sage.bigdata.etl.stream

import java.util.concurrent.LinkedBlockingQueue

import org.junit.Test

class QueueStreamTest {
  @Test
  def add(): Unit = {
    val queue = new LinkedBlockingQueue[String](1000 + 1)
    val consumer = new Thread("read") {
      override def run(): Unit = {
        (0 to 1000).foreach(i => {
          if (queue.size() > 0) {
            Thread.sleep(10)
            println(queue.take())
          } else {
            Thread.sleep(10)
            if (queue.size() > 0)
              println(queue.take())
            else {
              println("no data")
            }
          }
        })

      }
    }
    val producer = new Thread("read") {
      override def run(): Unit = {
        (0 to 1000).foreach(i => {
          Thread.sleep(10)
          queue.add("" + i)
        })

      }
    }

    consumer.start()
    producer.start()
    consumer.join()
    producer.join()

    println("main thread end...")
  }
}
