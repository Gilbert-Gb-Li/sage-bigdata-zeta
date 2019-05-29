package com.haima.sage.bigdata.etl.driver

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{KafkaWriter, Usability, UsabilityChecker}
import org.junit.Test

import scala.util.{Failure, Success, Try}

class DriverMateTest {
  @Test
  def isInstance(): Unit ={
  assert(KafkaWriter(hostPorts = "127.0.0.1").isInstanceOf[DriverMate],"sub class must be a class")
  }


}
