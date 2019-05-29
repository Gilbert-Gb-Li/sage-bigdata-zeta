package com.haima.sage.bigdata.etl.normalization

import java.util.Date

import com.haima.sage.bigdata.etl.normalization.format.DefaultTranslator
import org.junit.Test

import scala.util.Try

class DefaultTranslatorTest {

  @Test
  def dateTest(): Unit = {
    val current=System.currentTimeMillis()*1000
  println(current)
    println( Math.floor(Math.log10(current)).toLong)
    println( Math.pow(10,Math.floor(Math.log10(current)).toLong-12))
    println(new Date(current/ Math.pow(10,Math.log10(current).toLong-12).toLong ) )
  }

  @Test
  def dateEQTest(): Unit = {
    val current=System.currentTimeMillis()
   assert(current>1e12)
  }

  @Test
  def doubleTest(): Unit = {
    val translator = DefaultTranslator("double", _.toString)
    assert(translator.parse("1").equals(1.0))
    assert(translator.parse("1.2").equals(1.2))
    assert(translator.parse("1.2E2").equals(120.0))
    assert(translator.parse(".2E2").equals(20.0))
    assert(translator.parse("0.2E2").equals(20.0))
    assert(translator.parse("0.02E2").equals(2.0))
    assert(translator.parse("2E-2").equals(0.02))
  }

  @Test
  def floatTest(): Unit = {
    val translator = DefaultTranslator("float", _.toString)

    println(translator.parse("1.00000000000000001E-12"))
    println(1.00000000000000001E-12f)
    assert(translator.parse("1.00000000000000001E-12").equals(1.00000000000000001E-12f))
    assert(translator.parse("1").equals(1.0f))
    assert(translator.parse("1.2").equals(1.2f))
    assert(translator.parse("1.2E2").equals(120.0f))
    assert(translator.parse(".2E2").equals(20.0f))
    assert(translator.parse("0.2E2").equals(20.0f))
    assert(translator.parse("0.02E2").equals(2.0f))
    assert(translator.parse("2E-2").equals(0.02f))
  }

  @Test
  def intTest(): Unit = {
    val translator = DefaultTranslator("integer", _.toString)
    assert(Try(translator.parse("100000000000")).isFailure)
    assert(translator.parse("1").equals(1))
    assert(translator.parse("1.0").equals(1))
    assert(translator.parse("1.2E2").equals(120))
    assert(translator.parse(".2E2").equals(20))
    assert(translator.parse("0.2E2").equals(20))
    assert(translator.parse("0.02E2").equals(2))
    assert(translator.parse("2E-2").equals(0))
  }

  @Test
  def shortTest(): Unit = {
    val translator = DefaultTranslator("short", _.toString)
    assert(Try(translator.parse("100000")).isFailure)
    assert(translator.parse("1").equals(1.toShort))
    assert(translator.parse("1.5").equals(2.toShort))
    assert(translator.parse("1.0").equals(1.toShort))
    assert(translator.parse("1.2E2").equals(120.toShort))
    assert(translator.parse(".2E2").equals(20.toShort))
    assert(translator.parse("0.2E2").equals(20.toShort))
    assert(translator.parse("0.02E2").equals(2.toShort))
    assert(translator.parse("2E-2").equals(0.toShort))
  }

}
