package com.haima.sage.bigdata.etl.reader

import java.util.Date

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{AWSSource, ReadPosition}
import com.haima.sage.bigdata.etl.stream.AWSStream

/**
  * Created by zhhuiyan on 14/12/26.
  */
class AWSReader(val conf: AWSSource, val server: String, start: Date) extends LogReader[RichMap] with Position {


  override val stream = conf match {
    case AWSSource(Some(accessKey), Some(secretKey), region,_,_) =>
      val credentials: AWSCredentials = new BasicAWSCredentials("AKIAP73ALCIUYVKG6K5Q", "Yjyi1eFGPSPYWGPdyUdBZqO0mXgE0IfB/ot65Dwc")
      AWSStream(credentials, server, RegionUtils.getRegion(region.getOrElse("cn-north-1")), start)
    case AWSSource(_, _, region,_,_) =>
      val credentials: AWSCredentials = new DefaultAWSCredentialsProviderChain().getCredentials
      AWSStream(credentials, server, RegionUtils.getRegion(region.getOrElse("cn-north-1")), start)

  }

  def skip(skip: Long): Long = 0

  def path: String = conf.uri

  override val iterator: Iterator[RichMap] = new Iterator[RichMap] {
    override def next(): RichMap = {
      val event = stream.next()

      event.get("@timestamp") match {
        case Some(date:Date)=>
          position.setPosition(date.getTime)
        case _=>
      }
      event
    }

    override def hasNext: Boolean = stream.hasNext
  }
  val position = {
    val pos: Long = start.getTime
    new ReadPosition(path, 0, pos)
  }

}
