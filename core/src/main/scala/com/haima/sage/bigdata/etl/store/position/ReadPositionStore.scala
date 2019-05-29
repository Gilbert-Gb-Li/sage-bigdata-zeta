package com.haima.sage.bigdata.etl.store.position

import java.io.{Closeable, Flushable}

import com.haima.sage.bigdata.etl.common.model.ReadPosition

/**
  * Created by zhhuiyan on 15/1/29.
  */
trait ReadPositionStore extends Flushable with Closeable {
  private var cacheSize: Int = 1

  def setCache(size: Int) {
    this.cacheSize = size
  }

  def cache = cacheSize

  def init: Boolean

  def set(readPosition: ReadPosition): Boolean

  /**
    * @param path
    * @return [0] lineNumber , [1] pos
    */
  def get(path: String): Option[ReadPosition]

  def list(path: String): List[ReadPosition]

  /**
    * @param path
    * @return [0] lineNumber , [1] pos
    */
  def remove(path: String): Boolean

  def fuzzyRemove(path: String): Boolean
}
