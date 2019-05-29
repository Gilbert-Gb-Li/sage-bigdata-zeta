package com.haima.sage.bigdata.etl.common.plugin

/**
  * Created by lenovo on 2017/8/10.
  */
trait Plugin extends Checkable {
  def info: String

  def installed(): Boolean = {
    true
  }
}

trait Checkable
