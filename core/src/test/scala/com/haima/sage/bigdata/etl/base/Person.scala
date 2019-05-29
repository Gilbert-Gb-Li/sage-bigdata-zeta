package com.haima.sage.bigdata.etl.base

/**
  * Created by zhhuiyan on 2017/3/21.
  */
class Person(val name: String) {
  def sayHello = println("Hello, I'm " + name)

  def makeFriends(p: Person):Unit={
    sayHello
    p.sayHello
  }
}
class Student(override val name: String) extends Person(name)
class Party[T <: Person](p1: T, p2: T) {
  def play = p1.makeFriends(p2)
}



