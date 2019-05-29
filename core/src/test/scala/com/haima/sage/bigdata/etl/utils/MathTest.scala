package com.haima.sage.bigdata.etl.utils

import org.junit.Test

import scala.collection.mutable

/**
  * Created by zhhuiyan on 2017/1/7.
  */
class MathTest {

  @Test
  def computor(): Unit = {
      println(step(16))
  }

  @Test
  def testIncrement(): Unit = {
    var count = 5
    count += 1
    assert(count == 6)

  }

  def step(num: Long): Int = {
    if (num == 3) {
      1
    } else if (num == 5) {
      1
    } else if (num < 5) {
      0

    } else {
      step(num - 3) + step(num - 5)
    }
  }
  @Test
  def firtbtree(): Unit ={
   val tree=Node[Int](1, Node[Int](2,Node(4),Node(5)),new Node[Int](3,Node(6),Node(7)))

    Tree.bfs[Int](tree,printInt)
  }

  def printInt(v:Int): Unit ={
    print(s"$v,")
  }

}
object Tree{
  var quene =new mutable.Queue[Tree[_]]()
  def bfs[T](tree: Tree[T],func: T => Unit) {

    tree match {
      case Empty =>
      case node: Node[T@unchecked] =>
        func(node.value)
        quene.enqueue(node.left)
        quene.enqueue(node.right)
        bfs[T](quene.dequeue().asInstanceOf[Tree[T]],func)
        bfs[T](quene.dequeue().asInstanceOf[Tree[T]],func)
    }
  }
}

sealed trait Tree[+T] {
  /**
    * 深度优先遍历
    */
  def bfs(func: T => Unit) {
    var quene =new mutable.Queue[Tree[T]]()
    this match {
      case Empty =>
      case node: Node[T@unchecked] =>
        func(node.value)
        quene.enqueue(node.left)
        quene.enqueue(node.right)

    }
  }
}
/**
  * 空树
  */
case object Empty extends Tree[Nothing]()
/**
  * 节点, 单个节点是一棵树
  */

case class Node[T](value: T, left: Tree[T]=Empty, right: Tree[T]=Empty) extends Tree[T]
