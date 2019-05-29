package com.haima.sage.bigdata.etl.modeling.flink.multi

import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.junit.Test

import scala.reflect.ClassTag

/**
  * Created by evan on 18-4-9.
  */
class MultiOutputTest extends Serializable {

  val names: List[Name] = List(
    Name("1", "张三"),
    Name("2", "李四"),
    Name("3", "王麻子")
  )
  val departments: List[Department] = List(
    Department("1", "研发"),
    Department("2", "测试"),
    Department("3", "行政"),
    Department("3", "财务")
  )
  val salaries: List[Salary] = List(
    Salary("1", 100),
    Salary("1", 200),
    Salary("2", 300),
    Salary("3", 400)
  )

  @Test
  def multiOutput(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataSet1: DataSet[Name] = env.fromCollection(names)(ClassTag(classOf[Name]), createTypeInformation[Name])
    val dataSet2 = env.fromCollection(departments)(ClassTag(classOf[Department]), createTypeInformation[Department])
    val dataSet3 = env.fromCollection(salaries)(ClassTag(classOf[Salary]), createTypeInformation[Salary])

    dataSet1.writeAsText("file:///opt/IdeaProjects/project/sage-bigdata-etl/target/data1.txt", WriteMode.OVERWRITE)

    val dataSet1With2 = dataSet1.join(dataSet2).where(_.id).equalTo(_.id).setParallelism(CONF.getInt("flink.parallelism"))
    dataSet1With2.writeAsText("file:///opt/IdeaProjects/project/sage-bigdata-etl/target/data1-2.txt", WriteMode.OVERWRITE)


    dataSet2.join(dataSet3).where(_.id).equalTo(_.id).setParallelism(CONF.getInt("flink.parallelism")).writeAsText("file:///opt/IdeaProjects/project/sage-bigdata-etl/target/data2-3.txt", WriteMode.OVERWRITE)

    dataSet1With2.join(dataSet3).where(_._1.id).equalTo(_.id).setParallelism(CONF.getInt("flink.parallelism")).writeAsText("file:///opt/IdeaProjects/project/sage-bigdata-etl/target/data1-2-3.txt", WriteMode.OVERWRITE)
    env.execute()
  }

  @Test
  def test(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = List(
      RichMap(Map("name" -> "小米", "age" -> 5)),
      RichMap(Map("name" -> "大米", "age" -> 9))
    )

    val data2 = List(
      Map("name" -> "小米", "age" -> 5),
      Map("name" -> "大米", "age" -> 9)
    )
    val dataSet = env.fromCollection(data)
    val dataSet2 = dataSet.map(d => d.+("a@id" -> "11111")).collect()


    dataSet2.foreach(println)
  }

  @Test
  def test2(): Unit ={
    val data = List(
      RichMap(Map("name" -> "小米", "age" -> 5)),
      RichMap(Map("name" -> "大米", "age" -> 9))
    )
    data.map(_.+("a@id" -> "123")).foreach(println)
  //  println(map.+("a@id" -> "123"))
  }
}

case class Name(id: String, name: String) extends Serializable

case class Department(id: String, department: String) extends Serializable

case class Salary(id: String, salary: Int) extends Serializable
