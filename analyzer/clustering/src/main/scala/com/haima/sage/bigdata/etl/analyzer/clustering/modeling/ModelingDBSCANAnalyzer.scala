package com.haima.sage.bigdata.analyzer.clustering.modeling

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.{AnalyzerType, DBSCANAnalyzer}
import com.haima.sage.bigdata.etl.modeling.flink.analyzer.DataModelingAnalyzer
import org.apache.flink.api.scala.{DataSet, _}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.metrics.distances.EuclideanDistanceMetric
import org.apache.flink.api.scala.extensions._
import scala.collection.mutable.ArrayBuffer


class ModelingDBSCANAnalyzer(override val conf: DBSCANAnalyzer,
                             override val `type`: AnalyzerType.Type = AnalyzerType.ANALYZER) extends DataModelingAnalyzer[DBSCANAnalyzer](conf) with Serializable {

  lazy protected val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  override def action(data: DataSet[RichMap]): DataSet[RichMap] = {


    val minPts = conf.minpts
    //密度阈值
    val ePs = conf.eps
    //领域半径
    val fields = conf.fields
    //字段名称
    var index = 1
    data.map(ma => {
      ma.get("vector").map(_.asInstanceOf[DenseVector]).toList
    }).reduce(_ ++ _).map(data => runDBSCAN(data, ePs, minPts)).flatMap(_.toList)
  }

  def runDBSCAN(ds: List[DenseVector], ePs: Double, minPts: Int): List[RichMap] = {
    val data = ds.distinct
    //求出集合中不同的数据
    val visited = (for (i <- 0 to data.length - 1) yield 0).toArray
    //用于判断该点是否处理过，0表示未处理过
    var number = 1
    //用于标记类
    val neighPoints = new ArrayBuffer[DenseVector]()
    var cluster = scala.collection.mutable.Map[DenseVector, Int]()
    //用于标记每个数据点所属的类别
    var index = 0
    data.map(v => cluster += (v -> 0)) //对cluster进行初始化
    for (i <- 0 to data.length - 1) {
      //对每一个点进行处理
      if (visited(i) == 0) {
        //表示该点未被处理
        visited(i) = 1
        //标记为处理过
        var xTempPoint = data(i)
        //取到该点
        var distance = data.map(x => (vectorDis(x, xTempPoint), x)) //取得该点到其他所有点的距离Array{(distance,vector)}
        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => v._2) //找到半径ePs内的所有点(密度相连点集合)
        if (neighPoints.length >= minPts) {
          //核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
          cluster(xTempPoint) = number
          while (neighPoints.isEmpty == false) {
            //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
            var yTempPoint = neighPoints.head //取集合中第一个点
            index = data.indexOf(yTempPoint)
            if (cluster(yTempPoint) == 0)
              cluster(yTempPoint) = number //划分到与核心点一样的簇中
            if (visited(index) == 0) {
              //若该点未被处理，则标记已处理
              visited(index) = 1
              var distanceTemp = data.map(x => (vectorDis(x, yTempPoint), x))
              //取得该点到其他所有点的距离
              var neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => v._2) //找到半径ePs内的所有点
              if (neighPointsTemp.length >= minPts) {
                for (j <- 0 to neighPointsTemp.length - 1) {
                  //将其领域内未分类的对象划分到簇中,然后放入neighPoints
                  if (cluster(neighPointsTemp(j)) == 0) {
                    cluster(neighPointsTemp(j)) = number //只划分簇，没有访问到
                    if (!neighPoints.contains(neighPointsTemp(j)))
                      neighPoints += neighPointsTemp(j)
                  }
                }
              }
            }
            neighPoints -= yTempPoint //将该点剔除
          } //end-while
          number += 1 //进行新的聚类
        }
        //清空neighPoints集合
        neighPoints.clear()
      }
    }
    var result = ds.map(v => {
      (v, cluster(v))
    }).groupBy(_._2)
    var arr = new ArrayBuffer[RichMap]()
    result.foreach(v => {
      v._2.foreach(w => {
        val key = w._1
        val value = w._2
        if (value != 0)
          arr.append(Map("features" -> key.data, "label" -> value))
      })
    })
    arr.toList
  }

  //计算距离
  def vectorDis(v1: DenseVector, v2: DenseVector): Double = {
    EuclideanDistanceMetric().distance(v1, v2)
  }

  /**
    * 算法是否可以进行“生产模型”
    *
    * @return
    */
  override def modelAble: Boolean = false

  /**
    * 加载模型
    *
    * @return
    */
  override def load()(implicit env: ExecutionEnvironment): Option[DataSet[RichMap]] = ???

  /**
    * 训练模型
    *
    * @param data
    * @return
    */
  override def modelling(data: DataSet[RichMap]): DataSet[RichMap] = ???

  /**
    * 执行分析
    *
    * @param data
    * @param model
    * @return
    */
  override def actionWithModel(data: DataSet[RichMap], model: Option[DataSet[RichMap]]): DataSet[RichMap] = ???
}
