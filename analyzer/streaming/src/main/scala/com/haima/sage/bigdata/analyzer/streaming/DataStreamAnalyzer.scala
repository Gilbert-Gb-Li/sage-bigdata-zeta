package com.haima.sage.bigdata.analyzer.streaming

import java.util.Collections
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorIdentity, ActorPath, ActorRef, ActorSystem, Identify, Props, Terminated}
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.utils.ActorSystemFactory
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import  com.haima.sage.bigdata.etl.common.Constants.executor
import scala.concurrent.duration._

/**
  * Created by ChengJi on 2017/4/25.
  */
abstract class DataStreamAnalyzer[CONF <: Analyzer, M1, M2, T >: Null <: Any] extends DataAnalyzer[CONF, DataStream[RichMap], DataStream[RichMap]] {
  def conf: CONF

  override final def engine(): AnalyzerModel.Value = AnalyzerModel.STREAMING

  override val isModel: Boolean = false

  def marshaller: TypeInformation[M2]

  @throws[Exception]
  override def action(data: DataStream[RichMap]): DataStream[RichMap] = {
    fromIntermediate(new DataStream[M2](AsyncDataStream.unorderedWait(toIntermediate(data).javaStream, new AsyncRequest(analyzing, convert), 1000, TimeUnit.MILLISECONDS, 100).returns(marshaller)))
  }

  /**
    * 将模型数据转换成算法的所需的结构类型
    *
    * @param model 原始的模型数据
    * @return 目标结构类型的模型数据
    */
  def convert(model: Iterable[Map[String, Any]]): T

  /**
    * 将原始数据处理成可分析的中间结果1
    *
    * @param data 原始数据
    * @return 中间结果
    */
  def toIntermediate(data: DataStream[RichMap]): DataStream[M1]

  /**
    * 将中间结果的数据转成标准数据
    *
    * @param data 中间数据2
    * @return 标准数据
    */
  def fromIntermediate(data: DataStream[M2]): DataStream[RichMap]

  /**
    * 算法预测逻辑实现
    *
    * @param in     中间数据1
    * @param models 模型数据
    * @return 分析后的中间数据2
    */
  def analyzing(in: M1, models: T): M2

  /**
    * 异步请求类
    *
    * @param fun     算法预测逻辑实现方法
    * @param convert 模型数据转换方法
    */
  class AsyncRequest(fun: (M1, T) => M2, convert: Iterable[Map[String, Any]] => T) extends RichAsyncFunction[M1, M2] with Serializable {
    private lazy val system: ActorSystem = ActorSystemFactory.get("Model-Acquisition")
    private lazy val modelAcquisition: ActorRef = system.actorOf(Props.create(classOf[ModelAcquisition], this, conf.useModel.get))
    private lazy val modelData: ArrayBuffer[Iterable[Map[String, Any]]] = new ArrayBuffer[Iterable[Map[String, Any]]]()

    val invoke: (M1, ResultFuture[M2]) => Unit = if (conf.useModel.isDefined) {
      (in, resultFuture) =>
        if (modelData.nonEmpty) {
          val data = modelData(0)
          if (data != null && data.nonEmpty) {
            resultFuture.complete(Collections.singleton(fun(in, convert(data))))
          } else
            resultFuture.complete(Collections.singleton(fun(in, null)))
        } else {
          resultFuture.complete(Collections.singleton(fun(in, null)))
        }
    } else
      (in, resultFuture) =>
        resultFuture.complete(Collections.singleton(fun(in, null)))

    override def asyncInvoke(in: M1, resultFuture: ResultFuture[M2]): Unit = invoke(in, resultFuture)

    override def open(parameters: Configuration): Unit = {
      if (conf.useModel.isDefined) {
        if (modelData.nonEmpty) modelData.remove(0)
        modelData.append(getDataModel)
        logger.info(s"Finished initialize the model data ,records length is ${modelData(0).size}!")

      }
    }

    /**
      * 获取模型数据
      *
      * @return Iterable[Map[String, Any] ]
      */
    def getDataModel: Iterable[Map[String, Any]] = {
      implicit val timeout = Timeout(2 seconds)
      Await.result(modelAcquisition ? (Opt.GET, "MODEL_DATA"), timeout.duration).asInstanceOf[Iterable[Map[String, Any]]]
    }

    override def close(): Unit = {
      try {
        system.stop(modelAcquisition)
        ActorSystemFactory.close("Model-Acquisition")
      } catch {
        case e: Exception =>
          logger.debug(e.getMessage)
      }
    }

    /**
      * 模型数据获取
      *
      * @param modelingId 模型id，唯一标识
      */
    class ModelAcquisition(modelingId: String) extends Actor with Serializable {
      private lazy val remote = system.actorSelection(ActorPath.fromString(modelingId))

      override def receive: Receive = {
        case (Opt.GET, "MODEL_DATA") =>
          remote ! Identify(modelingId) //注册地址到knowledgeUserServer
          if (modelData.isEmpty) self.forward((Opt.GET, "MODEL_DATA", "")) else
            sender() ! modelData(0)

        case (Opt.GET, "MODEL_DATA", "") =>
          if (modelData.isEmpty) self.forward((Opt.GET, "MODEL_DATA", "")) else
            sender() ! modelData(0)

        //重新请求到了knowledge user server 提供的actorRef
        case ActorIdentity(_, Some(user)) =>
          logger.info(s"remote[$remote] is available")
          context.watch(user)
          user ! (conf.useModel, Status.CONNECTED)
        //没有请求到knowledge user server  提供的actorRef
        case ActorIdentity(_, None) =>
          logger.info(s"remote[$remote] is not available")
          context.system.scheduler.scheduleOnce(10 seconds) {
            remote ! Identify(modelingId)
          }
        case Terminated(actor) =>
          context.unwatch(actor)
          actor ! Identify(modelingId)
        case data: Iterable[Map[String, Any]]@unchecked =>
          logger.info("receive modeling data!")
          if (modelData.nonEmpty)
            modelData.remove(0)
          modelData.append(data)

        case Status.STOPPED =>
          context.stop(self)
        case _ =>
          logger.debug("unknown message type!")
      }
    }

  }

}

