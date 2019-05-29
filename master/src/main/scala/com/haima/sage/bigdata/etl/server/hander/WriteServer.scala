package com.haima.sage.bigdata.etl.server.hander

import akka.http.scaladsl.model.StatusCodes
import com.haima.sage.bigdata.etl.common.model.{BuildResult, Opt, SwitchWriter, WriteWrapper}
import com.haima.sage.bigdata.etl.store.{ModelingStore, Stores, WriteWrapperStore}


/**
  * Created by zhhuiyan on 16/7/18.
  */
class WriteServer extends StoreServer[WriteWrapper, String] {
  val store: WriteWrapperStore = Stores.writeStore
  lazy val modelingStore: ModelingStore = Stores.modelingStore


  override def receive: Receive = {
    case (Opt.SYNC, (Opt.UPDATE, data: WriteWrapper)) =>
      if (store.all().exists(ds => ds.name == data.name && !ds.id.contains(data.id.orNull))) {
        sender() ! BuildResult("304", "611", data.name, data.name)
        //sender() ! Result("304", message = s"your set datasource[${data.name}] has exist in system please rename it!")
      } else {
        if (store.set(data)) {
          sender() ! BuildResult("200", "609", data.name)
          //sender() ! Result("200", message = s"update datasource[${data.id}] success, effective after after restart channel that dependence this!")
        } else {
          sender() ! BuildResult("304", "610", data.name)
          //sender() ! Result("304", message = s"update datasource[${data.id}] failure!")
        }
      }
    case (Opt.SYNC, (Opt.CREATE, data: WriteWrapper)) =>
      if (store.all().exists(_.name == data.name)) {
        sender() ! BuildResult("304", "611", data.name, data.name)
        //sender() ! Result("304", message = s"YOUR SET DATASOURCE[${data.name}] HAS EXIST IN SYSTEM PLEASE RENAME IT!")
      } else {
        if (store.set(data)) {
          sender() ! BuildResult("200", "600", data.name)
          //sender() ! Result("200", message = s"add datasource[${data.id}] success!")
        } else {
          sender() ! BuildResult("304", "601", data.name)
          //sender() ! Result("304", message = s"add datasource[${data.id}] failure!")
        }
      }
    case (Opt.SYNC, (Opt.DELETE, id: String)) =>

      sender() ! delete(id)
    case obj =>
      super.receive(obj)

  }

  def save(writer: WriteWrapper): BuildResult = {
    val count = store.queryCountByName(writer)
    if (count > 0) {
      BuildResult(StatusCodes.NotModified.intValue.toString, "611", writer.name, writer.name)
      //Result(StatusCodes.NotModified.intValue.toString, message = s"SAVE WRITER[${writer.id.getOrElse("")}] ERROR,NAME[${writer.name}] EXIT!")
    } else if (store.set(writer)) {
      BuildResult(StatusCodes.OK.intValue.toString, "609", writer.name)
      //Result(StatusCodes.OK.intValue.toString, message = s"save writer[${(writer.id.getOrElse(""), writer.name)}] success,effective after restart channel that dependence this!")
    } else {
      BuildResult(StatusCodes.NotAcceptable.intValue.toString, "610", writer.name)
      //Result(StatusCodes.NotAcceptable.intValue.toString, message = s"save writer[${(writer.id.getOrElse(""), writer.name)}] failed")
    }
  }

  def delete(id: String): BuildResult = {
    val writer = store.get(id).orNull

    store.all().find(_.data match {
      case d: SwitchWriter if d.writers.map(_._2.id).contains(id) || d.default.id == id =>
        true
      case _ =>
        false
    }) match {
      // 被switch 数据存储使用
      case Some(x) =>
        BuildResult("304", "618", writer.name, x.name)
      case _ =>
        modelingStore.byWrite(id) match {
          // 被数据建模通道使用
          case head :: _ =>
            BuildResult("304", "617", writer.name, head.name)
          case Nil =>
            configStore.byWriter(id) match {
              // 被通道使用
              case head :: _ =>
                BuildResult(StatusCodes.NotModified.intValue.toString, "607", writer.name, head.name)
              case Nil =>
                if (store.delete(id))
                  BuildResult(StatusCodes.OK.intValue.toString, "604", writer.name)
                //Result(StatusCodes.OK.intValue.toString, message = s"DELETE WRITER[$id] SUCCESS!")
                else
                  BuildResult(StatusCodes.NotAcceptable.intValue.toString, "605", writer.name)

            }

        }
    }


  }
}
