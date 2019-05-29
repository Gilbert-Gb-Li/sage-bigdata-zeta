package com.haima.sage.bigdata.etl.common.model

import com.haima.sage.bigdata.etl.common.model

/**
  * Created by evan on 17-10-30.
  */
object FlinkAPIs extends Enumeration {
  type FlinkAPIs = Value
  // 集群概览
  val CLUSTER_OVERVIEW = Value
  //RPC 地址
  val RPC_ADDRESS = Value
  // 是否有 slots 可用
  val SLOTS_AVAILABLE = Value
  // jobs 列表概览
  val JOBS_OVERVIEW = Value
  // Job 详细状态信息
  val JOB_DETAILS = Value
  // Job 状态
  val JOB_STATUS = Value
  // 获取 JobManager config
  val JOB_MANAGER_CONFIG = Value

  // 取消Job
  val JOB_CONCELLATION = Value
  // Metrics
  val JOB_METRICS = Value
}
