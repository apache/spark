package org.apache.spark.scheduler.cluster

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Stores information about an executor to pass from the scheduler to SparkListeners.
 */
@DeveloperApi
class ExecutorInfo(
   val executorHost: String,
   val totalCores: Int,
   val logUrlMap: Map[String, String]) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[ExecutorInfo]

  override def equals(other: Any): Boolean = other match {
    case that: ExecutorInfo =>
      (that canEqual this) &&
        executorHost == that.executorHost &&
        totalCores == that.totalCores &&
        logUrlMap == that.logUrlMap
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(executorHost, totalCores, logUrlMap)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
