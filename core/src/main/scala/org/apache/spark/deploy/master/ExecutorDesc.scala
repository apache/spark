
package org.apache.spark.deploy.master

import org.apache.spark.deploy.{ExecutorDescription, ExecutorState}

private[master] class ExecutorDesc(
    val id: Int,
    val application: ApplicationInfo,
    val worker: WorkerInfo,
    val cores: Int,
    val memory: Int) {

  var state = ExecutorState.LAUNCHING

  /** Copy all state (non-val) variables from the given on-the-wire ExecutorDescription. */
  def copyState(execDesc: ExecutorDescription) {
    state = execDesc.state
  }

  def fullId: String = application.id + "/" + id

  override def equals(other: Any): Boolean = {
    other match {
      case info: ExecutorDesc =>
        fullId == info.fullId &&
        worker.id == info.worker.id &&
        cores == info.cores &&
        memory == info.memory
      case _ => false
    }
  }

  override def toString: String = fullId

  override def hashCode: Int = toString.hashCode()
}
