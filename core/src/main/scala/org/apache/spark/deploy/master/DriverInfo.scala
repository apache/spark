
package org.apache.spark.deploy.master

import java.util.Date

import org.apache.spark.deploy.DriverDescription
import org.apache.spark.util.Utils

private[deploy] class DriverInfo(
    val startTime: Long,
    val id: String,
    val desc: DriverDescription,
    val submitDate: Date)
  extends Serializable {

  @transient var state: DriverState.Value = DriverState.SUBMITTED
  /* If we fail when launching the driver, the exception is stored here. */
  @transient var exception: Option[Exception] = None
  /* Most recent worker assigned to this driver */
  @transient var worker: Option[WorkerInfo] = None

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init(): Unit = {
    state = DriverState.SUBMITTED
    worker = None
    exception = None
  }
}
