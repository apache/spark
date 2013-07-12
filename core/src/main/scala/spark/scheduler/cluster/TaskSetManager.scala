package spark.scheduler.cluster

import scala.collection.mutable.ArrayBuffer
import spark.scheduler._
import spark.TaskState.TaskState
import java.nio.ByteBuffer

private[spark] trait TaskSetManager extends Schedulable {
  def schedulingMode = SchedulingMode.NONE
  def taskSet: TaskSet
  def slaveOffer(execId: String, hostPort: String, availableCpus: Double,
    overrideLocality: TaskLocality.TaskLocality = null): Option[TaskDescription]
  def numPendingTasksForHostPort(hostPort: String): Int
  def numRackLocalPendingTasksForHost(hostPort :String): Int
  def numPendingTasksForHost(hostPort: String): Int
  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer)
  def error(message: String)
}
