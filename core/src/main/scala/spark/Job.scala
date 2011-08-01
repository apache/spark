package spark

import org.apache.mesos._
import org.apache.mesos.Protos._

/**
 * Class representing a parallel job in MesosScheduler. Schedules the
 * job by implementing various callbacks.
 */
abstract class Job(jobId: Int) {
  def slaveOffer(s: SlaveOffer, availableCpus: Double): Option[TaskDescription]

  def statusUpdate(t: TaskStatus): Unit

  def error(code: Int, message: String): Unit

  def getId(): Int = jobId
}
