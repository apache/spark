package spark

import mesos._

/**
 * Class representing a parallel job in MesosScheduler. Schedules the
 * job by implementing various callbacks.
 */
abstract class Job(jobId: Int) {
  def slaveOffer(s: SlaveOffer, availableCpus: Int, availableMem: Int)
    : Option[TaskDescription]

  def statusUpdate(t: TaskStatus): Unit

  def error(code: Int, message: String): Unit

  def getId(): Int = jobId
}
