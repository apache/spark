package spark

import mesos._

/**
 * Trait representing a parallel job in MesosScheduler. Schedules the
 * job by implementing various callbacks.
 */
trait Job {
  def slaveOffer(s: SlaveOffer, availableCpus: Int, availableMem: Int)
    : Option[TaskDescription]

  def statusUpdate(t: TaskStatus): Unit

  def error(code: Int, message: String): Unit
}
