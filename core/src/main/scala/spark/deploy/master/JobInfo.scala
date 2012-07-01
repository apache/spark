package spark.deploy.master

import spark.deploy.JobDescription
import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable

class JobInfo(val id: String, val desc: JobDescription, val submitDate: Date, val actor: ActorRef) {
  var state = JobState.WAITING
  var executors = new mutable.HashMap[Int, ExecutorInfo]

  var nextExecutorId = 0

  def newExecutorId(): Int = {
    val id = nextExecutorId
    nextExecutorId += 1
    id
  }

  def newExecutor(worker: WorkerInfo, cores: Int): ExecutorInfo = {
    val exec = new ExecutorInfo(newExecutorId(), this, worker, cores, desc.memoryPerSlave)
    executors(exec.id) = exec
    exec
  }
}
