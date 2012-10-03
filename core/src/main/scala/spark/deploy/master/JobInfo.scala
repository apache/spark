package spark.deploy.master

import spark.deploy.JobDescription
import java.util.Date
import akka.actor.ActorRef
import scala.collection.mutable

private[spark]
class JobInfo(val id: String, val desc: JobDescription, val submitDate: Date, val actor: ActorRef) {
  var state = JobState.WAITING
  var executors = new mutable.HashMap[Int, ExecutorInfo]
  var coresGranted = 0

  private var nextExecutorId = 0

  def newExecutorId(): Int = {
    val id = nextExecutorId
    nextExecutorId += 1
    id
  }

  def addExecutor(worker: WorkerInfo, cores: Int): ExecutorInfo = {
    val exec = new ExecutorInfo(newExecutorId(), this, worker, cores, desc.memoryPerSlave)
    executors(exec.id) = exec
    coresGranted += cores
    exec
  }

  def removeExecutor(exec: ExecutorInfo) {
    executors -= exec.id
    coresGranted -= exec.cores
  }

  def coresLeft: Int = desc.cores - coresGranted

  private var _retryCount = 0

  def retryCount = _retryCount

  def incrementRetryCount = {
    _retryCount += 1
    _retryCount
  }
}
