package spark.deploy.master

import akka.actor.ActorRef
import scala.collection.mutable

private[spark] class WorkerInfo(
  val id: String,
  val host: String,
  val port: Int,
  val cores: Int,
  val memory: Int,
  val actor: ActorRef,
  val webUiPort: Int) {

  var executors = new mutable.HashMap[String, ExecutorInfo]  // fullId => info

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def addExecutor(exec: ExecutorInfo) {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  def removeExecutor(exec: ExecutorInfo) {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
    }
  }
  
  def webUiAddress : String = {
    "http://" + this.host + ":" + this.webUiPort
  }
}
