package spark.deploy.master

import akka.actor.ActorRef
import scala.collection.mutable
import spark.Utils

private[spark] class WorkerInfo(
  val id: String,
  val host: String,
  val port: Int,
  val cores: Int,
  val memory: Int,
  val actor: ActorRef,
  val webUiPort: Int,
  val publicAddress: String) {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  var executors = new mutable.HashMap[String, ExecutorInfo]  // fullId => info
  var state: WorkerState.Value = WorkerState.ALIVE
  var coresUsed = 0
  var memoryUsed = 0

  var lastHeartbeat = System.currentTimeMillis()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

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

  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: WorkerState.Value) = {
    this.state = state
  }
}
