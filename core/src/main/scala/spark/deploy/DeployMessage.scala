package spark.deploy

import spark.deploy.ExecutorState.ExecutorState
import spark.deploy.master.{WorkerInfo, ApplicationInfo}
import spark.deploy.worker.ExecutorRunner
import scala.collection.immutable.List
import spark.Utils


private[spark] sealed trait DeployMessage extends Serializable

// Worker to Master

private[spark]
case class RegisterWorker(
    id: String,
    host: String,
    port: Int,
    cores: Int,
    memory: Int,
    webUiPort: Int,
    publicAddress: String)
  extends DeployMessage {
  Utils.checkHost(host, "Required hostname")
  assert (port > 0)
}

private[spark] 
case class ExecutorStateChanged(
    appId: String,
    execId: Int,
    state: ExecutorState,
    message: Option[String],
    exitStatus: Option[Int])
  extends DeployMessage

private[spark] case class Heartbeat(workerId: String) extends DeployMessage

// Master to Worker

private[spark] case class RegisteredWorker(masterWebUiUrl: String) extends DeployMessage
private[spark] case class RegisterWorkerFailed(message: String) extends DeployMessage
private[spark] case class KillExecutor(appId: String, execId: Int) extends DeployMessage

private[spark] case class LaunchExecutor(
    appId: String,
    execId: Int,
    appDesc: ApplicationDescription,
    cores: Int,
    memory: Int,
    sparkHome: String)
  extends DeployMessage

// Client to Master

private[spark] case class RegisterApplication(appDescription: ApplicationDescription)
  extends DeployMessage

// Master to Client

private[spark] 
case class RegisteredApplication(appId: String) extends DeployMessage

private[spark] 
case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
  Utils.checkHostPort(hostPort, "Required hostport")
}

private[spark]
case class ExecutorUpdated(id: Int, state: ExecutorState, message: Option[String],
                           exitStatus: Option[Int])

private[spark]
case class ApplicationRemoved(message: String)

// Internal message in Client

private[spark] case object StopClient

// MasterWebUI To Master

private[spark] case object RequestMasterState

// Master to MasterWebUI

private[spark] 
case class MasterState(host: String, port: Int, workers: Array[WorkerInfo],
  activeApps: Array[ApplicationInfo], completedApps: Array[ApplicationInfo]) {

  Utils.checkHost(host, "Required hostname")
  assert (port > 0)

  def uri = "spark://" + host + ":" + port
}

//  WorkerWebUI to Worker
private[spark] case object RequestWorkerState

// Worker to WorkerWebUI

private[spark]
case class WorkerState(host: String, port: Int, workerId: String, executors: List[ExecutorRunner],
  finishedExecutors: List[ExecutorRunner], masterUrl: String, cores: Int, memory: Int, 
  coresUsed: Int, memoryUsed: Int, masterWebUiUrl: String) {

  Utils.checkHost(host, "Required hostname")
  assert (port > 0)
}
