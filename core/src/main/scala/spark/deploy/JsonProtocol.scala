package spark.deploy

import master.{ApplicationInfo, WorkerInfo}
import net.liftweb.json.JsonDSL._
import worker.ExecutorRunner

private[spark] object JsonProtocol {
 def writeWorkerInfo(obj: WorkerInfo) = {
   ("id" -> obj.id) ~
   ("host" -> obj.host) ~
   ("port" -> obj.port) ~
   ("webuiaddress" -> obj.webUiAddress) ~
   ("cores" -> obj.cores) ~
   ("coresused" -> obj.coresUsed) ~
   ("memory" -> obj.memory) ~
   ("memoryused" -> obj.memoryUsed)
 }

  def writeApplicationInfo(obj: ApplicationInfo) = {
    ("starttime" -> obj.startTime) ~
    ("id" -> obj.id) ~
    ("name" -> obj.desc.name) ~
    ("cores" -> obj.desc.maxCores) ~
    ("user" ->  obj.desc.user) ~
    ("memoryperslave" -> obj.desc.memoryPerSlave) ~
    ("submitdate" -> obj.submitDate.toString)
  }

  def writeApplicationDescription(obj: ApplicationDescription) = {
    ("name" -> obj.name) ~
    ("cores" -> obj.maxCores) ~
    ("memoryperslave" -> obj.memoryPerSlave) ~
    ("user" -> obj.user)
  }

  def writeExecutorRunner(obj: ExecutorRunner) = {
    ("id" -> obj.execId) ~
    ("memory" -> obj.memory) ~
    ("appid" -> obj.appId) ~
    ("appdesc" -> writeApplicationDescription(obj.appDesc))
  }

  def writeMasterState(obj: MasterState) = {
    ("url" -> ("spark://" + obj.uri)) ~
    ("workers" -> obj.workers.toList.map(writeWorkerInfo)) ~
    ("cores" -> obj.workers.map(_.cores).sum) ~
    ("coresused" -> obj.workers.map(_.coresUsed).sum) ~
    ("memory" -> obj.workers.map(_.memory).sum) ~
    ("memoryused" -> obj.workers.map(_.memoryUsed).sum) ~
    ("activeapps" -> obj.activeApps.toList.map(writeApplicationInfo)) ~
    ("completedapps" -> obj.completedApps.toList.map(writeApplicationInfo))
  }

  def writeWorkerState(obj: WorkerState) = {
    ("id" -> obj.workerId) ~
    ("masterurl" -> obj.masterUrl) ~
    ("masterwebuiurl" -> obj.masterWebUiUrl) ~
    ("cores" -> obj.cores) ~
    ("coresused" -> obj.coresUsed) ~
    ("memory" -> obj.memory) ~
    ("memoryused" -> obj.memoryUsed) ~
    ("executors" -> obj.executors.toList.map(writeExecutorRunner)) ~
    ("finishedexecutors" -> obj.finishedExecutors.toList.map(writeExecutorRunner))
  }
}
