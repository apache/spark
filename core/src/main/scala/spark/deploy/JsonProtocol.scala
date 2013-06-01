package spark.deploy

import master.{ApplicationInfo, WorkerInfo}
import worker.ExecutorRunner
import cc.spray.json._

/**
 * spray-json helper class containing implicit conversion to json for marshalling responses
 */
private[spark] object JsonProtocol extends DefaultJsonProtocol {
  implicit object WorkerInfoJsonFormat extends RootJsonWriter[WorkerInfo] {
    def write(obj: WorkerInfo) = JsObject(
      "id" -> JsString(obj.id),
      "host" -> JsString(obj.host),
      "port" -> JsNumber(obj.port),
      "webuiaddress" -> JsString(obj.webUiAddress),
      "cores" -> JsNumber(obj.cores),
      "coresused" -> JsNumber(obj.coresUsed),
      "memory" -> JsNumber(obj.memory),
      "memoryused" -> JsNumber(obj.memoryUsed)
    )
  }

  implicit object AppInfoJsonFormat extends RootJsonWriter[ApplicationInfo] {
    def write(obj: ApplicationInfo) = JsObject(
      "starttime" -> JsNumber(obj.startTime),
      "id" -> JsString(obj.id),
      "name" -> JsString(obj.desc.name),
      "cores" -> JsNumber(obj.desc.maxCores),
      "user" -> JsString(obj.desc.user),
      "memoryperslave" -> JsNumber(obj.desc.memoryPerSlave),
      "submitdate" -> JsString(obj.submitDate.toString))
  }

  implicit object AppDescriptionJsonFormat extends RootJsonWriter[ApplicationDescription] {
    def write(obj: ApplicationDescription) = JsObject(
      "name" -> JsString(obj.name),
      "cores" -> JsNumber(obj.maxCores),
      "memoryperslave" -> JsNumber(obj.memoryPerSlave),
      "user" -> JsString(obj.user)
    )
  }

  implicit object ExecutorRunnerJsonFormat extends RootJsonWriter[ExecutorRunner] {
    def write(obj: ExecutorRunner) = JsObject(
      "id" -> JsNumber(obj.execId),
      "memory" -> JsNumber(obj.memory),
      "appid" -> JsString(obj.appId),
      "appdesc" -> obj.appDesc.toJson.asJsObject
    )
  }

  implicit object MasterStateJsonFormat extends RootJsonWriter[MasterState] {
    def write(obj: MasterState) = JsObject(
      "url" -> JsString("spark://" + obj.uri),
      "workers" -> JsArray(obj.workers.toList.map(_.toJson)),
      "cores" -> JsNumber(obj.workers.map(_.cores).sum),
      "coresused" -> JsNumber(obj.workers.map(_.coresUsed).sum),
      "memory" -> JsNumber(obj.workers.map(_.memory).sum),
      "memoryused" -> JsNumber(obj.workers.map(_.memoryUsed).sum),
      "activeapps" -> JsArray(obj.activeApps.toList.map(_.toJson)),
      "completedapps" -> JsArray(obj.completedApps.toList.map(_.toJson))
    )
  }

  implicit object WorkerStateJsonFormat extends RootJsonWriter[WorkerState] {
    def write(obj: WorkerState) = JsObject(
      "id" -> JsString(obj.workerId),
      "masterurl" -> JsString(obj.masterUrl),
      "masterwebuiurl" -> JsString(obj.masterWebUiUrl),
      "cores" -> JsNumber(obj.cores),
      "coresused" -> JsNumber(obj.coresUsed),
      "memory" -> JsNumber(obj.memory),
      "memoryused" -> JsNumber(obj.memoryUsed),
      "executors" -> JsArray(obj.executors.toList.map(_.toJson)),
      "finishedexecutors" -> JsArray(obj.finishedExecutors.toList.map(_.toJson))
    )
  }
}
