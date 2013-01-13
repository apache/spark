package spark.deploy

import master.{JobInfo, WorkerInfo}
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
      "webuiaddress" -> JsString(obj.webUiAddress),
      "cores" -> JsNumber(obj.cores),
      "coresused" -> JsNumber(obj.coresUsed),
      "memory" -> JsNumber(obj.memory),
      "memoryused" -> JsNumber(obj.memoryUsed)
    )
  }

  implicit object JobInfoJsonFormat extends RootJsonWriter[JobInfo] {
    def write(obj: JobInfo) = JsObject(
      "starttime" -> JsNumber(obj.startTime),
      "id" -> JsString(obj.id),
      "name" -> JsString(obj.desc.name),
      "cores" -> JsNumber(obj.desc.cores),
      "user" -> JsString(obj.desc.user),
      "memoryperslave" -> JsNumber(obj.desc.memoryPerSlave),
      "submitdate" -> JsString(obj.submitDate.toString))
  }

  implicit object JobDescriptionJsonFormat extends RootJsonWriter[JobDescription] {
    def write(obj: JobDescription) = JsObject(
      "name" -> JsString(obj.name),
      "cores" -> JsNumber(obj.cores),
      "memoryperslave" -> JsNumber(obj.memoryPerSlave),
      "user" -> JsString(obj.user)
    )
  }

  implicit object ExecutorRunnerJsonFormat extends RootJsonWriter[ExecutorRunner] {
    def write(obj: ExecutorRunner) = JsObject(
      "id" -> JsNumber(obj.execId),
      "memory" -> JsNumber(obj.memory),
      "jobid" -> JsString(obj.jobId),
      "jobdesc" -> obj.jobDesc.toJson.asJsObject
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
      "activejobs" -> JsArray(obj.activeJobs.toList.map(_.toJson)),
      "completedjobs" -> JsArray(obj.completedJobs.toList.map(_.toJson))
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
