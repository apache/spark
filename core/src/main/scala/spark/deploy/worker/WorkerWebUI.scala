package spark.deploy.worker

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import cc.spray.Directives
import cc.spray.typeconversion.TwirlSupport._
import spark.deploy.{WorkerState, RequestWorkerState}

private[spark]
class WorkerWebUI(val actorSystem: ActorSystem, worker: ActorRef) extends Directives {
  val RESOURCE_DIR = "spark/deploy/worker/webui"
  val STATIC_RESOURCE_DIR = "spark/deploy/static"
  
  implicit val timeout = Timeout(1 seconds)
  
  val handler = {
    get {
      path("") {
        completeWith{
          val future = worker ? RequestWorkerState
          future.map { workerState =>
            spark.deploy.worker.html.index(workerState.asInstanceOf[WorkerState])
          }
        }
      } ~
      path("log") {
        parameters("jobId", "executorId", "logType") { (jobId, executorId, logType) =>
          respondWithMediaType(cc.spray.http.MediaTypes.`text/plain`) {
            getFromFileName("work/" + jobId + "/" + executorId + "/" + logType)
          }
        }
      } ~
      pathPrefix("static") {
        getFromResourceDirectory(STATIC_RESOURCE_DIR)
      } ~
      getFromResourceDirectory(RESOURCE_DIR)
    }
  }
  
}
