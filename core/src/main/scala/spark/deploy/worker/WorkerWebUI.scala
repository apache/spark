package spark.deploy.worker

import akka.actor.{ActorRef, ActorContext}
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import spray.routing.Directives
import spray.httpx.TwirlSupport._
import spray.httpx.SprayJsonSupport._
import spray.http.MediaTypes._

import spark.deploy.{WorkerState, RequestWorkerState}
import spark.deploy.JsonProtocol._
import java.io.File

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(val actorSystem: ActorSystem, worker: ActorRef, workDir: File) extends Directives {
  val RESOURCE_DIR = "spark/deploy/worker/webui"
  val STATIC_RESOURCE_DIR = "spark/deploy/static"

  implicit val timeout = Timeout(10 seconds)

  val handler = {
    get {
      (path("")  & parameters('format ?)) {
        case Some(js) if js.equalsIgnoreCase("json") => {
          val future = (worker ? RequestWorkerState).mapTo[WorkerState]
          respondWithMediaType(`application/json`) { ctx =>
            ctx.complete(future)
          }
        }
        case _ =>
          complete {
            val future = (worker ? RequestWorkerState).mapTo[WorkerState]
            future.map { workerState =>
              spark.deploy.worker.html.index(workerState)
            }
          }
      } ~
      path("log") {
        parameters("jobId", "executorId", "logType") { (jobId, executorId, logType) =>
          respondWithMediaType(`text/plain`) {
            getFromFileName(workDir.getPath() + "/" + appId + "/" + executorId + "/" + logType)
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
