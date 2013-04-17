package spark.deploy.worker

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._
import cc.spray.Directives
import cc.spray.typeconversion.TwirlSupport._
import cc.spray.http.MediaTypes
import cc.spray.typeconversion.SprayJsonSupport._

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
  
  implicit val timeout = Timeout(Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds"))
  
  val handler = {
    get {
      (path("") & parameters('format ?)) {
        case Some(js) if js.equalsIgnoreCase("json") => {
          val future = worker ? RequestWorkerState
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            ctx.complete(future.mapTo[WorkerState])
          }
        }
        case _ =>
          completeWith{
            val future = worker ? RequestWorkerState
            future.map { workerState =>
              spark.deploy.worker.html.index(workerState.asInstanceOf[WorkerState])
            }
          }
      } ~
      path("log") {
        parameters("appId", "executorId", "logType") { (appId, executorId, logType) =>
          respondWithMediaType(cc.spray.http.MediaTypes.`text/plain`) {
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
