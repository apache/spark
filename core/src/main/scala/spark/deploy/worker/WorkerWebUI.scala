package spark.deploy.worker

import akka.actor.{ActorRef, ActorSystem}
import cc.spray.Directives

class WorkerWebUI(val actorSystem: ActorSystem, worker: ActorRef) extends Directives {
  val RESOURCE_DIR = "spark/deploy/worker/webui"

  val handler = {
    get {
      path("") {
        getFromResource(RESOURCE_DIR + "/index.html")
      } ~
      getFromResourceDirectory(RESOURCE_DIR)
    }
  }
}
