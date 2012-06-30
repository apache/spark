package spark.deploy.master

import akka.actor.{ActorRef, ActorSystem}
import cc.spray.Directives

class MasterWebUI(val actorSystem: ActorSystem, master: ActorRef) extends Directives {
  val RESOURCE_DIR = "spark/deploy/master/webui"

  val handler = {
    get {
      path("") {
        getFromResource(RESOURCE_DIR + "/index.html")
      } ~
      getFromResourceDirectory(RESOURCE_DIR)
    }
  }
}
