package spark.deploy

import akka.actor.{ActorRef, ActorSystem}
import cc.spray.Directives

class MasterWebUI(val actorSystem: ActorSystem, master: ActorRef) extends Directives {
  val handler = {
    path("") {
      get { _.complete("Hello world!") }
    }
  }
}
