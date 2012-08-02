package spark.deploy.master

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import cc.spray.Directives
import cc.spray.directives._
import cc.spray.typeconversion.TwirlSupport._
import spark.deploy._

class MasterWebUI(val actorSystem: ActorSystem, master: ActorRef) extends Directives {
  val RESOURCE_DIR = "spark/deploy/master/webui"

  val handler = {
    get {
      path("") {
        completeWith {
          val masterState = getMasterState()
          // Render the HTML
          masterui.html.index.render(masterState)
        }
      } ~
      path("job") {
        parameter("jobId") { jobId =>
          completeWith {
            val masterState = getMasterState
            // A bit ugly an inefficient, but we won't have a number of jobs so large that it will make a significant difference.
            (masterState.activeJobs ::: masterState.completedJobs).find(_.id == jobId) match {
              case Some(job) => masterui.html.job_details.render(job)
              case _ => null
            }
          }
        }
      } ~
      getFromResourceDirectory(RESOURCE_DIR)
    }
  }
  
  // Requests the current state from the Master and waits for the response
  def getMasterState() : MasterState = {
    implicit val timeout = Timeout(1 seconds)
    val future = master ? RequestMasterState
    return Await.result(future, timeout.duration).asInstanceOf[MasterState]
  }
  
}
