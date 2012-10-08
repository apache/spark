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

private[spark]
class MasterWebUI(val actorSystem: ActorSystem, master: ActorRef) extends Directives {
  val RESOURCE_DIR = "spark/deploy/master/webui"
  val STATIC_RESOURCE_DIR = "spark/deploy/static"
  
  implicit val timeout = Timeout(1 seconds)
  
  val handler = {
    get {
      path("") {
        completeWith {
          val future = master ? RequestMasterState
          future.map { 
            masterState => spark.deploy.master.html.index.render(masterState.asInstanceOf[MasterState])
          }
        }
      } ~
      path("job") {
        parameter("jobId") { jobId =>
          completeWith {
            val future = master ? RequestMasterState
            future.map { state => 
              val masterState = state.asInstanceOf[MasterState]
              
              // A bit ugly an inefficient, but we won't have a number of jobs 
              // so large that it will make a significant difference.
              (masterState.activeJobs ::: masterState.completedJobs).find(_.id == jobId) match {
                case Some(job) => spark.deploy.master.html.job_details.render(job)
                case _ => null
              }
            }
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
