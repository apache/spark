package spark.deploy.master

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._
import cc.spray.Directives
import cc.spray.directives._
import cc.spray.typeconversion.TwirlSupport._
import cc.spray.http.MediaTypes
import cc.spray.typeconversion.SprayJsonSupport._

import spark.deploy._
import spark.deploy.JsonProtocol._

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val actorSystem: ActorSystem, master: ActorRef) extends Directives {
  val RESOURCE_DIR = "spark/deploy/master/webui"
  val STATIC_RESOURCE_DIR = "spark/deploy/static"
  
  implicit val timeout = Timeout(Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds"))
  
  val handler = {
    get {
      (path("") & parameters('format ?)) {
        case Some(js) if js.equalsIgnoreCase("json") =>
          val future = master ? RequestMasterState
          respondWithMediaType(MediaTypes.`application/json`) { ctx =>
            ctx.complete(future.mapTo[MasterState])
          }
        case _ =>
          completeWith {
            val future = master ? RequestMasterState
            future.map {
              masterState => spark.deploy.master.html.index.render(masterState.asInstanceOf[MasterState])
            }
          }
      } ~
      path("app") {
        parameters("appId", 'format ?) {
          case (appId, Some(js)) if (js.equalsIgnoreCase("json")) =>
            val future = master ? RequestMasterState
            val appInfo = for (masterState <- future.mapTo[MasterState]) yield {
              masterState.activeApps.find(_.id == appId).getOrElse({
                masterState.completedApps.find(_.id == appId).getOrElse(null)
              })
            }
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              ctx.complete(appInfo.mapTo[ApplicationInfo])
            }
          case (appId, _) =>
            completeWith {
              val future = master ? RequestMasterState
              future.map { state =>
                val masterState = state.asInstanceOf[MasterState]
                val app = masterState.activeApps.find(_.id == appId).getOrElse({
                  masterState.completedApps.find(_.id == appId).getOrElse(null)
                })
                spark.deploy.master.html.app_details.render(app)
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
