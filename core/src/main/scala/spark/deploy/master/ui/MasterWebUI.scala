package spark.deploy.master.ui

import akka.actor.ActorRef
import akka.util.Duration

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import spark.{Logging, Utils}
import spark.ui.JettyUtils
import spark.ui.JettyUtils._

/**
 * Web UI server for the standalone master.
 */
private[spark]
class MasterWebUI(val master: ActorRef) extends Logging {
  implicit val timeout = Duration.create(
    System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")
  val host = Utils.localHostName()
  val port = Option(System.getProperty("master.ui.port"))
    .getOrElse(MasterWebUI.DEFAULT_PORT).toInt

  val applicationPage = new ApplicationPage(this)
  val indexPage = new IndexPage(this)

  def start() {
    try {
      val (server, boundPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      logInfo("Started Master web UI at http://%s:%d".format(host, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Master JettyUtils", e)
        System.exit(1)
    }
  }

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR)),
    ("/app/json", (request: HttpServletRequest) => applicationPage.renderJson(request)),
    ("/app", (request: HttpServletRequest) => applicationPage.render(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )
}

object MasterWebUI {
  val STATIC_RESOURCE_DIR = "spark/webui/static"
  val DEFAULT_PORT = "8080"
}