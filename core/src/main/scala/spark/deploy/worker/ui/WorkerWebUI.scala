package spark.deploy.worker.ui

import akka.actor.ActorRef
import akka.util.{Duration, Timeout}

import java.io.File

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.io.Source

import spark.{Utils, Logging}
import spark.ui.JettyUtils
import spark.ui.JettyUtils._

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(val worker: ActorRef, val workDir: File) extends Logging {
  implicit val timeout = Timeout(
    Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds"))
  val host = Utils.localHostName()
  val port = Option(System.getProperty("wroker.ui.port"))
    .getOrElse(WorkerWebUI.DEFAULT_PORT).toInt

  val indexPage = new IndexPage(this)

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(WorkerWebUI.STATIC_RESOURCE_DIR)),
    ("/log", (request: HttpServletRequest) => log(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def start() {
    try {
      val (server, boundPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      logInfo("Started Worker web UI at http://%s:%d".format(host, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Worker JettyUtils", e)
        System.exit(1)
    }
  }

  def log(request: HttpServletRequest): String = {
    val appId = request.getParameter("appId")
    val executorId = request.getParameter("executorId")
    val logType = request.getParameter("logType")
    val path = "%s/%s/%s/%s".format(workDir.getPath, appId, executorId, logType)
    val source = Source.fromFile(path)
    val lines = source.mkString
    source.close()
    lines
  }
}

object WorkerWebUI {
  val STATIC_RESOURCE_DIR = "spark/webui/static"
  val DEFAULT_PORT="8081"
}
