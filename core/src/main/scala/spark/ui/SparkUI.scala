package spark.ui

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import spark.{Logging, SparkContext, Utils}
import spark.ui.storage.BlockManagerUI
import spark.ui.jobs.JobProgressUI
import spark.ui.UIUtils._
import spark.ui.JettyUI._

/** Top level user interface for Spark */
private[spark] class SparkUI(sc: SparkContext) extends Logging {
  val host = Utils.localHostName()
  val port = Option(System.getProperty("spark.ui.port")).getOrElse(SparkUI.DEFAULT_PORT).toInt
  var boundPort: Option[Int] = None

  val handlers = Seq[(String, Handler)](
    ("/static", createStaticHandler(SparkUI.STATIC_RESOURCE_DIR)),
    ("*", (request: HttpServletRequest) => headerSparkPage(<h1>Test</h1>, "Test page"))
  )
  val storage = new BlockManagerUI(sc)
  val jobs = new JobProgressUI(sc)
  val allHandlers = handlers ++ storage.getHandlers ++ jobs.getHandlers

  def start() {
    /** Start an HTTP server to run the Web interface */
    try {
      val (server, usedPort) = JettyUI.startJettyServer("0.0.0.0", port, allHandlers)
      logInfo("Started Spark Web UI at http://%s:%d".format(host, usedPort))
      boundPort = Some(usedPort)
    } catch {
    case e: Exception =>
      logError("Failed to create Spark JettyUI", e)
      System.exit(1)
    }
  }

  private[spark] def appUIAddress = "http://" + host + ":" + boundPort.getOrElse("-1")
}

object SparkUI {
  val DEFAULT_PORT = "33000"
  val STATIC_RESOURCE_DIR = "spark/webui/static"
}
