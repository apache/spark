package spark.ui

import spark.{Logging, SparkContext, Utils}
import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.server.Handler
import WebUI._

private[spark] class SparkUI(sc: SparkContext) extends Logging {
  val host = Utils.localHostName()
  val port = Option(System.getProperty("spark.ui.port")).getOrElse(SparkUI.DEFAULT_PORT).toInt


  val handlers = Seq[(String, Handler)](
    ("/static", createStaticHandler(SparkUI.STATIC_RESOURCE_DIR)),
    ("*", (request: HttpServletRequest) => WebUI.headerSparkPage(<h1>Test</h1>, "Test page"))
  )
  val components = Seq(new BlockManagerUI(sc), new JobProgressUI(sc))

  def start() {
    /** Start an HTTP server to run the Web interface */
    try {
      val allHandlers = components.flatMap(_.getHandlers) ++ handlers
      val (server, boundPort) = WebUI.startJettyServer("0.0.0.0", port, allHandlers)
      logInfo("Started Spark Web UI at http://%s:%d".format(host, boundPort))
    } catch {
    case e: Exception =>
      logError("Failed to create Spark WebUI", e)
      System.exit(1)
    }
  }

  private[spark] def appUIAddress = "http://" + host + ":" + port
}

object SparkUI {
  val DEFAULT_PORT = "33000"
  val STATIC_RESOURCE_DIR = "spark/webui/static"
}



