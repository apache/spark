package spark.ui.storage

import akka.util.Duration
import javax.servlet.http.HttpServletRequest
import org.eclipse.jetty.server.Handler
import spark.{Logging, SparkContext}
import spark.ui.WebUI._
import spark.ui.{UIComponent}

/**
 * Web UI server for the BlockManager inside each SparkContext.
 */
private[spark]
class BlockManagerUI(val sc: SparkContext)
    extends UIComponent with Logging  {
  implicit val timeout = Duration.create(
    System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")

  val indexPage = new IndexPage(this)
  val rddPage = new RDDPage(this)

  def getHandlers = Seq[(String, Handler)](
    ("/storage/rdd", (request: HttpServletRequest) => rddPage.render(request)),
    ("/storage", (request: HttpServletRequest) => indexPage.render(request))
  )
}
