package spark.deploy.worker.ui

import akka.actor.ActorRef
import akka.util.{Duration, Timeout}

import java.io.{FileInputStream, File}

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import spark.{Utils, Logging}
import spark.ui.JettyUtils
import spark.ui.JettyUtils._

import scala.xml._
import spark.ui.UIUtils
import scala.io.Source._

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(val worker: ActorRef, val workDir: File, requestedPort: Option[Int] = None)
    extends Logging {
  implicit val timeout = Timeout(
    Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds"))
  val host = Utils.localHostName()
  val port = requestedPort.getOrElse(
    System.getProperty("worker.ui.port", WorkerWebUI.DEFAULT_PORT).toInt)

  var server: Option[Server] = None
  var boundPort: Option[Int] = None

  val indexPage = new IndexPage(this)

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(WorkerWebUI.STATIC_RESOURCE_DIR)),
    ("/log", (request: HttpServletRequest) => log(request)),
    ("/logPage", (request: HttpServletRequest) => logPage(request)),
    ("/json", (request: HttpServletRequest) => indexPage.renderJson(request)),
    ("*", (request: HttpServletRequest) => indexPage.render(request))
  )

  def start() {
    try {
      val (srv, bPort) = JettyUtils.startJettyServer("0.0.0.0", port, handlers)
      server = Some(srv)
      boundPort = Some(bPort)
      logInfo("Started Worker web UI at http://%s:%d".format(host, bPort))
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

    val maxBytes = 1024 * 1024 // Guard against OOM
    val defaultBytes = 100 * 1024
    val numBytes = Option(request.getParameter("numBytes"))
      .flatMap(s => Some(s.toInt)).getOrElse(defaultBytes)

    val path = "%s/%s/%s/%s".format(workDir.getPath, appId, executorId, logType)
    val pre = "==== Last %s bytes of %s/%s/%s ====\n".format(numBytes, appId, executorId, logType)
    pre + Utils.lastNBytes(path, math.min(numBytes, maxBytes))
  }

  def logPage(request: HttpServletRequest): Seq[scala.xml.Node] = {
    val appId = request.getParameter("appId")
    val executorId = request.getParameter("executorId")
    val logType = request.getParameter("logType")
    val getOffset = request.getParameter("offset")
    val getLineLength = request.getParameter("lineLength")
    val path = "%s/%s/%s/%s".format(workDir.getPath, appId, executorId, logType)
    val source = fromFile(path)
    val lines = source.getLines().toArray
    val logLength = lines.length
    val offset = {
      if (getOffset == null) 0
      else if (getOffset.toInt < 0) 0
      else getOffset.toInt
    }
    val lineLength = {
      if (getLineLength == null) 0
      else getLineLength.toInt
    }
    val logText = "<node>" + lines.slice(offset, offset+lineLength).mkString("\n") + "</node>"
    val logXML = XML.loadString(logText)
    val backButton =
      if (offset > 0) {
        if (offset-lineLength < 0) {
          <a href={"?appId=%s&executorId=%s&logType=%s&offset=0&lineLength=%s".format(appId, executorId, logType, lineLength)}> <button style="float:left">back</button> </a>
        }
        else {
          <a href={"?appId=%s&executorId=%s&logType=%s&offset=%s&lineLength=%s".format(appId, executorId, logType, offset-lineLength, lineLength)}> <button style="float:left">back</button> </a>
        }
      }
      else {
        <button style="float:left" disabled="disabled">back</button>
      }
    val nextButton =
      if (offset+lineLength < logLength) {
        <a href={"?appId=%s&executorId=%s&logType=%s&offset=%s&lineLength=%s".format(appId, executorId, logType, offset+lineLength, lineLength)}> <button style="float:right">next</button> </a>
      }
      else {
        <button style="float:right" disabled="disabled">next</button>
      }
    val content =
      <html>
        <body>
          <hr></hr>
          {backButton}
          {nextButton}
          <br></br>
          <pre>{logXML}</pre>
          {backButton}
          {nextButton}
        </body>
      </html>
    source.close()
    UIUtils.basicSparkPage(content, "Log Page for " + appId)
  }

  def stop() {
    server.foreach(_.stop())
  }
}

private[spark] object WorkerWebUI {
  val STATIC_RESOURCE_DIR = "spark/ui/static"
  val DEFAULT_PORT="8081"
}
