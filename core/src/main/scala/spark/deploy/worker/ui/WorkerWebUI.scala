package spark.deploy.worker.ui

import akka.actor.ActorRef
import akka.util.{Duration, Timeout}

import java.io.{FileInputStream, File}

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.{Handler, Server}

import spark.deploy.worker.Worker
import spark.{Utils, Logging}
import spark.ui.JettyUtils
import spark.ui.JettyUtils._
import spark.ui.UIUtils

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(val worker: Worker, val workDir: File, requestedPort: Option[Int] = None)
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

    val maxBytes = 1024 * 1024
    val defaultBytes = 10000
    val byteLength = Option(request.getParameter("byteLength")).map(_.toInt).getOrElse(defaultBytes)

    val path = "%s/%s/%s/%s".format(workDir.getPath, appId, executorId, logType)
    val logLength = new File(path).length()
    val offset = Option(request.getParameter("offset")).map(_.toLong).getOrElse(logLength-10000)
    val logPageLength = math.min(byteLength, maxBytes)

    val fixedOffset =
      if (offset < 0) 0
      else if (offset > logLength) logLength
      else offset

    val endOffset = math.min(fixedOffset+logPageLength, logLength)

    val linkToMaster = <p><a href={worker.masterWebUiUrl}>Back to Master</a></p>

    val range = <span>Bytes {fixedOffset.toString} - {(endOffset).toString} of {logLength}</span>

    val backButton =
      if (fixedOffset > 0) {
        <a href={"?appId=%s&executorId=%s&logType=%s&offset=%s&byteLength=%s"
          .format(appId, executorId, logType, math.max(fixedOffset-logPageLength, 0),
            logPageLength)}>
          <button>Previous {math.min(logPageLength, fixedOffset)} Bytes</button>
        </a>
      }
      else {
        <button disabled="disabled">Previous 0 Bytes</button>
      }

    val nextButton =
      if (endOffset < logLength) {
        <a href={"?appId=%s&executorId=%s&logType=%s&offset=%s&byteLength=%s".
          format(appId, executorId, logType, endOffset, logPageLength)}>
          <button>Next {math.min(logPageLength, logLength-endOffset)} Bytes</button>
        </a>
      }
      else {
        <button disabled="disabled">Next 0 Bytes</button>
      }

    val logText = <node>{Utils.offsetBytes(path, fixedOffset, endOffset)}</node>

    val content =
      <html>
        <body>
          {linkToMaster}
          <hr />
          <div>
            <div style="float:left;width:40%">{backButton}</div>
            <div style="float:left;">{range}</div>
            <div style="float:right;">{nextButton}</div>
          </div>
          <br />
          <div style="height:500px;overflow:scroll;padding:5px;">
            <pre>{logText}</pre>
          </div>
        </body>
      </html>
    UIUtils.basicSparkPage(content, logType + " log Page for " + appId)
  }

  def stop() {
    server.foreach(_.stop())
  }
}

private[spark] object WorkerWebUI {
  val STATIC_RESOURCE_DIR = "spark/ui/static"
  val DEFAULT_PORT="8081"
}
