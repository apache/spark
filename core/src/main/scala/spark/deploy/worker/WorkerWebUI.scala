package spark.deploy.worker

import akka.actor.ActorRef
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._
import java.io.File
import javax.servlet.http.HttpServletRequest
import net.liftweb.json.JsonAST.JValue
import org.eclipse.jetty.server.Handler
import scala.io.Source
import spark.{Utils, Logging}
import spark.deploy.{JsonProtocol, WorkerState, RequestWorkerState}
import spark.ui.{JettyUI => UtilsWebUI}
import spark.ui.JettyUI._
import xml.Node

/**
 * Web UI server for the standalone worker.
 */
private[spark]
class WorkerWebUI(worker: ActorRef, workDir: File) extends Logging {
  implicit val timeout = Timeout(
    Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds"))
  val host = Utils.localHostName()
  val port = Option(System.getProperty("wroker.ui.port"))
    .getOrElse(WorkerWebUI.DEFAULT_PORT).toInt

  val handlers = Array[(String, Handler)](
    ("/static", createStaticHandler(WorkerWebUI.STATIC_RESOURCE_DIR)),
    ("/log", (request: HttpServletRequest) => log(request)),
    ("/json", (request: HttpServletRequest) => indexJson),
    ("*", (request: HttpServletRequest) => index)
  )

  def start() {
    try {
      val (server, boundPort) = UtilsWebUI.startJettyServer("0.0.0.0", port, handlers)
      logInfo("Started Worker web UI at http://%s:%d".format(host, boundPort))
    } catch {
      case e: Exception =>
        logError("Failed to create Worker JettyUI", e)
        System.exit(1)
    }
  }

  def indexJson(): JValue = {
    val stateFuture = (worker ? RequestWorkerState)(timeout).mapTo[WorkerState]
    val workerState = Await.result(stateFuture, 3 seconds)
    JsonProtocol.writeWorkerState(workerState)
  }

  def index(): Seq[Node] = {
    val stateFuture = (worker ? RequestWorkerState)(timeout).mapTo[WorkerState]
    val workerState = Await.result(stateFuture, 3 seconds)
    val content =
      <hr />
      <div class="row"> <!-- Worker Details -->
        <div class="span12">
          <ul class="unstyled">
            <li><strong>ID:</strong> {workerState.workerId}</li>
            <li><strong>
              Master URL:</strong> {workerState.masterUrl}
            </li>
            <li><strong>Cores:</strong> {workerState.cores} ({workerState.coresUsed} Used)</li>
            <li><strong>Memory:</strong> {Utils.memoryMegabytesToString(workerState.memory)}
              ({Utils.memoryMegabytesToString(workerState.memoryUsed)} Used)</li>
          </ul>
          <p><a href={workerState.masterWebUiUrl}>Back to Master</a></p>
        </div>
      </div>
      <hr/>

      <div class="row"> <!-- Running Executors -->
        <div class="span12">
          <h3> Running Executors {workerState.executors.size} </h3>
          <br/>
          {executorTable(workerState.executors)}
        </div>
      </div>
      <hr/>

      <div class="row"> <!-- Finished Executors  -->
        <div class="span12">
          <h3> Finished Executors </h3>
          <br/>
          {executorTable(workerState.finishedExecutors)}
        </div>
      </div>;

    UtilsWebUI.sparkPage(content, "Spark Worker on %s:%s".format(workerState.host, workerState.port))
  }

  def executorTable(executors: Seq[ExecutorRunner]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <tr>
          <th>ExecutorID</th>
          <th>Cores</th>
          <th>Memory</th>
          <th>Job Details</th>
          <th>Logs</th>
        </tr>
      </thead>
      <tbody>
        {executors.map(executorRow)}
    </tbody>
    </table>
  }

  def executorRow(executor: ExecutorRunner): Seq[Node] = {
    <tr>
      <td>{executor.execId}</td>
      <td>{executor.cores}</td>
      <td>{Utils.memoryMegabytesToString(executor.memory)}</td>
      <td>
        <ul class="unstyled">
          <li><strong>ID:</strong> {executor.appId}</li>
          <li><strong>Name:</strong> {executor.appDesc.name}</li>
          <li><strong>User:</strong> {executor.appDesc.user}</li>
        </ul>
      </td>
      <td>
        <a href={"log?appId=%s&executorId=%s&logType=stdout"
          .format(executor.appId, executor.execId)}>stdout</a>
        <a href={"log?appId=%s&executorId=%s&logType=stderr"
          .format(executor.appId, executor.execId)}>stderr</a>
      </td>
    </tr>
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
