package spark.deploy.master.ui

import akka.dispatch.Await
import akka.pattern.ask
import akka.util.duration._

import javax.servlet.http.HttpServletRequest

import net.liftweb.json.JsonAST.JValue

import scala.xml.Node

import spark.deploy.{RequestMasterState, JsonProtocol, MasterState}
import spark.deploy.master.ExecutorInfo
import spark.ui.UIUtils

private[spark] class ApplicationPage(parent: MasterWebUI) {
  val master = parent.master
  implicit val timeout = parent.timeout

  /** Executor details for a particular application */
  def renderJson(request: HttpServletRequest): JValue = {
    val appId = request.getParameter("appId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)
    val app = state.activeApps.find(_.id == appId).getOrElse({
      state.completedApps.find(_.id == appId).getOrElse(null)
    })
    JsonProtocol.writeApplicationInfo(app)
  }

  /** Executor details for a particular application */
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = request.getParameter("appId")
    val stateFuture = (master ? RequestMasterState)(timeout).mapTo[MasterState]
    val state = Await.result(stateFuture, 30 seconds)
    val app = state.activeApps.find(_.id == appId).getOrElse({
      state.completedApps.find(_.id == appId).getOrElse(null)
    })

    val executorHeaders = Seq("ExecutorID", "Worker", "Cores", "Memory", "State", "Log Pages")
    val executors = app.executors.values.toSeq
    val executorTable = UIUtils.listingTable(executorHeaders, executorRow, executors)

    val content =
        <hr />
        <div class="row">
          <div class="span12">
            <ul class="unstyled">
              <li><strong>ID:</strong> {app.id}</li>
              <li><strong>Description:</strong> {app.desc.name}</li>
              <li><strong>User:</strong> {app.desc.user}</li>
              <li><strong>Cores:</strong>
                {
                if (app.desc.maxCores == Integer.MAX_VALUE) {
                  "Unlimited %s granted".format(app.coresGranted)
                } else {
                  "%s (%s granted, %s left)".format(
                    app.desc.maxCores, app.coresGranted, app.coresLeft)
                }
                }
              </li>
              <li><strong>Memory per Slave:</strong> {app.desc.memoryPerSlave}</li>
              <li><strong>Submit Date:</strong> {app.submitDate}</li>
              <li><strong>State:</strong> {app.state}</li>
              <li><strong><a href={app.appUiUrl}>Application Detail UI</a></strong></li>
            </ul>
          </div>
        </div>

          <hr/>

        <div class="row"> <!-- Executors -->
          <div class="span12">
            <h3> Executor Summary </h3>
            <br/>
            {executorTable}
          </div>
        </div>;
    UIUtils.basicSparkPage(content, "Application Info: " + app.desc.name)
  }

  def executorRow(executor: ExecutorInfo): Seq[Node] = {
    <tr>
      <td>{executor.id}</td>
      <td>
        <a href={executor.worker.webUiAddress}>{executor.worker.id}</a>
      </td>
      <td>{executor.cores}</td>
      <td>{executor.memory}</td>
      <td>{executor.state}</td>
      <td>
        <a href={"%s/logPage?appId=%s&executorId=%s&logType=stdout&offset=0&byteLength=2000"
          .format(executor.worker.webUiAddress, executor.application.id, executor.id)}>stdout</a>
        <a href={"%s/logPage?appId=%s&executorId=%s&logType=stderr&offset=0&byteLength=2000"
          .format(executor.worker.webUiAddress, executor.application.id, executor.id)}>stderr</a>
      </td>
    </tr>
  }
}
