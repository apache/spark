package spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashSet

import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._

/** Page showing specific pool details */
private[spark] class PoolPage(parent: JobProgressUI) {
  def listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val poolName = request.getParameter("poolname")
    val poolToActiveStages = listener.poolToActiveStages
    val activeStages = poolToActiveStages.getOrElseUpdate(poolName, new HashSet[Stage]).toSeq
    val activeStagesTable = new StageTable(activeStages, parent)

    val pool = listener.sc.getPoolForName(poolName).get
    val poolTable = new PoolTable(Seq(pool), listener)

    val content = <h3>Pool </h3> ++ poolTable.toNodeSeq() ++
                  <h3>Active Stages : {activeStages.size}</h3> ++ activeStagesTable.toNodeSeq()

    headerSparkPage(content, parent.sc, "Spark Pool Details", Jobs)
  }
}
