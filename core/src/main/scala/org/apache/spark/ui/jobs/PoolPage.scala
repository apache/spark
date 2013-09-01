package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest

import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashSet

import org.apache.spark.scheduler.Stage
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._

/** Page showing specific pool details */
private[spark] class PoolPage(parent: JobProgressUI) {
  def listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val poolName = request.getParameter("poolname")
      val poolToActiveStages = listener.poolToActiveStages
      val activeStages = poolToActiveStages.get(poolName).toSeq.flatten
      val activeStagesTable = new StageTable(activeStages.sortBy(_.submissionTime).reverse, parent)

      val pool = listener.sc.getPoolForName(poolName).get
      val poolTable = new PoolTable(Seq(pool), listener)

      val content = <h4>Summary </h4> ++ poolTable.toNodeSeq() ++
                    <h4>{activeStages.size} Active Stages</h4> ++ activeStagesTable.toNodeSeq()

      headerSparkPage(content, parent.sc, "Fair Scheduler Pool: " + poolName, Stages)
    }
  }
}
