package spark.ui.jobs

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._
import spark.storage.StorageLevel
import spark.scheduler.cluster.Schedulable
import spark.scheduler.cluster.SchedulingMode
import spark.scheduler.cluster.SchedulingMode.SchedulingMode

/** Page showing list of all ongoing and recently finished stages and pools*/
private[spark] class IndexPage(parent: JobProgressUI) {
  def listener = parent.listener

  def stageTable: StageTable = parent.stageTable

  def poolTable: PoolTable = parent.poolTable

  def render(request: HttpServletRequest): Seq[Node] = {
    val activeStages = listener.activeStages.toSeq
    val completedStages = listener.completedStages.reverse.toSeq
    val failedStages = listener.failedStages.reverse.toSeq

    stageTable.setStagePoolInfo(parent.stagePoolInfo)
    poolTable.setPoolSource(parent.stagePagePoolSource)

    val activeStageNodeSeq = stageTable.toNodeSeq(activeStages)
    val completedStageNodeSeq = stageTable.toNodeSeq(completedStages)
    val failedStageNodeSeq = stageTable.toNodeSeq(failedStages)

    val content = <div class="row">
                    <div class="span12">
                      <ul class="unstyled">
                        <li><strong>Active Stages Number:</strong> {activeStages.size} </li>
                        <li><strong>Completed Stages Number:</strong> {completedStages.size} </li>
                        <li><strong>Failed Stages Number:</strong> {failedStages.size} </li>
                        <li><strong>Scheduling Mode:</strong> {parent.sc.getSchedulingMode}</li>
                      </ul>
                    </div>
                  </div> ++
                  <h3>Pools </h3> ++ poolTable.toNodeSeq ++
                  <h3>Active Stages : {activeStages.size}</h3> ++ activeStageNodeSeq++
                  <h3>Completed Stages : {completedStages.size}</h3> ++ completedStageNodeSeq++
                  <h3>Failed Stages : {failedStages.size}</h3> ++ failedStageNodeSeq

    headerSparkPage(content, parent.sc, "Spark Stages/Pools", Jobs)
  }
}
