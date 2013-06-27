package spark.ui.jobs

import akka.util.Duration

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}

import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._
import spark.storage.StorageLevel

/** Page showing list of all ongoing and recently finished stages */
private[spark] class IndexPage(parent: JobProgressUI) {
  def listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    val stageHeaders = Seq("Stage ID", "Origin", "Submitted", "Duration", "Progress",
                           "Tasks: Complete/Total", "Shuffle Activity", "Stored RDD")
    val activeStages = listener.activeStages.toSeq
    val completedStages = listener.completedStages.reverse.toSeq
    val failedStages = listener.failedStages.reverse.toSeq

    val activeStageTable: NodeSeq = listingTable(stageHeaders, stageRow(), activeStages)
    val completedStageTable = listingTable(stageHeaders, stageRow(), completedStages)
    val failedStageTable: NodeSeq = listingTable(stageHeaders, stageRow(false), failedStages)

    val content = <h2>Active Stages</h2> ++ activeStageTable ++
                  <h2>Completed Stages</h2>  ++ completedStageTable ++
                  <h2>Failed Stages</h2>  ++ failedStageTable

    headerSparkPage(content, parent.sc, "Spark Stages", Jobs)
  }

  def getElapsedTime(submitted: Option[Long], completed: Long): String = {
    submitted match {
      case Some(t) => parent.formatDuration(completed - t)
      case _ => "Unknown"
    }
  }

  def makeSlider(completed: Int, total: Int): Seq[Node] = {
    val width=130
    val height=15
    val completeWidth = (completed.toDouble / total) * width

    <svg width={width.toString} height={height.toString}>
      <rect width={width.toString} height={height.toString}
            fill="white" stroke="black" stroke-width="1" />
      <rect width={completeWidth.toString} height={height.toString}
            fill="rgb(206,206,247)" stroke="black" stroke-width="1" />
    </svg>
  }


  def stageRow(showLink: Boolean = true)(s: Stage): Seq[Node] = {
    val submissionTime = s.submissionTime match {
      case Some(t) => dateFmt.format(new Date(t))
      case None => "Unknown"
    }
    val (read, write) = (listener.hasShuffleRead(s.id), listener.hasShuffleWrite(s.id))
    val shuffleInfo = (read, write) match {
      case (true, true) => "Read/Write"
      case (true, false) => "Read"
      case (false, true) => "Write"
      case _ => ""
    }
    val completedTasks = listener.stageToTasksComplete.getOrElse(s.id, 0)
    val totalTasks = s.numPartitions

    <tr>
      {if (showLink) {<td><a href={"/stages/stage?id=%s".format(s.id)}>{s.id}</a></td>}
       else {<td>{s.id}</td>}}
      <td>{s.origin}</td>
      <td>{submissionTime}</td>
      <td>{getElapsedTime(s.submissionTime,
             s.completionTime.getOrElse(System.currentTimeMillis()))}</td>
      <td>{makeSlider(completedTasks, totalTasks)}</td>
      <td>{completedTasks} / {totalTasks}
        {listener.stageToTasksFailed.getOrElse(s.id, 0) match {
        case f if f > 0 => "(%s failed)".format(f)
        case _ =>
      }}
      </td>
      <td>{shuffleInfo}</td>
      <td>{if (s.rdd.getStorageLevel != StorageLevel.NONE) {
             <a href={"/storage/rdd?id=%s".format(s.rdd.id)}>
               {Option(s.rdd.name).getOrElse(s.rdd.id)}
             </a>
          }}
      </td>
    </tr>
  }
}
