package spark.ui.jobs

import java.util.Date
import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark.scheduler.cluster.{SchedulingMode, TaskInfo}
import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._
import spark.Utils
import spark.storage.StorageLevel

/** Page showing list of all ongoing and recently finished stages */
private[spark] class StageTable(val stages: Seq[Stage], val parent: JobProgressUI) {

  val listener = parent.listener
  val dateFmt = parent.dateFmt
  val isFairScheduler = listener.sc.getSchedulingMode == SchedulingMode.FAIR

  def toNodeSeq(): Seq[Node] = {
    listener.synchronized {
      stageTable(stageRow, stages)
    }
  }

  /** Special table which merges two header cells. */
  private def stageTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Stage Id</th>
        {if (isFairScheduler) {<th>Pool Name</th>} else {}}
        <th>Description</th>
        <th>Submitted</th>
        <td>Duration</td>
        <td>Tasks: Succeeded/Total</td>
        <td>Shuffle Read</td>
        <td>Shuffle Write</td>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  private def getElapsedTime(submitted: Option[Long], completed: Long): String = {
    submitted match {
      case Some(t) => parent.formatDuration(completed - t)
      case _ => "Unknown"
    }
  }

  private def makeProgressBar(started: Int, completed: Int, failed: String, total: Int): Seq[Node] = {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    val startWidth = "width: %s%%".format((started.toDouble/total)*100)

    <div class="progress" style="height: 15px; margin-bottom: 0px; position: relative">
      <span style="text-align:center; position:absolute; width:100%;">
        {completed}/{total} {failed}
      </span>
      <div class="bar bar-completed" style={completeWidth}></div>
      <div class="bar bar-running" style={startWidth}></div>
    </div>
  }


  private def stageRow(s: Stage): Seq[Node] = {
    val submissionTime = s.submissionTime match {
      case Some(t) => dateFmt.format(new Date(t))
      case None => "Unknown"
    }

    val shuffleRead = listener.stageToShuffleRead.getOrElse(s.id, 0L) match {
      case 0 => ""
      case b => Utils.memoryBytesToString(b)
    }
    val shuffleWrite = listener.stageToShuffleWrite.getOrElse(s.id, 0L) match {
      case 0 => ""
      case b => Utils.memoryBytesToString(b)
    }

    val startedTasks = listener.stageToTasksActive.getOrElse(s.id, HashSet[TaskInfo]()).size
    val completedTasks = listener.stageToTasksComplete.getOrElse(s.id, 0)
    val failedTasks = listener.stageToTasksFailed.getOrElse(s.id, 0) match {
        case f if f > 0 => "(%s failed)".format(f)
        case _ => ""
    }
    val totalTasks = s.numPartitions

    val poolName = listener.stageToPool.get(s)

    val nameLink = <a href={"/stages/stage?id=%s".format(s.id)}>{s.name}</a>
    val description = listener.stageToDescription.get(s)
      .map(d => <div><em>{d}</em></div><div>{nameLink}</div>).getOrElse(nameLink)

    <tr>
      <td>{s.id}</td>
      {if (isFairScheduler) {
        <td><a href={"/stages/pool?poolname=%s".format(poolName.get)}>{poolName.get}</a></td>}
      }
      <td>{description}</td>
      <td valign="middle">{submissionTime}</td>
      <td>{getElapsedTime(s.submissionTime,
             s.completionTime.getOrElse(System.currentTimeMillis()))}</td>
      <td class="progress-cell">
        {makeProgressBar(startedTasks, completedTasks, failedTasks, totalTasks)}
      </td>
      <td>{shuffleRead}</td>
      <td>{shuffleWrite}</td>
    </tr>
  }
}
