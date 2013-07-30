package spark.ui.jobs

import java.util.Date
import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}
import scala.collection.mutable.HashMap

import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.ui.Page._
import spark.storage.StorageLevel

/*
 * Interface to get stage's pool name
 */
private[spark] trait StagePoolInfo {
  def getStagePoolName(s: Stage): String
  
  def hasHerf: Boolean
}

/*
 * For FIFO scheduler algorithm, just show "N/A" and its link status is false
 */
private[spark] class FIFOStagePoolInfo extends StagePoolInfo {
  def getStagePoolName(s: Stage): String = "N/A"
  
  def hasHerf: Boolean = false
} 

/*
 * For Fair scheduler algorithm, show its pool name  and pool detail  link status is true
 */
private[spark] class FairStagePoolInfo(listener: JobProgressListener) extends StagePoolInfo {
  def getStagePoolName(s: Stage): String = {
    listener.stageToPool(s)
  }

  def hasHerf: Boolean = true
}

/** Page showing list of all ongoing and recently finished stages */
private[spark] class StageTable(
  val stages: Seq[Stage],
  val parent: JobProgressUI) {

  val listener = parent.listener
  val dateFmt = parent.dateFmt
  var stagePoolInfo = parent.stagePoolInfo
  
  def toNodeSeq(): Seq[Node] = {
    stageTable(stageRow, stages)
  }

  /** Special table which merges two header cells. */
  def stageTable[T](makeRow: T => Seq[Node], rows: Seq[T]): Seq[Node] = {
     <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Stage Id</th>
        <th>Pool Name</th>
        <th>Origin</th>
        <th>Submitted</th>
        <td>Duration</td>
        <td colspan="2">Tasks: Complete/Total</td>
        <td>Shuffle Activity</td>
        <td>Stored RDD</td>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  def getElapsedTime(submitted: Option[Long], completed: Long): String = {
    submitted match {
      case Some(t) => parent.formatDuration(completed - t)
      case _ => "Unknown"
    }
  }

  def makeProgressBar(completed: Int, total: Int): Seq[Node] = {
    val width=130
    val height=15
    val completeWidth = (completed.toDouble / total) * width

    <svg width={width.toString} height={height.toString}>
      <rect width={width.toString} height={height.toString}
            fill="white" stroke="rgb(51,51,51)" stroke-width="1" />
      <rect width={completeWidth.toString} height={height.toString}
            fill="rgb(0,136,204)" stroke="black" stroke-width="1" />
    </svg>
  }


  def stageRow(s: Stage): Seq[Node] = {
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

    val poolName = stagePoolInfo.getStagePoolName(s)

    <tr>
      <td>{s.id}</td>
      <td>{if (stagePoolInfo.hasHerf) {
        <a href={"/stages/pool?poolname=%s".format(poolName)}>{poolName}</a>
          } else {
          {poolName}
          }}</td>
      <td><a href={"/stages/stage?id=%s".format(s.id)}>{s.origin}</a></td>
      <td>{submissionTime}</td>
      <td>{getElapsedTime(s.submissionTime,
             s.completionTime.getOrElse(System.currentTimeMillis()))}</td>
      <td class="progress-cell">{makeProgressBar(completedTasks, totalTasks)}</td>
      <td style="border-left: 0; text-align: center;">{completedTasks} / {totalTasks}
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
