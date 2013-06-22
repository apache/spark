package spark.ui.jobs

import akka.util.Duration

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.Some
import scala.xml.{NodeSeq, Node}

import spark.scheduler.Stage
import spark.ui.UIUtils._
import spark.storage.StorageLevel

/** Page showing list of all ongoing and recently finished stages */
class IndexPage(parent: JobProgressUI) {
  val listener = parent.listener
  val dateFmt = parent.dateFmt

  def render(request: HttpServletRequest): Seq[Node] = {
    val stageHeaders = Seq("Stage ID", "Origin", "Submitted", "Duration", "Tasks: Complete/Total",
                           "Shuffle Activity", "RDDs")
    val activeStages = listener.activeStages.toSeq
    val completedStages = listener.completedStages.toSeq

    val activeStageTable: NodeSeq = listingTable(stageHeaders, stageRow, activeStages)
    val completedStageTable = listingTable(stageHeaders, stageRow, completedStages)

    val content = <h2>Active Stages</h2> ++ activeStageTable ++
                  <h2>Completed Stages</h2>  ++ completedStageTable

    headerSparkPage(content, parent.sc, "Spark Stages")
  }

  def getElapsedTime(submitted: Option[Long], completed: Long): String = {
    submitted match {
      case Some(t) => Duration(completed - t, "milliseconds").printHMS
      case _ => "Unknown"
    }
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
      case _ => "None"
    }

    <tr>
      <td><a href={"/stages/stage?id=%s".format(s.id)}>{s.id}</a></td>
      <td>{s.origin}</td>
      <td>{submissionTime}</td>
      <td>{getElapsedTime(s.submissionTime,
             s.completionTime.getOrElse(System.currentTimeMillis()))}</td>
      <td>{listener.stageToTasksComplete.getOrElse(s.id, 0)} / {s.numPartitions}
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
