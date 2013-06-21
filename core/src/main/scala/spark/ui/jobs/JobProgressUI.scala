package spark.ui.jobs

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.Seq
import scala.collection.mutable.{HashSet, ListBuffer, HashMap, ArrayBuffer}

import spark.ui.JettyUtils._
import spark.SparkContext
import spark.scheduler._
import spark.scheduler.cluster.TaskInfo
import spark.executor.TaskMetrics
import spark.Success

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[spark] class JobProgressUI(sc: SparkContext) {
  val listener = new JobProgressListener
  val dateFmt = new SimpleDateFormat("EEE, MMM d yyyy HH:mm:ss")

  sc.addSparkListener(listener)

  private val indexPage = new IndexPage(this)
  private val stagePage = new StagePage(this)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage.render(request)),
    ("/stages", (request: HttpServletRequest) => indexPage.render(request))
  )
}

private[spark] class JobProgressListener extends SparkListener {
  // TODO(pwendell) Currently does not handle entirely failed stages

  // How many stages to remember
  val RETAINED_STAGES = 1000

  val activeStages = HashSet[Stage]()
  val stageToTasksComplete = HashMap[Int, Int]()
  val stageToTasksFailed = HashMap[Int, Int]()
  val stageToTaskInfos = HashMap[Int, ArrayBuffer[(TaskInfo, TaskMetrics)]]()
  val completedStages = ListBuffer[Stage]()

  override def onJobStart(jobStart: SparkListenerJobStart) { }

  override def onStageCompleted(stageCompleted: StageCompleted) = {
    val stage = stageCompleted.stageInfo.stage
    activeStages -= stage
    stage +=: completedStages
    if (completedStages.size > RETAINED_STAGES) purgeStages()
  }

  /** Remove and garbage collect old stages */
  def purgeStages() {
    val toRemove = RETAINED_STAGES / 10
    completedStages.takeRight(toRemove).foreach( s => {
      stageToTasksComplete.remove(s.id)
      stageToTasksFailed.remove(s.id)
      stageToTaskInfos.remove(s.id)
    })
    completedStages.trimEnd(toRemove)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
    activeStages += stageSubmitted.stage

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val sid = taskEnd.event.task.stageId
    taskEnd.event.reason match {
      case Success =>
        stageToTasksComplete(sid) = stageToTasksComplete.getOrElse(sid, 0) + 1
      case _ =>
        stageToTasksFailed(sid) = stageToTasksFailed.getOrElse(sid, 0) + 1
    }
    val taskList = stageToTaskInfos.getOrElse(sid, ArrayBuffer[(TaskInfo, TaskMetrics)]())
    taskList += ((taskEnd.event.taskInfo, taskEnd.event.taskMetrics))
    stageToTaskInfos(sid) = taskList
  }

  override def onJobEnd(jobEnd: SparkListenerEvents) { }

  /** Is this stage's input from a shuffle read. */
  def hasShuffleRead(stageID: Int): Boolean = {
    // This is written in a slightly complicated way to avoid having to scan all tasks
    for (s <- stageToTaskInfos.get(stageID).getOrElse(Seq())) {
      if (s._2 != null) return s._2.shuffleReadMetrics.isDefined
    }
    return false // No tasks have finished for this stage
  }

  /** Is this stage's output to a shuffle write. */
  def hasShuffleWrite(stageID: Int): Boolean = {
    // This is written in a slightly complicated way to avoid having to scan all tasks
    for (s <- stageToTaskInfos.get(stageID).getOrElse(Seq())) {
      if (s._2 != null) return s._2.shuffleWriteMetrics.isDefined
    }
    return false // No tasks have finished for this stage
  }
}