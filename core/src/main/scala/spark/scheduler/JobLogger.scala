package spark.scheduler

import java.io.PrintWriter
import java.io.File
import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.{Map, HashMap, ListBuffer}
import scala.io.Source
import spark._
import spark.executor.TaskMetrics
import spark.scheduler.cluster.TaskInfo

// used to record runtime information for each job, including RDD graph 
// tasks' start/stop shuffle information and information from outside

sealed trait JobLoggerEvent
case class JobLoggerOnJobStart(job: ActiveJob, properties: Properties) extends JobLoggerEvent
case class JobLoggerOnStageSubmitted(stage: Stage, info: String) extends JobLoggerEvent
case class JobLoggerOnStageCompleted(stageCompleted: StageCompleted) extends JobLoggerEvent
case class JobLoggerOnJobEnd(job: ActiveJob, event: SparkListenerEvents) extends JobLoggerEvent
case class JobLoggerOnTaskEnd(event: CompletionEvent) extends JobLoggerEvent

class JobLogger(val logDirName: String) extends SparkListener with Logging {
  private val logDir =  
    if (System.getenv("SPARK_LOG_DIR") != null)  
      System.getenv("SPARK_LOG_DIR")
    else 
      "/tmp/spark"
  private val jobIDToPrintWriter = new HashMap[Int, PrintWriter] 
  private val stageIDToJobID = new HashMap[Int, Int]
  private val jobIDToStages = new HashMap[Int, ListBuffer[Stage]]
  private val DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private val eventQueue = new LinkedBlockingQueue[JobLoggerEvent]
  
  createLogDir()
  def this() = this(String.valueOf(System.currentTimeMillis()))
  
  def getLogDir = logDir
  def getJobIDtoPrintWriter = jobIDToPrintWriter
  def getStageIDToJobID = stageIDToJobID
  def getJobIDToStages = jobIDToStages
  def getEventQueue = eventQueue
  
  new Thread("JobLogger") {
    setDaemon(true)
    override def run() {
      while (true) {
        val event = eventQueue.take
        if (event != null) {
          logDebug("Got event of type " + event.getClass.getName)
          event match {
            case JobLoggerOnJobStart(job, info) =>
              processJobStartEvent(job, info)
            case JobLoggerOnStageSubmitted(stage, info) =>
              processStageSubmittedEvent(stage, info)
            case JobLoggerOnStageCompleted(stageCompleted) =>
              processStageCompletedEvent(stageCompleted)
            case JobLoggerOnJobEnd(job, event) =>
              processJobEndEvent(job, event)
            case JobLoggerOnTaskEnd(event) =>
              processTaskEndEvent(event)
            case _ =>
          }
        }
      }
    }
  }.start()

  //create a folder for log files, the folder's name is the creation time of the jobLogger
  protected def createLogDir() {
    val dir = new File(logDir + "/" + logDirName + "/")
    if (dir.exists()) {
      return
    }
    if (dir.mkdirs() == false) {
      logError("create log directory error:" + logDir + "/" + logDirName + "/")
    }
  }
  
  // create a log file for one job, the file name is the jobID
  protected def createLogWriter(jobID: Int) {
    try{
      val fileWriter = new PrintWriter(logDir + "/" + logDirName + "/" + jobID)
      jobIDToPrintWriter += (jobID -> fileWriter)
      } catch {
        case e: FileNotFoundException => e.printStackTrace()
      }
  }
  
  // close log file for one job, and clean the stage relationship in stageIDToJobID 
  protected def closeLogWriter(jobID: Int) = 
    jobIDToPrintWriter.get(jobID).foreach { fileWriter => 
      fileWriter.close()
      jobIDToStages.get(jobID).foreach(_.foreach{ stage => 
        stageIDToJobID -= stage.id
      })
      jobIDToPrintWriter -= jobID
      jobIDToStages -= jobID
    }
  
  // write log information to log file, withTime parameter controls whether to recored 
  // time stamp for the information
  protected def jobLogInfo(jobID: Int, info: String, withTime: Boolean = true) {
    var writeInfo = info
    if (withTime) {
      val date = new Date(System.currentTimeMillis())
      writeInfo = DATE_FORMAT.format(date) + ": " +info
    }
    jobIDToPrintWriter.get(jobID).foreach(_.println(writeInfo))
  }
  
  protected def stageLogInfo(stageID: Int, info: String, withTime: Boolean = true) = 
    stageIDToJobID.get(stageID).foreach(jobID => jobLogInfo(jobID, info, withTime))

  protected def buildJobDep(jobID: Int, stage: Stage) {
    if (stage.priority == jobID) {
      jobIDToStages.get(jobID) match {
        case Some(stageList) => stageList += stage
        case None => val stageList = new  ListBuffer[Stage]
                     stageList += stage
                     jobIDToStages += (jobID -> stageList)
      }
      stageIDToJobID += (stage.id -> jobID)
      stage.parents.foreach(buildJobDep(jobID, _))
    }
  }

  protected def recordStageDep(jobID: Int) {
    def getRddsInStage(rdd: RDD[_]): ListBuffer[RDD[_]] = {
      var rddList = new ListBuffer[RDD[_]]
      rddList += rdd
      rdd.dependencies.foreach{ dep => dep match {
          case shufDep: ShuffleDependency[_,_] =>
          case _ => rddList ++= getRddsInStage(dep.rdd)
        }
      }
      rddList
    }
    jobIDToStages.get(jobID).foreach {_.foreach { stage => 
        var depRddDesc: String = ""
        getRddsInStage(stage.rdd).foreach { rdd => 
          depRddDesc += rdd.id + ","
        }
        var depStageDesc: String = ""
        stage.parents.foreach { stage => 
          depStageDesc += "(" + stage.id + "," + stage.shuffleDep.get.shuffleId + ")"
        }
        jobLogInfo(jobID, "STAGE_ID=" + stage.id + " RDD_DEP=(" + 
                   depRddDesc.substring(0, depRddDesc.length - 1) + ")" + 
                   " STAGE_DEP=" + depStageDesc, false)
      }
    }
  }
  
  // generate indents and convert to String
  protected def indentString(indent: Int) = {
    val sb = new StringBuilder()
    for (i <- 1 to indent) {
      sb.append(" ")
    }
    sb.toString()
  }
  
  protected def getRddName(rdd: RDD[_]) = {
    var rddName = rdd.getClass.getName
    if (rdd.name != null) {
      rddName = rdd.name 
    }
    rddName
  }
  
  protected def recordRddInStageGraph(jobID: Int, rdd: RDD[_], indent: Int) {
    val rddInfo = "RDD_ID=" + rdd.id + "(" + getRddName(rdd) + "," + rdd.generator + ")"
    jobLogInfo(jobID, indentString(indent) + rddInfo, false)
    rdd.dependencies.foreach{ dep => dep match {
        case shufDep: ShuffleDependency[_,_] => 
          val depInfo = "SHUFFLE_ID=" + shufDep.shuffleId
          jobLogInfo(jobID, indentString(indent + 1) + depInfo, false)
        case _ => recordRddInStageGraph(jobID, dep.rdd, indent + 1)
      }
    }
  }
  
  protected def recordStageDepGraph(jobID: Int, stage: Stage, indent: Int = 0) {
    var stageInfo: String = ""
    if (stage.isShuffleMap) {
      stageInfo = "STAGE_ID=" + stage.id + " MAP_STAGE SHUFFLE_ID=" + 
                  stage.shuffleDep.get.shuffleId
    }else{
      stageInfo = "STAGE_ID=" + stage.id + " RESULT_STAGE"
    }
    if (stage.priority == jobID) {
      jobLogInfo(jobID, indentString(indent) + stageInfo, false)
      recordRddInStageGraph(jobID, stage.rdd, indent)
      stage.parents.foreach(recordStageDepGraph(jobID, _, indent + 2))
    } else
      jobLogInfo(jobID, indentString(indent) + stageInfo + " JOB_ID=" + stage.priority, false)
  }
  
  // record task metrics into job log files
  protected def recordTaskMetrics(stageID: Int, status: String, 
                                taskInfo: TaskInfo, taskMetrics: TaskMetrics) {
    val info = " TID=" + taskInfo.taskId + " STAGE_ID=" + stageID + 
               " START_TIME=" + taskInfo.launchTime + " FINISH_TIME=" + taskInfo.finishTime + 
               " EXECUTOR_ID=" + taskInfo.executorId +  " HOST=" + taskMetrics.hostname
    val executorRunTime = " EXECUTOR_RUN_TIME=" + taskMetrics.executorRunTime
    val readMetrics = 
      taskMetrics.shuffleReadMetrics match {
        case Some(metrics) => 
          " SHUFFLE_FINISH_TIME=" + metrics.shuffleFinishTime + 
          " BLOCK_FETCHED_TOTAL=" + metrics.totalBlocksFetched + 
          " BLOCK_FETCHED_LOCAL=" + metrics.localBlocksFetched + 
          " BLOCK_FETCHED_REMOTE=" + metrics.remoteBlocksFetched + 
          " REMOTE_FETCH_WAIT_TIME=" + metrics.fetchWaitTime + 
          " REMOTE_FETCH_TIME=" + metrics.remoteFetchTime + 
          " REMOTE_BYTES_READ=" + metrics.remoteBytesRead
        case None => ""
      }
    val writeMetrics = 
      taskMetrics.shuffleWriteMetrics match {
        case Some(metrics) => 
          " SHUFFLE_BYTES_WRITTEN=" + metrics.shuffleBytesWritten
        case None => ""
      }
    stageLogInfo(stageID, status + info + executorRunTime + readMetrics + writeMetrics)
  }
  
  override def onStageSubmitted(stage: Stage, info: String = "") {
    eventQueue.put(JobLoggerOnStageSubmitted(stage, info))
  }

  protected def processStageSubmittedEvent(stage: Stage, info: String) {
    stageLogInfo(stage.id, "STAGE_ID=" + stage.id + " STATUS=SUBMITTED " + info)
  }
  
  override def onStageCompleted(stageCompleted: StageCompleted) {
    eventQueue.put(JobLoggerOnStageCompleted(stageCompleted))
  }

  protected def processStageCompletedEvent(stageCompleted: StageCompleted) {
    stageLogInfo(stageCompleted.stageInfo.stage.id, "STAGE_ID=" + 
                 stageCompleted.stageInfo.stage.id + " STATUS=COMPLETED")
    
  }
  
  override def onTaskEnd(event: CompletionEvent) {
    eventQueue.put(JobLoggerOnTaskEnd(event))
  }

  protected def processTaskEndEvent(event: CompletionEvent) {
    var taskStatus = ""
    event.task match {
      case resultTask: ResultTask[_, _] => taskStatus = "TASK_TYPE=RESULT_TASK"
      case shuffleMapTask: ShuffleMapTask => taskStatus = "TASK_TYPE=SHUFFLE_MAP_TASK"
    }
    event.reason match {
      case Success => taskStatus += " STATUS=SUCCESS"
        recordTaskMetrics(event.task.stageId, taskStatus, event.taskInfo, event.taskMetrics)
      case Resubmitted => 
        taskStatus += " STATUS=RESUBMITTED TID=" + event.taskInfo.taskId + 
                      " STAGE_ID=" + event.task.stageId
        stageLogInfo(event.task.stageId, taskStatus)
      case FetchFailed(bmAddress, shuffleId, mapId, reduceId) => 
        taskStatus += " STATUS=FETCHFAILED TID=" + event.taskInfo.taskId + " STAGE_ID=" + 
                      event.task.stageId + " SHUFFLE_ID=" + shuffleId + " MAP_ID=" + 
                      mapId + " REDUCE_ID=" + reduceId
        stageLogInfo(event.task.stageId, taskStatus)
      case OtherFailure(message) => 
        taskStatus += " STATUS=FAILURE TID=" + event.taskInfo.taskId + 
                      " STAGE_ID=" + event.task.stageId + " INFO=" + message
        stageLogInfo(event.task.stageId, taskStatus)
      case _ =>
    }
  }
  
  override def onJobEnd(job: ActiveJob, event: SparkListenerEvents) {
    eventQueue.put(JobLoggerOnJobEnd(job, event))
  }

  protected def processJobEndEvent(job: ActiveJob, event: SparkListenerEvents) {
    var info = "JOB_ID=" + job.runId + " STATUS="
    var validEvent = true
    event match {
      case SparkListenerJobSuccess => info += "SUCCESS"
      case SparkListenerJobFailed(failedStage) => 
        info += "FAILED REASON=STAGE_FAILED FAILED_STAGE_ID=" + failedStage.id
      case SparkListenerJobCancelled(reason) => info += "CANCELLED REASON=" + reason
      case _ => validEvent = false
    }
    if (validEvent) {
      jobLogInfo(job.runId, info)
      closeLogWriter(job.runId)
    }
  }
  
  protected def recordJobProperties(jobID: Int, properties: Properties) {
    if(properties != null) {
      val annotation = properties.getProperty("spark.job.annotation", "")
      jobLogInfo(jobID, annotation, false)
    }
  }

  override def onJobStart(job: ActiveJob, properties: Properties = null) {
    eventQueue.put(JobLoggerOnJobStart(job, properties))
  }
 
  protected def processJobStartEvent(job: ActiveJob, properties: Properties) {
    createLogWriter(job.runId)
    recordJobProperties(job.runId, properties)
    buildJobDep(job.runId, job.finalStage)
    recordStageDep(job.runId)
    recordStageDepGraph(job.runId, job.finalStage) 
    jobLogInfo(job.runId, "JOB_ID=" + job.runId + " STATUS=STARTED")
  }
}
