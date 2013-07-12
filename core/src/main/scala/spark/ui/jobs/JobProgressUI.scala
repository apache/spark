package spark.ui.jobs

import akka.util.Duration

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import scala.Seq
import scala.collection.mutable.{HashSet, ListBuffer, HashMap, ArrayBuffer}

import spark.ui.JettyUtils._
import spark.{ExceptionFailure, SparkContext, Success, Utils}
import spark.scheduler._
import collection.mutable
import spark.scheduler.cluster.SchedulingMode
import spark.scheduler.cluster.SchedulingMode.SchedulingMode

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[spark] class JobProgressUI(val sc: SparkContext) {
  private var _listener: Option[JobProgressListener] = None
  def listener = _listener.get
  val dateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")


  private val indexPage = new IndexPage(this)
  private val stagePage = new StagePage(this)
  private val poolPage = new PoolPage(this)

  var stageTable: StageTable = null
  var stagePoolInfo: StagePoolInfo = null
  var poolTable: PoolTable = null
  var stagePagePoolSource: PoolSource = null

  def start() {
    sc.getSchedulingMode match {
      case SchedulingMode.FIFO =>
        _listener = Some(new JobProgressListener(sc))
        stagePoolInfo = new FIFOStagePoolInfo()
        stagePagePoolSource = new FIFOSource()
      case SchedulingMode.FAIR =>
        _listener = Some(new FairJobProgressListener(sc))
        stagePoolInfo = new FairStagePoolInfo(listener)
        stagePagePoolSource = new FairSource(sc)
    }

    sc.addSparkListener(listener)
    stageTable = new StageTable(dateFmt, formatDuration, listener)
    poolTable = new PoolTable(listener)
  }

  def formatDuration(ms: Long) = Utils.msDurationToString(ms)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage.render(request)),
    ("/stages/pool", (request: HttpServletRequest) => poolPage.render(request)),
    ("/stages", (request: HttpServletRequest) => indexPage.render(request))
  )
}
