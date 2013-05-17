package spark.scheduler

import org.scalatest.FunSuite
import spark.{SparkContext, LocalSparkContext}
import scala.collection.mutable
import org.scalatest.matchers.ShouldMatchers
import spark.SparkContext._

/**
 *
 */

class SparkListenerSuite extends FunSuite with LocalSparkContext with ShouldMatchers {

  test("local metrics") {
    sc = new SparkContext("local[4]", "test")
    val listener = new SaveStageInfo
    sc.addSparkListener(listener)
    sc.addSparkListener(new StatsReportListener)
    //just to make sure some of the tasks take a noticeable amount of time
    val w = {i:Int =>
      if (i == 0)
        Thread.sleep(100)
      i
    }

    val d = sc.parallelize(1 to 1e4.toInt, 64).map{i => w(i)}
    d.count
    listener.stageInfos.size should be (1)

    val d2 = d.map{i => w(i) -> i * 2}.setName("shuffle input 1")

    val d3 = d.map{i => w(i) -> (0 to (i % 5))}.setName("shuffle input 2")

    val d4 = d2.cogroup(d3, 64).map{case(k,(v1,v2)) => w(k) -> (v1.size, v2.size)}
    d4.setName("A Cogroup")

    d4.collectAsMap

    listener.stageInfos.size should be (4)
    listener.stageInfos.foreach {stageInfo =>
      //small test, so some tasks might take less than 1 millisecond, but average should be greater than 1 ms
      checkNonZeroAvg(stageInfo.taskInfos.map{_._1.duration}, stageInfo + " duration")
      checkNonZeroAvg(stageInfo.taskInfos.map{_._2.executorRunTime.toLong}, stageInfo + " executorRunTime")
      checkNonZeroAvg(stageInfo.taskInfos.map{_._2.executorDeserializeTime.toLong}, stageInfo + " executorDeserializeTime")
      if (stageInfo.stage.rdd.name == d4.name) {
        checkNonZeroAvg(stageInfo.taskInfos.map{_._2.shuffleReadMetrics.get.fetchWaitTime}, stageInfo + " fetchWaitTime")
      }

        stageInfo.taskInfos.foreach{case (taskInfo, taskMetrics) =>
        taskMetrics.resultSize should be > (0l)
        if (isStage(stageInfo, Set(d2.name, d3.name), Set(d4.name))) {
          taskMetrics.shuffleWriteMetrics should be ('defined)
          taskMetrics.shuffleWriteMetrics.get.shuffleBytesWritten should be > (0l)
        }
        if (stageInfo.stage.rdd.name == d4.name) {
          taskMetrics.shuffleReadMetrics should be ('defined)
          val sm = taskMetrics.shuffleReadMetrics.get
          sm.totalBlocksFetched should be > (0)
          sm.localBlocksFetched should be > (0)
          sm.remoteBlocksFetched should be (0)
          sm.remoteBytesRead should be (0l)
          sm.remoteFetchTime should be (0l)
        }
      }
    }
  }

  def checkNonZeroAvg(m: Traversable[Long], msg: String) {
    assert(m.sum / m.size.toDouble > 0.0, msg)
  }

  def isStage(stageInfo: StageInfo, rddNames: Set[String], excludedNames: Set[String]) = {
    val names = Set(stageInfo.stage.rdd.name) ++ stageInfo.stage.rdd.dependencies.map{_.rdd.name}
    !names.intersect(rddNames).isEmpty && names.intersect(excludedNames).isEmpty
  }

  class SaveStageInfo extends SparkListener {
    val stageInfos = mutable.Buffer[StageInfo]()
    def onStageCompleted(stage: StageCompleted) {
      stageInfos += stage.stageInfo
    }
  }

}
