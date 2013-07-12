package spark.scheduler

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import scala.collection.mutable
import spark._
import spark.SparkContext._


class JobLoggerSuite extends FunSuite with LocalSparkContext with ShouldMatchers {

  test("inner method") {
    sc = new SparkContext("local", "joblogger")
    val joblogger = new JobLogger {
      def createLogWriterTest(jobID: Int) = createLogWriter(jobID)
      def closeLogWriterTest(jobID: Int) = closeLogWriter(jobID)
      def getRddNameTest(rdd: RDD[_]) = getRddName(rdd)
      def buildJobDepTest(jobID: Int, stage: Stage) = buildJobDep(jobID, stage) 
    }
    type MyRDD = RDD[(Int, Int)]
    def makeRdd(
        numPartitions: Int,
        dependencies: List[Dependency[_]]
      ): MyRDD = {
      val maxPartition = numPartitions - 1
      return new MyRDD(sc, dependencies) {
        override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
          throw new RuntimeException("should not be reached")
        override def getPartitions = (0 to maxPartition).map(i => new Partition {
          override def index = i
        }).toArray
      }
    }
    val jobID = 5
    val parentRdd = makeRdd(4, Nil)
    val shuffleDep = new ShuffleDependency(parentRdd, null)
    val rootRdd = makeRdd(4, List(shuffleDep))
    val shuffleMapStage = new Stage(1, parentRdd, Some(shuffleDep), Nil, jobID) 
    val rootStage = new Stage(0, rootRdd, None, List(shuffleMapStage), jobID)
    
    joblogger.onStageSubmitted(SparkListenerStageSubmitted(rootStage, 4, null))
    joblogger.getRddNameTest(parentRdd) should be (parentRdd.getClass.getName)
    parentRdd.setName("MyRDD")
    joblogger.getRddNameTest(parentRdd) should be ("MyRDD")
    joblogger.createLogWriterTest(jobID)
    joblogger.getJobIDtoPrintWriter.size should be (1)
    joblogger.buildJobDepTest(jobID, rootStage)
    joblogger.getJobIDToStages.get(jobID).get.size should be (2)
    joblogger.getStageIDToJobID.get(0) should be (Some(jobID))
    joblogger.getStageIDToJobID.get(1) should be (Some(jobID))
    joblogger.closeLogWriterTest(jobID)
    joblogger.getStageIDToJobID.size should be (0)
    joblogger.getJobIDToStages.size should be (0)
    joblogger.getJobIDtoPrintWriter.size should be (0)
  }
  
  test("inner variables") {
    sc = new SparkContext("local[4]", "joblogger")
    val joblogger = new JobLogger {
      override protected def closeLogWriter(jobID: Int) = 
        getJobIDtoPrintWriter.get(jobID).foreach { fileWriter => 
          fileWriter.close()
        }
    }
    sc.addSparkListener(joblogger)
    val rdd = sc.parallelize(1 to 1e2.toInt, 4).map{ i => (i % 12, 2 * i) }
    rdd.reduceByKey(_+_).collect()
    
    joblogger.getLogDir should be ("/tmp/spark")
    joblogger.getJobIDtoPrintWriter.size should be (1)
    joblogger.getStageIDToJobID.size should be (2)
    joblogger.getStageIDToJobID.get(0) should be (Some(0))
    joblogger.getStageIDToJobID.get(1) should be (Some(0))
    joblogger.getJobIDToStages.size should be (1)
  }
  
  
  test("interface functions") {
    sc = new SparkContext("local[4]", "joblogger")
    val joblogger = new JobLogger {
      var onTaskEndCount = 0
      var onJobEndCount = 0 
      var onJobStartCount = 0
      var onStageCompletedCount = 0
      var onStageSubmittedCount = 0
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd)  = onTaskEndCount += 1
      override def onJobEnd(jobEnd: SparkListenerJobEnd) = onJobEndCount += 1
      override def onJobStart(jobStart: SparkListenerJobStart) = onJobStartCount += 1
      override def onStageCompleted(stageCompleted: StageCompleted) = onStageCompletedCount += 1
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = onStageSubmittedCount += 1
    }
    sc.addSparkListener(joblogger)
    val rdd = sc.parallelize(1 to 1e2.toInt, 4).map{ i => (i % 12, 2 * i) }
    rdd.reduceByKey(_+_).collect()
    
    joblogger.onJobStartCount should be (1)
    joblogger.onJobEndCount should be (1)
    joblogger.onTaskEndCount should be (8)
    joblogger.onStageSubmittedCount should be (2)
    joblogger.onStageCompletedCount should be (2)
  }
}
