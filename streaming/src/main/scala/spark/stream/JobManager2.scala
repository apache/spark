package spark.stream

import spark.{Logging, SparkEnv}
import java.util.concurrent.Executors


class JobManager2(ssc: SparkStreamContext, numThreads: Int = 1) extends Logging {
  
  class JobHandler(ssc: SparkStreamContext, job: Job) extends Runnable {
    def run() {
      SparkEnv.set(ssc.env)
      try {
        logInfo("Starting "  + job)
        job.run()
        logInfo("Finished " + job)
        if (job.time.isInstanceOf[LongTime]) {
          val longTime = job.time.asInstanceOf[LongTime]
          logInfo("Total notification + skew + processing delay for " + longTime + " is " +
            (System.currentTimeMillis - longTime.milliseconds) / 1000.0  + " s")
          if (System.getProperty("spark.stream.distributed", "false") == "true") {
            TestInputBlockTracker.setEndTime(job.time)
          }
        }
      } catch {
        case e: Exception => logError("SparkStream job failed", e)
      }
    }
  }

  initLogging()

  val jobExecutor = Executors.newFixedThreadPool(numThreads) 
  
  def runJob(job: Job) {
    jobExecutor.execute(new JobHandler(ssc, job))
  }
}
