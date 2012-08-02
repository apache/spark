package spark.streaming

import spark.Logging 
import spark.SparkEnv
import java.util.concurrent.Executors


class JobManager(ssc: SparkStreamContext, numThreads: Int = 1) extends Logging {
  
  class JobHandler(ssc: SparkStreamContext, job: Job) extends Runnable {
    def run() {
      SparkEnv.set(ssc.env)
      try {
        val timeTaken = job.run()
        logInfo(
          "Running " + job + " took " + timeTaken + " ms, " +
          "total delay was " + (System.currentTimeMillis - job.time) + " ms"
        )
      } catch {
        case e: Exception =>
          logError("Running " + job + " failed", e)
      }
    }
  }

  initLogging()

  val jobExecutor = Executors.newFixedThreadPool(numThreads) 
  
  def runJob(job: Job) {
    jobExecutor.execute(new JobHandler(ssc, job))
    logInfo("Added " + job + " to queue")
  }
}
