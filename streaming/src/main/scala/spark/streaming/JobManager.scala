package spark.streaming

import spark.Logging 
import spark.SparkEnv
import java.util.concurrent.Executors


class JobManager(ssc: StreamingContext, numThreads: Int = 1) extends Logging {
  
  class JobHandler(ssc: StreamingContext, job: Job) extends Runnable {
    def run() {
      SparkEnv.set(ssc.env)
      try {
        val timeTaken = job.run()
        logInfo("Total delay: %.5f s for job %s (execution: %.5f s)".format(
          (System.currentTimeMillis() - job.time) / 1000.0, job.id, timeTaken / 1000.0))
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
