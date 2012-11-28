package spark.util

import java.util.concurrent.{TimeUnit, ScheduledFuture, Executors}
import java.util.{TimerTask, Timer}
import spark.Logging

class CleanupTask(name: String, cleanupFunc: (Long) => Unit) extends Logging {
  val delaySeconds = (System.getProperty("spark.cleanup.delay", "-100").toDouble * 60).toInt
  val periodSeconds = math.max(10, delaySeconds / 10)
  val timer = new Timer(name + " cleanup timer", true)
  val task = new TimerTask {
    def run() {
      try {
        if (delaySeconds > 0) {
          cleanupFunc(System.currentTimeMillis() - (delaySeconds * 1000))
          logInfo("Ran cleanup task for " + name)
        } 
      } catch {
        case e: Exception => logError("Error running cleanup task for " + name, e)
      }
    }
  }
  if (periodSeconds > 0) {
    logInfo("Starting cleanup task for " + name + " with delay of " + delaySeconds + " seconds and "
      + "period of " + periodSeconds + " secs")
    timer.schedule(task, periodSeconds * 1000, periodSeconds * 1000)
  }

  def cancel() {
    timer.cancel()
  }
}
