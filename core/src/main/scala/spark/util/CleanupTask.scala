package spark.util

import java.util.concurrent.{TimeUnit, ScheduledFuture, Executors}
import java.util.{TimerTask, Timer}
import spark.Logging

class CleanupTask(name: String, cleanupFunc: (Long) => Unit) extends Logging {
  val delayMins = System.getProperty("spark.cleanup.delay", "-100").toInt
  val periodMins = System.getProperty("spark.cleanup.period", (delayMins / 10).toString).toInt
  val timer = new Timer(name + " cleanup timer", true)
  val task = new TimerTask {
    def run() {
      try {
        if (delayMins > 0) {

          cleanupFunc(System.currentTimeMillis() - (delayMins * 60 * 1000))
          logInfo("Ran cleanup task for " + name)
        }
      } catch {
        case e: Exception => logError("Error running cleanup task for " + name, e)
      }
    }
  }
  if (periodMins > 0) {
    timer.schedule(task, periodMins * 60 * 1000, periodMins * 60 * 1000)
  }

  def cancel() {
    timer.cancel()
  }
}
