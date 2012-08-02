package spark.streaming.util

class RecurringTimer(period: Long, callback: (Long) => Unit) {
  
  val minPollTime = 25L
  
  val pollTime = {
    if (period / 10.0 > minPollTime) {
      (period / 10.0).toLong
    } else {
      minPollTime
    }  
  }
  
  val thread = new Thread() {
    override def run() { loop }    
  }
  
  var nextTime = 0L   
  
  def start(): Long = {
    nextTime = (math.floor(System.currentTimeMillis() / period) + 1).toLong * period
    thread.start() 
    nextTime
  }
  
  def stop() { 
    thread.interrupt() 
  }
  
  def loop() {
    try {
      while (true) {
        val beforeSleepTime = System.currentTimeMillis()
        while (beforeSleepTime >= nextTime) {
          callback(nextTime)          
          nextTime += period
        }
        val sleepTime = if (nextTime - beforeSleepTime < 2 * pollTime) {
          nextTime - beforeSleepTime
        } else {
          pollTime
        }
        Thread.sleep(sleepTime)
        val afterSleepTime = System.currentTimeMillis()
      }
    } catch {
      case e: InterruptedException =>
    }
  }
}

