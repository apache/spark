package spark.streaming.util

private[streaming]
class RecurringTimer(val clock: Clock, val period: Long, val callback: (Long) => Unit) {
  
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

  def start(startTime: Long): Long = {
    nextTime = startTime
    thread.start()
    nextTime
  }

  def start(): Long = {
    val startTime = (math.floor(clock.currentTime.toDouble / period) + 1).toLong * period
    start(startTime)
  }

  def restart(originalStartTime: Long): Long = {
    val gap = clock.currentTime - originalStartTime
    val newStartTime = (math.floor(gap.toDouble / period).toLong + 1) * period + originalStartTime
    start(newStartTime)
  }
  
  def stop() { 
    thread.interrupt() 
  }
  
  def loop() {
    try {
      while (true) {
        clock.waitTillTime(nextTime)
        callback(nextTime)
        nextTime += period
      }
      
    } catch {
      case e: InterruptedException =>
    }
  }
}

private[streaming]
object RecurringTimer {
  
  def main(args: Array[String]) {
    var lastRecurTime = 0L
    val period = 1000
    
    def onRecur(time: Long) {
      val currentTime = System.currentTimeMillis()
      println("" + currentTime + ": " + (currentTime - lastRecurTime))
      lastRecurTime = currentTime
    }
    val timer = new  RecurringTimer(new SystemClock(), period, onRecur)
    timer.start()
    Thread.sleep(30 * 1000)
    timer.stop()
  }
}

