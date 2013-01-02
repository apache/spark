package spark.streaming

import java.util.concurrent.atomic.AtomicLong

private[streaming]
class Job(val time: Time, func: () => _) {
  val id = Job.getNewId()
  def run(): Long = {
    val startTime = System.currentTimeMillis 
    func() 
    val stopTime = System.currentTimeMillis
    (stopTime - startTime)
  }

  override def toString = "streaming job " + id + " @ " + time 
}

private[streaming]
object Job {
  val id = new AtomicLong(0)

  def getNewId() = id.getAndIncrement()
}

