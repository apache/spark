package spark.streaming

import java.util.concurrent.atomic.AtomicLong

class Job(val time: Time, func: () => _) {
  val id = Job.getNewId()
  def run(): Long = {
    val startTime = System.currentTimeMillis 
    func() 
    val stopTime = System.currentTimeMillis
    (startTime - stopTime)
  }

  override def toString = "streaming job " + id + " @ " + time 
}

object Job {
  val id = new AtomicLong(0)

  def getNewId() = id.getAndIncrement()
}

