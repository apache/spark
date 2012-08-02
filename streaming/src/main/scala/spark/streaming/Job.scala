package spark.streaming

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
  var lastId = 1

  def getNewId() = synchronized {
    lastId += 1
    lastId
  }
}

