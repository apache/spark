package spark.streaming

class Job(val time: Time, func: () => _) {
  val id = Job.getNewId()
  
  def run() {
    func()
  }

  override def toString = "SparkStream Job " + id + ":" + time 
}

object Job {
  var lastId = 1

  def getNewId() = synchronized {
    lastId += 1
    lastId
  }
}

