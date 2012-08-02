package spark.streaming

import spark.streaming.util.Utils

class Job(val time: Time, func: () => _) {
  val id = Job.getNewId()
  def run(): Long = {
    Utils.time { func() }
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

