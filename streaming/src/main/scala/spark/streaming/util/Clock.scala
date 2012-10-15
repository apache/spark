package spark.streaming.util

import spark.streaming._

trait Clock {  
  def currentTime(): Long 
  def waitTillTime(targetTime: Long): Long
}


class SystemClock() extends Clock {
  
  val minPollTime = 25L
  
  def currentTime(): Long = {
    System.currentTimeMillis()
  } 
  
  def waitTillTime(targetTime: Long): Long = {
    var currentTime = 0L
    currentTime = System.currentTimeMillis()
    
    var waitTime = targetTime - currentTime
    if (waitTime <= 0) {
      return currentTime
    }
    
    val pollTime = {
      if (waitTime / 10.0 > minPollTime) {
        (waitTime / 10.0).toLong
      } else {
        minPollTime 
      }  
    }
    
    
    while (true) {
      currentTime = System.currentTimeMillis()
      waitTime = targetTime - currentTime
      
      if (waitTime <= 0) {
        
        return currentTime
      }
      val sleepTime = 
        if (waitTime < pollTime) {
          waitTime
        } else {
          pollTime
        }
      Thread.sleep(sleepTime)
    }
    return -1
  }
}

class ManualClock() extends Clock {
  
  var time = 0L

  def currentTime() = time

  def setTime(timeToSet: Long) = {
    this.synchronized {
      time = timeToSet
      this.notifyAll()
    }
  }

  def addToTime(timeToAdd: Long) = {
    this.synchronized {
      time += timeToAdd
      this.notifyAll()
    } 
  }
  def waitTillTime(targetTime: Long): Long = {
    this.synchronized {
      while (time < targetTime) {
        this.wait(100)
      }      
    }
    return currentTime()
  }
}
