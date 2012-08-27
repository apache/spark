package spark.streaming

import spark.streaming.SparkStreamContext._

import spark.RDD
import spark.UnionRDD
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

class WindowedDStream[T: ClassManifest](
    parent: DStream[T],
    _windowTime: Time,
    _slideTime: Time) 
  extends DStream[T](parent.ssc) {

  if (!_windowTime.isMultipleOf(parent.slideTime))
    throw new Exception("The window duration of WindowedDStream (" + _slideTime + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideTime + ")")

  if (!_slideTime.isMultipleOf(parent.slideTime))
    throw new Exception("The slide duration of WindowedDStream (" + _slideTime + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideTime + ")")

  val allowPartialWindows = true
  
  override def dependencies = List(parent)

  def windowTime: Time =  _windowTime

  override def slideTime: Time = _slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    val parentRDDs = new ArrayBuffer[RDD[T]]()
    val windowEndTime = validTime
    val windowStartTime = if (allowPartialWindows && windowEndTime - windowTime < parent.zeroTime) {
          parent.zeroTime
        } else { 
          windowEndTime - windowTime
        }
  
    logInfo("Window = " + windowStartTime  + " - " + windowEndTime)
    logInfo("Parent.zeroTime = " + parent.zeroTime)
    
    if (windowStartTime >= parent.zeroTime) {
      // Walk back through time, from the 'windowEndTime' to 'windowStartTime'
      // and get all parent RDDs from the parent DStream
      var t = windowEndTime
      while (t > windowStartTime) {
        parent.getOrCompute(t) match {
          case Some(rdd) => parentRDDs += rdd
          case None => throw new Exception("Could not generate parent RDD for time " + t)
        }
        t -= parent.slideTime
      }
    }

    // Do a union of all parent RDDs to generate the new RDD
    if (parentRDDs.size > 0) {
      Some(new UnionRDD(ssc.sc, parentRDDs))
    } else {
      None
    }
  }
}



