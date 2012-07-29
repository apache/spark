package spark.stream

import spark.stream.SparkStreamContext._

import spark.RDD
import spark.UnionRDD
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

class WindowedRDS[T: ClassManifest](
    parent: RDS[T],
    _windowTime: Time,
    _slideTime: Time) 
  extends RDS[T](parent.ssc) {

  if (!_windowTime.isMultipleOf(parent.slideTime))
    throw new Exception("The window duration of WindowedRDS (" + _slideTime + ") " + 
    "must be multiple of the slide duration of parent RDS (" + parent.slideTime + ")")

  if (!_slideTime.isMultipleOf(parent.slideTime))
    throw new Exception("The slide duration of WindowedRDS (" + _slideTime + ") " + 
    "must be multiple of the slide duration of parent RDS (" + parent.slideTime + ")")

  val allowPartialWindows = true
  
  override def dependencies = List(parent)

  def windowTime: Time =  _windowTime

  override def slideTime: Time = _slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    val parentRDDs = new ArrayBuffer[RDD[T]]()
    val windowEndTime = validTime.copy()
    val windowStartTime = if (allowPartialWindows && windowEndTime - windowTime < parent.zeroTime) {
          parent.zeroTime
        } else { 
          windowEndTime - windowTime
        }
  
    logInfo("Window = " + windowStartTime  + " - " + windowEndTime)
    logInfo("Parent.zeroTime = " + parent.zeroTime)
    
    if (windowStartTime >= parent.zeroTime) {
      // Walk back through time, from the 'windowEndTime' to 'windowStartTime'
      // and get all parent RDDs from the parent RDS
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



