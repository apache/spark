package spark.streaming

import spark.streaming.StreamingContext._

import spark.RDD
import spark.rdd.UnionRDD
import spark.rdd.CoGroupedRDD
import spark.Partitioner
import spark.SparkContext._
import spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import collection.SeqProxy

class ReducedWindowedDStream[K: ClassManifest, V: ClassManifest](
    parent: DStream[(K, V)],
    reduceFunc: (V, V) => V,
    invReduceFunc: (V, V) => V, 
    _windowTime: Time,
    _slideTime: Time,
    partitioner: Partitioner
  ) extends DStream[(K,V)](parent.ssc) {

  if (!_windowTime.isMultipleOf(parent.slideTime))
    throw new Exception("The window duration of ReducedWindowedDStream (" + _slideTime + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideTime + ")")

  if (!_slideTime.isMultipleOf(parent.slideTime))
    throw new Exception("The slide duration of ReducedWindowedDStream (" + _slideTime + ") " +
    "must be multiple of the slide duration of parent DStream (" + parent.slideTime + ")")

  @transient val reducedStream = parent.reduceByKey(reduceFunc, partitioner)

  def windowTime: Time =  _windowTime

  override def dependencies = List(reducedStream)

  override def slideTime: Time = _slideTime

  //TODO: This is wrong. This should depend on the checkpointInterval
  override def parentRememberDuration: Time = rememberDuration + windowTime

  override def persist(
      storageLevel: StorageLevel, 
      checkpointLevel: StorageLevel, 
      checkpointInterval: Time): DStream[(K,V)] = {
    super.persist(storageLevel, checkpointLevel, checkpointInterval)
    reducedStream.persist(storageLevel, checkpointLevel, checkpointInterval)
    this
  }

  protected[streaming] override def setRememberDuration(time: Time) {
    if (rememberDuration == null || rememberDuration < time) {
      rememberDuration = time
      dependencies.foreach(_.setRememberDuration(rememberDuration + windowTime))
    }
  }

  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    val reduceF = reduceFunc
    val invReduceF = invReduceFunc

    val currentTime = validTime
    val currentWindow = Interval(currentTime - windowTime + parent.slideTime, currentTime)
    val previousWindow = currentWindow - slideTime

    logDebug("Window time = " + windowTime)
    logDebug("Slide time = " + slideTime)
    logDebug("ZeroTime = " + zeroTime)
    logDebug("Current window = " + currentWindow)
    logDebug("Previous window = " + previousWindow)

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //

    // Get the RDDs of the reduced values in "old time steps"
    val oldRDDs = reducedStream.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideTime)
    logDebug("# old RDDs = " + oldRDDs.size)

    // Get the RDDs of the reduced values in "new time steps"
    val newRDDs = reducedStream.slice(previousWindow.endTime + parent.slideTime, currentWindow.endTime)
    logDebug("# new RDDs = " + newRDDs.size)

    // Get the RDD of the reduced value of the previous window
    val previousWindowRDD = getOrCompute(previousWindow.endTime).getOrElse(ssc.sc.makeRDD(Seq[(K,V)]()))

    // Make the list of RDDs that needs to cogrouped together for reducing their reduced values
    val allRDDs = new ArrayBuffer[RDD[(K, V)]]() += previousWindowRDD ++= oldRDDs ++= newRDDs

    // Cogroup the reduced RDDs and merge the reduced values
    val cogroupedRDD = new CoGroupedRDD[K](allRDDs.toSeq.asInstanceOf[Seq[RDD[(_, _)]]], partitioner)
    //val mergeValuesFunc = mergeValues(oldRDDs.size, newRDDs.size) _

    val numOldValues = oldRDDs.size
    val numNewValues = newRDDs.size

    val mergeValues = (seqOfValues: Seq[Seq[V]]) => {
      if (seqOfValues.size != 1 + numOldValues + numNewValues) {
        throw new Exception("Unexpected number of sequences of reduced values")
      }
      // Getting reduced values "old time steps" that will be removed from current window
      val oldValues = (1 to numOldValues).map(i => seqOfValues(i)).filter(!_.isEmpty).map(_.head)
      // Getting reduced values "new time steps"
      val newValues = (1 to numNewValues).map(i => seqOfValues(numOldValues + i)).filter(!_.isEmpty).map(_.head)
      if (seqOfValues(0).isEmpty) {
        // If previous window's reduce value does not exist, then at least new values should exist
        if (newValues.isEmpty) {
          throw new Exception("Neither previous window has value for key, nor new values found")
        }
        // Reduce the new values
        newValues.reduce(reduceF) // return
      } else {
        // Get the previous window's reduced value
        var tempValue = seqOfValues(0).head
        // If old values exists, then inverse reduce then from previous value
        if (!oldValues.isEmpty) {
          tempValue = invReduceF(tempValue, oldValues.reduce(reduceF))
        }
        // If new values exists, then reduce them with previous value
        if (!newValues.isEmpty) {
          tempValue = reduceF(tempValue, newValues.reduce(reduceF))
        }
        tempValue // return
      }
    }

    val mergedValuesRDD = cogroupedRDD.asInstanceOf[RDD[(K,Seq[Seq[V]])]].mapValues(mergeValues)

    Some(mergedValuesRDD)
  }


}


