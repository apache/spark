package spark.stream

import spark.stream.SparkStreamContext._

import spark.RDD
import spark.UnionRDD
import spark.CoGroupedRDD
import spark.HashPartitioner
import spark.SparkContext._
import spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class ReducedWindowedRDS[K: ClassManifest, V: ClassManifest](
    parent: RDS[(K, V)],
    reduceFunc: (V, V) => V,
    invReduceFunc: (V, V) => V, 
    _windowTime: Time,
    _slideTime: Time,
    numPartitions: Int)
extends RDS[(K,V)](parent.ssc) {

  if (!_windowTime.isMultipleOf(parent.slideTime))
    throw new Exception("The window duration of ReducedWindowedRDS (" + _slideTime + ") " + 
    "must be multiple of the slide duration of parent RDS (" + parent.slideTime + ")")

  if (!_slideTime.isMultipleOf(parent.slideTime))
    throw new Exception("The slide duration of ReducedWindowedRDS (" + _slideTime + ") " + 
    "must be multiple of the slide duration of parent RDS (" + parent.slideTime + ")")

  val reducedRDS = parent.reduceByKey(reduceFunc, numPartitions)
  val allowPartialWindows = true
  //reducedRDS.persist(StorageLevel.MEMORY_ONLY_DESER_2)

  override def dependencies = List(reducedRDS)

  def windowTime: Time =  _windowTime

  override def slideTime: Time = _slideTime

  override def persist(
      storageLevel: StorageLevel, 
      checkpointLevel: StorageLevel, 
      checkpointInterval: Time): RDS[(K,V)] = {
    super.persist(storageLevel, checkpointLevel, checkpointInterval)
    reducedRDS.persist(storageLevel, checkpointLevel, checkpointInterval)
  }
  
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    

    // Notation: 
    //  _____________________________
    // |  previous window   _________|___________________   
    // |___________________|       current window        |  --------------> Time  
    //                     |_____________________________|
    // 
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //   old time steps                new time steps   
    //
    def getAdjustedWindow(endTime: Time, windowTime: Time): Interval = {
      val beginTime = 
        if (allowPartialWindows && endTime - windowTime < parent.zeroTime) {
          parent.zeroTime
        } else { 
          endTime - windowTime 
        }
      Interval(beginTime, endTime)
    }
    
    val currentTime = validTime.copy
    val currentWindow = getAdjustedWindow(currentTime, windowTime)
    val previousWindow = getAdjustedWindow(currentTime - slideTime, windowTime)
    
    logInfo("Current window = " + currentWindow)
    logInfo("Previous window = " + previousWindow)
    logInfo("Parent.zeroTime = " + parent.zeroTime)

    if (allowPartialWindows) {
      if (currentTime - slideTime == parent.zeroTime) {
        reducedRDS.getOrCompute(currentTime) match {
          case Some(rdd) => return Some(rdd)
          case None => throw new Exception("Could not get first reduced RDD for time " + currentTime)
        }
      } 
    } else {
      if (previousWindow.beginTime < parent.zeroTime) {
        if (currentWindow.beginTime < parent.zeroTime) {
          return None
        } else {
          // If this is the first feasible window, then generate reduced value in the naive manner
          val reducedRDDs = new ArrayBuffer[RDD[(K, V)]]()
          var t = currentWindow.endTime 
          while (t > currentWindow.beginTime) {
            reducedRDS.getOrCompute(t) match {
              case Some(rdd) => reducedRDDs += rdd
              case None => throw new Exception("Could not get reduced RDD for time " + t)
            }
            t -= reducedRDS.slideTime
          }
          if (reducedRDDs.size == 0) {
            throw new Exception("Could not generate the first RDD for time " + validTime) 
          }
          return Some(new UnionRDD(ssc.sc, reducedRDDs).reduceByKey(reduceFunc, numPartitions))
        }
      }
    }
    
    // Get the RDD of the reduced value of the previous window
    val previousWindowRDD = getOrCompute(previousWindow.endTime) match {
      case Some(rdd) => rdd.asInstanceOf[RDD[(_, _)]]
      case None => throw new Exception("Could not get previous RDD for time " + previousWindow.endTime) 
    }

    val oldRDDs = new ArrayBuffer[RDD[(_, _)]]()
    val newRDDs = new ArrayBuffer[RDD[(_, _)]]()
    
    // Get the RDDs of the reduced values in "old time steps"
    var t = currentWindow.beginTime 
    while (t > previousWindow.beginTime) {
      reducedRDS.getOrCompute(t) match {
        case Some(rdd) => oldRDDs += rdd.asInstanceOf[RDD[(_, _)]]
        case None => throw new Exception("Could not get old reduced RDD for time " + t)
      }
      t -= reducedRDS.slideTime
    }

    // Get the RDDs of the reduced values in "new time steps"
    t = currentWindow.endTime 
    while (t > previousWindow.endTime) {
      reducedRDS.getOrCompute(t) match {
        case Some(rdd) => newRDDs += rdd.asInstanceOf[RDD[(_, _)]]
        case None => throw new Exception("Could not get new reduced RDD for time " + t)
      }
      t -= reducedRDS.slideTime
    }
    
    val partitioner = new HashPartitioner(numPartitions)
    val allRDDs = new ArrayBuffer[RDD[(_, _)]]()
    allRDDs += previousWindowRDD
    allRDDs ++= oldRDDs
    allRDDs ++= newRDDs
   

    val numOldRDDs = oldRDDs.size
    val numNewRDDs = newRDDs.size
    logInfo("Generated numOldRDDs = " + numOldRDDs + ", numNewRDDs = " + numNewRDDs)
    logInfo("Generating CoGroupedRDD with " + allRDDs.size + " RDDs")
    val newRDD = new CoGroupedRDD[K](allRDDs.toSeq, partitioner).asInstanceOf[RDD[(K,Seq[Seq[V]])]].map(x => {
      val (key, value) = x 
      logDebug("value.size = " + value.size + ", numOldRDDs = " + numOldRDDs + ", numNewRDDs = " + numNewRDDs)
      if (value.size != 1 + numOldRDDs + numNewRDDs) {
        throw new Exception("Number of groups not odd!")
      }

      // old values = reduced values of the "old time steps" that are eliminated from current window
      // new values = reduced values of the "new time steps" that are introduced to the current window
      // previous value = reduced value of the previous window 

      /*val numOldValues = (value.size - 1) / 2*/
      // Getting reduced values "old time steps"
      val oldValues = 
        (0 until numOldRDDs).map(i => value(1 + i)).filter(_.size > 0).map(x => x(0))
      // Getting reduced values "new time steps"
      val newValues = 
        (0 until numNewRDDs).map(i => value(1 + numOldRDDs + i)).filter(_.size > 0).map(x => x(0))
       
      // If reduced value for the key does not exist in previous window, it should not exist in "old time steps"
      if (value(0).size == 0 && oldValues.size != 0) {
        throw new Exception("Unexpected: Key exists in old reduced values but not in previous reduced values")
      }

      // For the key, at least one of "old time steps", "new time steps" and previous window should have reduced values
      if (value(0).size == 0 && oldValues.size == 0 && newValues.size == 0) {
        throw new Exception("Unexpected: Key does not exist in any of old, new, or previour reduced values")
      }

      // Logic to generate the final reduced value for current window:
      //
      // If previous window did not have reduced value for the key
      // Then, return reduced value of "new time steps" as the final value
      // Else, reduced value exists in previous window
      //     If "old" time steps did not have reduced value for the key
      //     Then, reduce previous window's reduced value with that of "new time steps" for final value
      //     Else, reduced values exists in "old time steps"
      //         If "new values" did not have reduced value for the key
      //         Then, inverse-reduce "old values" from previous window's reduced value for final value
      //         Else, all 3 values exist, combine all of them together 
      //
      logDebug("# old values = " + oldValues.size + ", # new values = " + newValues)
      val finalValue = {
        if (value(0).size == 0) {
          newValues.reduce(reduceFunc)
        } else {
          val prevValue = value(0)(0)
          logDebug("prev value = " + prevValue)
          if (oldValues.size == 0) {
            // assuming newValue.size > 0 (all 3 cannot be zero, as checked earlier)  
            val temp = newValues.reduce(reduceFunc)
            reduceFunc(prevValue, temp)
          } else if (newValues.size == 0) {
            invReduceFunc(prevValue, oldValues.reduce(reduceFunc))
          } else {
            val tempValue = invReduceFunc(prevValue, oldValues.reduce(reduceFunc))
            reduceFunc(tempValue, newValues.reduce(reduceFunc))
          }
        }
      }
      (key, finalValue)
    })
    //newRDD.persist(StorageLevel.MEMORY_ONLY_DESER_2)
    Some(newRDD)
  }
}


