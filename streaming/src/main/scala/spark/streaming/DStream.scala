package spark.streaming

import spark.streaming.StreamingContext._

import spark.RDD
import spark.BlockRDD
import spark.UnionRDD
import spark.Logging
import spark.SparkContext
import spark.SparkContext._
import spark.storage.StorageLevel
    
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.util.concurrent.ArrayBlockingQueue 

abstract class DStream[T: ClassManifest] (@transient val ssc: StreamingContext)
extends Logging with Serializable {

  initLogging()

  /** 
   * ----------------------------------------------
   * Methods that must be implemented by subclasses
   * ---------------------------------------------- 
   */

  // Time by which the window slides in this DStream
  def slideTime: Time 

  // List of parent DStreams on which this DStream depends on
  def dependencies: List[DStream[_]]

  // Key method that computes RDD for a valid time 
  def compute (validTime: Time): Option[RDD[T]]

  /**
   * --------------------------------------- 
   * Other general fields and methods of DStream
   * --------------------------------------- 
   */

  // Variable to store the RDDs generated earlier in time
  @transient private val generatedRDDs = new HashMap[Time, RDD[T]] ()
  
  // Variable to be set to the first time seen by the DStream (effective time zero)
  private[streaming] var zeroTime: Time = null

  // Variable to specify storage level
  private var storageLevel: StorageLevel = StorageLevel.NONE

  // Checkpoint level and checkpoint interval
  private var checkpointLevel: StorageLevel = StorageLevel.NONE  // NONE means don't checkpoint
  private var checkpointInterval: Time = null 

  // Change this RDD's storage level
  def persist(
      storageLevel: StorageLevel,
      checkpointLevel: StorageLevel, 
      checkpointInterval: Time): DStream[T] = {
    if (this.storageLevel != StorageLevel.NONE && this.storageLevel != storageLevel) {
      // TODO: not sure this is necessary for DStreams
      throw new UnsupportedOperationException(
        "Cannot change storage level of an DStream after it was already assigned a level")
    }
    this.storageLevel = storageLevel
    this.checkpointLevel = checkpointLevel
    this.checkpointInterval = checkpointInterval
    this
  }

  // Set caching level for the RDDs created by this DStream
  def persist(newLevel: StorageLevel): DStream[T] = persist(newLevel, StorageLevel.NONE, null)

  def persist(): DStream[T] = persist(StorageLevel.MEMORY_ONLY_DESER)
  
  // Turn on the default caching level for this RDD
  def cache(): DStream[T] = persist()

  def isInitialized = (zeroTime != null)

  /**
   * This method initializes the DStream by setting the "zero" time, based on which
   * the validity of future times is calculated. This method also recursively initializes
   * its parent DStreams.
   */
  def initialize(time: Time) {
    if (zeroTime == null) {
      zeroTime = time
    }
    logInfo(this + " initialized")
    dependencies.foreach(_.initialize(zeroTime))
  }

  /** This method checks whether the 'time' is valid wrt slideTime for generating RDD */
  private def isTimeValid (time: Time): Boolean = {
    if (!isInitialized) 
      throw new Exception (this.toString + " has not been initialized")
    if ((time - zeroTime).isMultipleOf(slideTime)) { 
      true
    } else {
      false
    }
  }

  /**
   * This method either retrieves a precomputed RDD of this DStream,
   * or computes the RDD (if the time is valid) 
   */  
  def getOrCompute(time: Time): Option[RDD[T]] = {
    // If this DStream was not initialized (i.e., zeroTime not set), then do it
    // If RDD was already generated, then retrieve it from HashMap
    generatedRDDs.get(time) match {
      
      // If an RDD was already generated and is being reused, then 
      // probably all RDDs in this DStream will be reused and hence should be cached
      case Some(oldRDD) => Some(oldRDD)
      
      // if RDD was not generated, and if the time is valid 
      // (based on sliding time of this DStream), then generate the RDD
      case None =>
        if (isTimeValid(time)) {
          compute(time) match {
            case Some(newRDD) =>
              if (checkpointInterval != null && (time - zeroTime).isMultipleOf(checkpointInterval)) { 
                newRDD.persist(checkpointLevel)
                logInfo("Persisting " + newRDD + " to " + checkpointLevel + " at time " + time)
              } else if (storageLevel != StorageLevel.NONE) {
                newRDD.persist(storageLevel)
                logInfo("Persisting " + newRDD + " to " + storageLevel + " at time " + time)
              }
              generatedRDDs.put(time.copy(), newRDD)
              Some(newRDD)
            case None => 
              None
          }
        } else {
          None
        }
    }
  }

  /**
   * This method generates a SparkStreaming job for the given time
   * and may require to be overriden by subclasses
   */
  def generateJob(time: Time): Option[Job] = {
    getOrCompute(time) match {
      case Some(rdd) => {
        val jobFunc = () => {
          val emptyFunc = { (iterator: Iterator[T]) => {} } 
          ssc.sc.runJob(rdd, emptyFunc)
        }
        Some(new Job(time, jobFunc))
      }
      case None => None
    }
  }

  /** 
   * --------------
   * DStream operations
   * -------------- 
   */
  
  def map[U: ClassManifest](mapFunc: T => U) = new MappedDStream(this, ssc.sc.clean(mapFunc))

  def flatMap[U: ClassManifest](flatMapFunc: T => Traversable[U]) = 
    new FlatMappedDStream(this, ssc.sc.clean(flatMapFunc))

  def filter(filterFunc: T => Boolean) = new FilteredDStream(this, filterFunc)

  def glom() = new GlommedDStream(this)

  def mapPartitions[U: ClassManifest](mapPartFunc: Iterator[T] => Iterator[U]) = 
    new MapPartitionedDStream(this, ssc.sc.clean(mapPartFunc))

  def reduce(reduceFunc: (T, T) => T) = this.map(x => (1, x)).reduceByKey(reduceFunc, 1).map(_._2)

  def count() = this.map(_ => 1).reduce(_ + _)
  
  def collect() = this.map(x => (1, x)).groupByKey(1).map(_._2)

  def foreach(foreachFunc: T => Unit) = { 
    val newStream = new PerElementForEachDStream(this, ssc.sc.clean(foreachFunc))
    ssc.registerOutputStream(newStream)
    newStream
  }

  def foreachRDD(foreachFunc: RDD[T] => Unit) = {
    val newStream = new PerRDDForEachDStream(this, ssc.sc.clean(foreachFunc))
    ssc.registerOutputStream(newStream)
    newStream
  }

  private[streaming] def toQueue = {
    val queue = new ArrayBlockingQueue[RDD[T]](10000)
    this.foreachRDD(rdd => {
      queue.add(rdd)
    })
    queue
  }
  
  def print() = {
    def foreachFunc = (rdd: RDD[T], time: Time) => {
      val first11 = rdd.take(11)
      println ("-------------------------------------------")
      println ("Time: " + time)
      println ("-------------------------------------------")
      first11.take(10).foreach(println)
      if (first11.size > 10) println("...")
      println()
    }
    val newStream = new PerRDDForEachDStream(this, ssc.sc.clean(foreachFunc))
    ssc.registerOutputStream(newStream)
    newStream
  }

  def window(windowTime: Time, slideTime: Time) = new WindowedDStream(this, windowTime, slideTime)

  def batch(batchTime: Time) = window(batchTime, batchTime)

  def reduceByWindow(reduceFunc: (T, T) => T, windowTime: Time, slideTime: Time) = 
    this.window(windowTime, slideTime).reduce(reduceFunc)

  def reduceByWindow(
    reduceFunc: (T, T) => T, 
    invReduceFunc: (T, T) => T, 
    windowTime: Time, 
    slideTime: Time) = { 
      this.map(x => (1, x))
          .reduceByKeyAndWindow(reduceFunc, invReduceFunc, windowTime, slideTime, 1)
          .map(_._2)
  }

  def countByWindow(windowTime: Time, slideTime: Time) = {
    def add(v1: Int, v2: Int) = (v1 + v2) 
    def subtract(v1: Int, v2: Int) = (v1 - v2) 
    this.map(_ => 1).reduceByWindow(add _, subtract _, windowTime, slideTime)
  }

  def union(that: DStream[T]) = new UnifiedDStream(Array(this, that))

  def register() {
    ssc.registerOutputStream(this)
  }
}


abstract class InputDStream[T: ClassManifest] (
    @transient ssc: StreamingContext)
extends DStream[T](ssc) {
  
  override def dependencies = List()

  override def slideTime = ssc.batchDuration 
  
  def start()  
  
  def stop()
}


/**
 * TODO
 */

class MappedDStream[T: ClassManifest, U: ClassManifest] (
    parent: DStream[T],
    mapFunc: T => U)
extends DStream[U](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.map[U](mapFunc))
  }
}


/**
 * TODO
 */

class FlatMappedDStream[T: ClassManifest, U: ClassManifest](
    parent: DStream[T],
    flatMapFunc: T => Traversable[U])
extends DStream[U](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.flatMap(flatMapFunc))
  }
}


/**
 * TODO
 */

class FilteredDStream[T: ClassManifest](parent: DStream[T], filterFunc: T => Boolean)
extends DStream[T](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    parent.getOrCompute(validTime).map(_.filter(filterFunc))
  }
}


/**
 * TODO
 */

class MapPartitionedDStream[T: ClassManifest, U: ClassManifest](
    parent: DStream[T],
    mapPartFunc: Iterator[T] => Iterator[U])
extends DStream[U](parent.ssc) {

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.mapPartitions[U](mapPartFunc))
  }
}


/**
 * TODO
 */

class GlommedDStream[T: ClassManifest](parent: DStream[T]) extends DStream[Array[T]](parent.ssc) {

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[Array[T]]] = {
    parent.getOrCompute(validTime).map(_.glom())
  }
}


/**
 * TODO
 */

class ShuffledDStream[K: ClassManifest, V: ClassManifest, C: ClassManifest](
    parent: DStream[(K,V)],
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    numPartitions: Int)
  extends DStream [(K,C)] (parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime
 
  override def compute(validTime: Time): Option[RDD[(K,C)]] = {
    parent.getOrCompute(validTime) match {
      case Some(rdd) => 
        val newrdd = {
          if (numPartitions > 0) {
            rdd.combineByKey[C](createCombiner, mergeValue, mergeCombiner, numPartitions) 
          } else {
            rdd.combineByKey[C](createCombiner, mergeValue, mergeCombiner)
          }
        }
        Some(newrdd)
      case None => None
    }
  }
}


/**
 * TODO
 */

class UnifiedDStream[T: ClassManifest](parents: Array[DStream[T]])
extends DStream[T](parents(0).ssc) {

  if (parents.length == 0) {
    throw new IllegalArgumentException("Empty array of parents")
  }

  if (parents.map(_.ssc).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different StreamingContexts")
  }
  
  if (parents.map(_.slideTime).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different slide times")
  }

  override def dependencies = parents.toList

  override def slideTime: Time = parents(0).slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    val rdds = new ArrayBuffer[RDD[T]]()
    parents.map(_.getOrCompute(validTime)).foreach(_ match {
      case Some(rdd) => rdds += rdd
      case None => throw new Exception("Could not generate RDD from a parent for unifying at time " + validTime) 
    })
    if (rdds.size > 0) {
      Some(new UnionRDD(ssc.sc, rdds))
    } else {
      None
    }
  }
}


/**
 * TODO
 */

class PerElementForEachDStream[T: ClassManifest] (
    parent: DStream[T],
    foreachFunc: T => Unit) 
extends DStream[Unit](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[Unit]] = None 

  override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => {
          val sparkJobFunc = { 
            (iterator: Iterator[T]) => iterator.foreach(foreachFunc) 
          } 
          ssc.sc.runJob(rdd, sparkJobFunc)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }
}


/**
 * TODO
 */

class PerRDDForEachDStream[T: ClassManifest] (
    parent: DStream[T],
    foreachFunc: (RDD[T], Time) => Unit)
extends DStream[Unit](parent.ssc) {
  
  def this(parent: DStream[T], altForeachFunc: (RDD[T]) => Unit) =
    this(parent, (rdd: RDD[T], time: Time) => altForeachFunc(rdd))

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[Unit]] = None 

  override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }
}
