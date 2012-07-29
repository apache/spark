package spark.streaming

import spark.streaming.SparkStreamContext._

import spark.RDD
import spark.BlockRDD
import spark.UnionRDD
import spark.Logging
import spark.SparkContext
import spark.SparkContext._
import spark.storage.StorageLevel
    
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.net.InetSocketAddress

abstract class RDS[T: ClassManifest] (@transient val ssc: SparkStreamContext) 
extends Logging with Serializable {

  initLogging()

  /* ---------------------------------------------- */
  /* Methods that must be implemented by subclasses */
  /* ---------------------------------------------- */

  // Time by which the window slides in this RDS
  def slideTime: Time 

  // List of parent RDSs on which this RDS depends on 
  def dependencies: List[RDS[_]]

  // Key method that computes RDD for a valid time 
  def compute (validTime: Time): Option[RDD[T]]

  /* --------------------------------------- */
  /* Other general fields and methods of RDS */
  /* --------------------------------------- */

  // Variable to store the RDDs generated earlier in time
  @transient private val generatedRDDs = new HashMap[Time, RDD[T]] ()
  
  // Variable to be set to the first time seen by the RDS (effective time zero)
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
      checkpointInterval: Time): RDS[T] = {
    if (this.storageLevel != StorageLevel.NONE && this.storageLevel != storageLevel) {
      // TODO: not sure this is necessary for RDSes
      throw new UnsupportedOperationException(
        "Cannot change storage level of an RDS after it was already assigned a level")
    }
    this.storageLevel = storageLevel
    this.checkpointLevel = checkpointLevel
    this.checkpointInterval = checkpointInterval
    this
  }

  def persist(newLevel: StorageLevel): RDS[T] = persist(newLevel, StorageLevel.NONE, null)

  // Turn on the default caching level for this RDD
  def persist(): RDS[T] = persist(StorageLevel.MEMORY_ONLY_DESER)
  
  // Turn on the default caching level for this RDD
  def cache(): RDS[T] = persist()

  def isInitialized = (zeroTime != null)

  // This method initializes the RDS by setting the "zero" time, based on which 
  // the validity of future times is calculated. This method also recursively initializes
  // its parent RDSs.
  def initialize(firstInterval: Interval) {
    if (zeroTime == null) {
      zeroTime = firstInterval.beginTime
    }
    logInfo(this + " initialized")
    dependencies.foreach(_.initialize(firstInterval))
  }

  // This method checks whether the 'time' is valid wrt slideTime for generating RDD
  private def isTimeValid (time: Time): Boolean = {
    if (!isInitialized) 
      throw new Exception (this.toString + " has not been initialized")
    if ((time - zeroTime).isMultipleOf(slideTime)) { 
      true
    } else {
      false
    }
  }

  // This method either retrieves a precomputed RDD of this RDS, 
  // or computes the RDD (if the time is valid)
  def getOrCompute(time: Time): Option[RDD[T]] = {
      
    // if RDD was already generated, then retrieve it from HashMap
    generatedRDDs.get(time) match {
      
      // If an RDD was already generated and is being reused, then 
      // probably all RDDs in this RDS will be reused and hence should be cached
      case Some(oldRDD) => Some(oldRDD)
      
      // if RDD was not generated, and if the time is valid 
      // (based on sliding time of this RDS), then generate the RDD 
      case None =>
        if (isTimeValid(time)) {
          compute(time) match {
            case Some(newRDD) =>
              if (System.getProperty("spark.fake", "false") != "true" || 
                  newRDD.getStorageLevel == StorageLevel.NONE) {
                if (checkpointInterval != null && (time - zeroTime).isMultipleOf(checkpointInterval)) { 
                  newRDD.persist(checkpointLevel)
                  logInfo("Persisting " + newRDD + " to " + checkpointLevel + " at time " + time)
                } else if (storageLevel != StorageLevel.NONE) {
                  newRDD.persist(storageLevel)
                  logInfo("Persisting " + newRDD + " to " + storageLevel + " at time " + time)
                }
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

  // This method generates a SparkStream job for the given time
  // and may require to be overriden by subclasses
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

  /* -------------- */
  /* RDS operations */
  /* -------------- */
  
  def map[U: ClassManifest](mapFunc: T => U) = new MappedRDS(this, ssc.sc.clean(mapFunc))

  def flatMap[U: ClassManifest](flatMapFunc: T => Traversable[U]) = 
    new FlatMappedRDS(this, ssc.sc.clean(flatMapFunc))

  def filter(filterFunc: T => Boolean) = new FilteredRDS(this, filterFunc)

  def glom() = new GlommedRDS(this) 

  def mapPartitions[U: ClassManifest](mapPartFunc: Iterator[T] => Iterator[U]) = 
    new MapPartitionedRDS(this, ssc.sc.clean(mapPartFunc))

  def reduce(reduceFunc: (T, T) => T) = this.map(x => (1, x)).reduceByKey(reduceFunc, 1).map(_._2)

  def count() = this.map(_ => 1).reduce(_ + _)
  
  def collect() = this.map(x => (1, x)).groupByKey(1).map(_._2)

  def foreach(foreachFunc: T => Unit) = { 
    val newrds = new PerElementForEachRDS(this, ssc.sc.clean(foreachFunc))
    ssc.registerOutputStream(newrds)
    newrds
  }

  def foreachRDD(foreachFunc: RDD[T] => Unit) = {
    val newrds = new PerRDDForEachRDS(this, ssc.sc.clean(foreachFunc)) 
    ssc.registerOutputStream(newrds)
    newrds
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
    val newrds = new PerRDDForEachRDS(this, ssc.sc.clean(foreachFunc))
    ssc.registerOutputStream(newrds)
    newrds
  }

  def window(windowTime: Time, slideTime: Time) = new WindowedRDS(this, windowTime, slideTime)

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

  def union(that: RDS[T]) = new UnifiedRDS(Array(this, that))

  def register() = ssc.registerOutputStream(this)
}


class PairRDSFunctions[K: ClassManifest, V: ClassManifest](rds: RDS[(K,V)])
extends Serializable {
 
  def ssc = rds.ssc

  /* ---------------------------------- */
  /* RDS operations for key-value pairs */
  /* ---------------------------------- */
  
  def groupByKey(numPartitions: Int = 0): ShuffledRDS[K, V, ArrayBuffer[V]] = {
    def createCombiner(v: V) = ArrayBuffer[V](v)
    def mergeValue(c: ArrayBuffer[V], v: V) = (c += v)
    def mergeCombiner(c1: ArrayBuffer[V], c2: ArrayBuffer[V]) = (c1 ++ c2)
    combineByKey[ArrayBuffer[V]](createCombiner, mergeValue, mergeCombiner, numPartitions)
  }
  
  def reduceByKey(reduceFunc: (V, V) => V, numPartitions: Int = 0): ShuffledRDS[K, V, V] = {
    val cleanedReduceFunc = ssc.sc.clean(reduceFunc)
    combineByKey[V]((v: V) => v, cleanedReduceFunc, cleanedReduceFunc, numPartitions)  
  }

  private def combineByKey[C: ClassManifest](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    numPartitions: Int) : ShuffledRDS[K, V, C] = {
    new ShuffledRDS[K, V, C](rds, createCombiner, mergeValue, mergeCombiner, numPartitions)
  }

  def groupByKeyAndWindow(
    windowTime: Time, 
    slideTime: Time, 
    numPartitions: Int = 0): ShuffledRDS[K, V, ArrayBuffer[V]] = {
    rds.window(windowTime, slideTime).groupByKey(numPartitions)
  }

  def reduceByKeyAndWindow(
    reduceFunc: (V, V) => V, 
    windowTime: Time, 
    slideTime: Time, 
    numPartitions: Int = 0): ShuffledRDS[K, V, V] = {
    rds.window(windowTime, slideTime).reduceByKey(ssc.sc.clean(reduceFunc), numPartitions)
  }

  // This method is the efficient sliding window reduce operation, 
  // which requires the specification of an inverse reduce function, 
  // so that new elements introduced in the window can be "added" using 
  // reduceFunc to the previous window's result and old elements can be 
  // "subtracted using invReduceFunc.
  def reduceByKeyAndWindow(
    reduceFunc: (V, V) => V, 
    invReduceFunc: (V, V) => V,
    windowTime: Time, 
    slideTime: Time,
    numPartitions: Int): ReducedWindowedRDS[K, V] = {

    new ReducedWindowedRDS[K, V](
      rds, 
      ssc.sc.clean(reduceFunc),
      ssc.sc.clean(invReduceFunc),
      windowTime,
      slideTime,
      numPartitions)
  }
}


abstract class InputRDS[T: ClassManifest] (
    val inputName: String, 
    val batchDuration: Time,
    ssc: SparkStreamContext)
extends RDS[T](ssc) {
  
  override def dependencies = List()

  override def slideTime = batchDuration 
  
  def setReference(time: Time, reference: AnyRef) 
}


class FileInputRDS(
    val fileInputName: String,
    val directory: String,
    ssc: SparkStreamContext) 
extends InputRDS[String](fileInputName, LongTime(1000), ssc) {
  
  @transient val generatedFiles = new HashMap[Time,String]
 
  // TODO(Haoyuan): This is for the performance test.
  @transient
  val rdd = ssc.sc.textFile(SparkContext.inputFile, 
      SparkContext.idealPartitions).asInstanceOf[RDD[String]]
  
  override def compute(validTime: Time): Option[RDD[String]] = {
    generatedFiles.get(validTime) match {
      case Some(file) => 
        logInfo("Reading from file " + file  + " for time " + validTime)
        // Some(ssc.sc.textFile(file).asInstanceOf[RDD[String]])
        // The following line is for HDFS performance test. Sould comment out the above line.
        Some(rdd)
      case None =>
        throw new Exception(this.toString + ": Reference missing for time " + validTime + "!!!")
        None
    }
  }

  def setReference(time: Time, reference: AnyRef) {
    generatedFiles += ((time, reference.toString))
    logInfo("Reference added for time " + time + " - " + reference.toString)
  }
}

class NetworkInputRDS[T: ClassManifest](
    val networkInputName: String,
    val addresses: Array[InetSocketAddress],
    batchDuration: Time,
    ssc: SparkStreamContext) 
extends InputRDS[T](networkInputName, batchDuration, ssc) {

 
  // TODO(Haoyuan): This is for the performance test.
  @transient var rdd: RDD[T] = null

  if (System.getProperty("spark.fake", "false") == "true") {
    logInfo("Running initial count to cache fake RDD")
    rdd = ssc.sc.textFile(SparkContext.inputFile, 
        SparkContext.idealPartitions).asInstanceOf[RDD[T]]
    val fakeCacheLevel = System.getProperty("spark.fake.cache", "")
    if (fakeCacheLevel == "MEMORY_ONLY_2") {
      rdd.persist(StorageLevel.MEMORY_ONLY_2)
    } else if (fakeCacheLevel == "MEMORY_ONLY_DESER_2") {
      rdd.persist(StorageLevel.MEMORY_ONLY_2)
    } else if (fakeCacheLevel != "") {
      logError("Invalid fake cache level: " + fakeCacheLevel)
      System.exit(1)
    }
    rdd.count()
  }
  
  @transient val references = new HashMap[Time,String]
 
  override def compute(validTime: Time): Option[RDD[T]] = {
    if (System.getProperty("spark.fake", "false") == "true") {
      logInfo("Returning fake RDD at " + validTime)
      return Some(rdd)
    } 
    references.get(validTime) match {
      case Some(reference) => 
        if (reference.startsWith("file") || reference.startsWith("hdfs")) {
          logInfo("Reading from file " + reference  + " for time " + validTime)
          Some(ssc.sc.textFile(reference).asInstanceOf[RDD[T]])
        } else {
          logInfo("Getting from BlockManager " + reference + " for time " + validTime)
          Some(new BlockRDD(ssc.sc, Array(reference)))
        }
      case None =>
        throw new Exception(this.toString + ": Reference missing for time " + validTime + "!!!")
        None
    }
  }

  def setReference(time: Time, reference: AnyRef) {
    references += ((time, reference.toString))
    logInfo("Reference added for time " + time + " - " + reference.toString)
  }
}


class TestInputRDS(
    val testInputName: String,
    batchDuration: Time,
    ssc: SparkStreamContext) 
extends InputRDS[String](testInputName, batchDuration, ssc) {
  
  @transient val references = new HashMap[Time,Array[String]]
 
  override def compute(validTime: Time): Option[RDD[String]] = {
    references.get(validTime) match {
      case Some(reference) =>
        Some(new BlockRDD[String](ssc.sc, reference))
      case None =>
        throw new Exception(this.toString + ": Reference missing for time " + validTime + "!!!")
        None
    }
  }

  def setReference(time: Time, reference: AnyRef) {
    references += ((time, reference.asInstanceOf[Array[String]]))
  }
}


class MappedRDS[T: ClassManifest, U: ClassManifest] (
    parent: RDS[T], 
    mapFunc: T => U)
extends RDS[U](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.map[U](mapFunc))
  }
}


class FlatMappedRDS[T: ClassManifest, U: ClassManifest](
    parent: RDS[T], 
    flatMapFunc: T => Traversable[U])
extends RDS[U](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.flatMap(flatMapFunc))
  }
}


class FilteredRDS[T: ClassManifest](parent: RDS[T], filterFunc: T => Boolean)
extends RDS[T](parent.ssc) {
  
  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[T]] = {
    parent.getOrCompute(validTime).map(_.filter(filterFunc))
  }
}

class MapPartitionedRDS[T: ClassManifest, U: ClassManifest](
    parent: RDS[T], 
    mapPartFunc: Iterator[T] => Iterator[U])
extends RDS[U](parent.ssc) {

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[U]] = {
    parent.getOrCompute(validTime).map(_.mapPartitions[U](mapPartFunc))
  }
}

class GlommedRDS[T: ClassManifest](parent: RDS[T]) extends RDS[Array[T]](parent.ssc) {

  override def dependencies = List(parent)

  override def slideTime: Time = parent.slideTime

  override def compute(validTime: Time): Option[RDD[Array[T]]] = {
    parent.getOrCompute(validTime).map(_.glom())
  }
}


class ShuffledRDS[K: ClassManifest, V: ClassManifest, C: ClassManifest](
    parent: RDS[(K,V)],
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    numPartitions: Int)
  extends RDS [(K,C)] (parent.ssc) {
  
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


class UnifiedRDS[T: ClassManifest](parents: Array[RDS[T]]) 
extends RDS[T](parents(0).ssc) {

  if (parents.length == 0) {
    throw new IllegalArgumentException("Empty array of parents")
  }

  if (parents.map(_.ssc).distinct.size > 1) {
    throw new IllegalArgumentException("Array of parents have different SparkStreamContexts")
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


class PerElementForEachRDS[T: ClassManifest] (
    parent: RDS[T], 
    foreachFunc: T => Unit) 
extends RDS[Unit](parent.ssc) {
  
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


class PerRDDForEachRDS[T: ClassManifest] (
    parent: RDS[T], 
    foreachFunc: (RDD[T], Time) => Unit)
extends RDS[Unit](parent.ssc) {
  
  def this(parent: RDS[T], altForeachFunc: (RDD[T]) => Unit) =
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
