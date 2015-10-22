package org.apache.spark.streaming.dstream

import java.io.{ObjectInputStream, IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.util.{CompletionIterator, Utils}
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.streaming.dstream.OpenHashMapBasedStateMap._


// ==================================================
// ==================================================
// ================= PUBLIC CLASSES =================
// ==================================================
// ==================================================

sealed abstract class State[S] {
  def isDefined(): Boolean
  def get(): S
  def update(newState: S): Unit
  def remove(): Unit
  def isTimingOut(): Boolean

  @inline final def getOrElse[S1 >: S](default: => S1): S1 =
    if (isDefined) default else this.get
}


/** Class representing all the specification of session */
abstract class TrackStateSpec[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag]
  extends Serializable {

  def initialState(rdd: RDD[(K, S)]): this.type
  def initialState(javaPairRDD: JavaPairRDD[K, S]): this.type

  def numPartitions(numPartitions: Int): this.type
  def partitioner(partitioner: Partitioner): this.type

  def timeout(interval: Duration): this.type
}

object TrackStateSpec {

  def apply[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      trackingFunction: (K, Option[V], State[S]) => Option[T]): TrackStateSpec[K, V, S, T] = {
    new TrackStateSpecImpl[K, V, S, T](trackingFunction)
  }

  def create[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
      trackingFunction: (K, Option[V], State[S]) => Option[T]): TrackStateSpec[K, V, S, T] = {
    apply(trackingFunction)
  }
}


// ===============================================
// ===============================================
// ============== PRIVATE CLASSES ================
// ===============================================
// ===============================================


/** Class representing all the specification of session */
private[streaming] case class TrackStateSpecImpl[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    function: (K, Option[V], State[S]) => Option[T]) extends TrackStateSpec[K, V, S, T] {

  require(function != null)

  @volatile private var partitioner: Partitioner = null
  @volatile private var initialStateRDD: RDD[(K, S)] = null
  @volatile private var timeoutInterval: Duration = null


  def initialState(rdd: RDD[(K, S)]): this.type = {
    this.initialStateRDD = rdd
    this
  }

  def initialState(javaPairRDD: JavaPairRDD[K, S]): this.type = {
    this.initialStateRDD = javaPairRDD.rdd
    this
  }


  def numPartitions(numPartitions: Int): this.type = {
    this.partitioner(new HashPartitioner(numPartitions))
    this
  }

  def partitioner(partitioner: Partitioner): this.type = {
    this.partitioner = partitioner
    this
  }

  def timeout(interval: Duration): this.type = {
    this.timeoutInterval = interval
    this
  }

  // ================= Private Methods =================

  private[streaming] def getFunction(): (K, Option[V], State[S]) => Option[T] = function

  private[streaming] def getInitialStateRDD(): Option[RDD[(K, S)]] = Option(initialStateRDD)

  private[streaming] def getPartitioner(): Option[Partitioner] = Option(partitioner)

  private[streaming] def getTimeoutInterval(): Option[Duration] = Option(timeoutInterval)
}


private[streaming] class StateImpl[S] extends State[S] {

  private var state: S = null.asInstanceOf[S]
  private var defined: Boolean = true
  private var timingOut: Boolean = false
  private var updated: Boolean = false
  private var removed: Boolean = false

  // ========= Public API =========
  def isDefined(): Boolean = {
    defined
  }

  def get(): S = {
    null.asInstanceOf[S]
  }

  def update(newState: S): Unit = {
    require(!removed, "Cannot update the state after it has been removed")
    require(!timingOut, "Cannot update the state that is timing out")
    updated = true
    state = newState
  }

  def isTimingOut(): Boolean = {
    timingOut
  }

  def remove(): Unit = {
    require(!timingOut, "Cannot remove the state that is timing out")
    removed = true
  }

  // ========= Internal API =========

  def isRemoved(): Boolean = {
    removed
  }

  def isUpdated(): Boolean = {
    updated
  }

  def wrap(optionalState: Option[S]): Unit = {
    optionalState match {
      case Some(newState) =>
        this.state = newState
        defined = true

      case None =>
        this.state = null.asInstanceOf[S]
        defined = false
    }
    timingOut = false
    removed = false
    updated = false
  }

  def wrapTiminoutState(newState: S): Unit = {
    this.state = newState
    defined = true
    timingOut = true
    removed = false
    updated = false
  }


}



// -----------------------------------------------
// --------------- StateMap stuff --------------
// -----------------------------------------------

/** Internal interface for defining the map that keeps track of sessions. */
private[streaming] abstract class StateMap[K: ClassTag, S: ClassTag] extends Serializable {

  /** Get the state for a key if it exists */
  def get(key: K): Option[S]

  /** Get all the keys and states whose updated time is older than the give threshold time */
  def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)]

  /** Get all the keys and states in this map. */
  def getAll(): Iterator[(K, S, Long)]

  /** Add or update state */
  def put(key: K, state: S, updatedTime: Long): Unit

  /** Remove a key */
  def remove(key: K): Unit

  /**
   * Shallow copy `this` map to create a new state map.
   * Updates to the new map should not mutate `this` map.
   */
  def copy(): StateMap[K, S]

  def toDebugString(): String = toString()
}

private[streaming] object StateMap {
  def empty[K: ClassTag, S: ClassTag]: StateMap[K, S] = new EmptyStateMap[K, S]

  def create[K: ClassTag, S: ClassTag](conf: SparkConf): StateMap[K, S] = {
    val deltaChainThreshold = conf.getInt("spark.streaming.sessionByKey.deltaChainThreshold",
      DELTA_CHAIN_LENGTH_THRESHOLD)
    new OpenHashMapBasedStateMap[K, S](64, deltaChainThreshold)
  }
}

/** Specific implementation of SessionStore interface representing an empty map */
private[streaming] class EmptyStateMap[K: ClassTag, S: ClassTag] extends StateMap[K, S] {
  override def put(key: K, session: S, updateTime: Long): Unit = ???
  override def get(key: K): Option[S] = None
  override def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = Iterator.empty
  override def copy(): StateMap[K, S] = new EmptyStateMap[K, S]
  override def remove(key: K): Unit = { }
  override def getAll(): Iterator[(K, S, Long)] = Iterator.empty
  override def toDebugString(): String = ""
}

/** Implementation of StateMap based on Spark's OpenHashMap */
private[streaming] class OpenHashMapBasedStateMap[K: ClassTag, S: ClassTag](
    @transient @volatile private var parentStateMap: StateMap[K, S],
    initialCapacity: Int = 64,
    deltaChainThreshold: Int = DELTA_CHAIN_LENGTH_THRESHOLD
  ) extends StateMap[K, S] { self =>

  def this(initialCapacity: Int, deltaChainThreshold: Int) = this(
    new EmptyStateMap[K, S],
    initialCapacity = initialCapacity,
    deltaChainThreshold = deltaChainThreshold)

  def this(deltaChainThreshold: Int) = this(
    initialCapacity = 64, deltaChainThreshold = deltaChainThreshold)

  def this() = this(DELTA_CHAIN_LENGTH_THRESHOLD)

  @transient @volatile private var deltaMap =
    new OpenHashMap[K, StateInfo[S]](initialCapacity)

  /** Get the session data if it exists */
  override def get(key: K): Option[S] = {
    val stateInfo = deltaMap(key)
    if (stateInfo != null && !stateInfo.deleted) {
      Some(stateInfo.data)
    } else {
      parentStateMap.get(key)
    }
  }

  /** Get all the keys and states whose updated time is older than the give threshold time */
  override def getByTime(threshUpdatedTime: Long): Iterator[(K, S, Long)] = {
    val oldStates = parentStateMap.getByTime(threshUpdatedTime).filter { case (key, value, _) =>
      !deltaMap.contains(key)
    }

    val updatedStates = deltaMap.iterator.flatMap { case (key, stateInfo) =>
      if (! stateInfo.deleted && stateInfo.updateTime < threshUpdatedTime) {
        Some((key, stateInfo.data, stateInfo.updateTime))
      } else None
    }
    oldStates ++ updatedStates
  }

  /** Get all the keys and states in this map. */
  override def getAll(): Iterator[(K, S, Long)] = {

    val oldStates = parentStateMap.getAll().filter { case (key, _, _) =>
      !deltaMap.contains(key)
    }
    
    val updatedStates = deltaMap.iterator.filter { ! _._2.deleted }.map { case (key, stateInfo) =>
      (key, stateInfo.data, stateInfo.updateTime)
    }
    oldStates ++ updatedStates
  }

  /** Add or update state */
  override def put(key: K, state: S, updateTime: Long): Unit = {
    val stateInfo = deltaMap(key)
    if (stateInfo != null) {
      stateInfo.update(state, updateTime)
    } else {
      deltaMap.update(key, new StateInfo(state, updateTime))
    }
  }

  /** Remove a state */
  override def remove(key: K): Unit = {
    val stateInfo = deltaMap(key)
    if (stateInfo != null) {
      stateInfo.markDeleted()
    } else {
      val newInfo = new StateInfo[S](deleted = true)
      deltaMap.update(key, newInfo)
    }
  }

  /**
   * Shallow copy the map to create a new session store. Updates to the new map
   * should not mutate `this` map.
   */
  override def copy(): StateMap[K, S] = {
    new OpenHashMapBasedStateMap[K, S](this, deltaChainThreshold = deltaChainThreshold)
  }

  def shouldCompact: Boolean = {
    deltaChainLength >= deltaChainThreshold
  }

  def deltaChainLength: Int = parentStateMap match {
    case map: OpenHashMapBasedStateMap[_, _] => map.deltaChainLength + 1
    case _ => 0
  }

  def approxSize: Int = deltaMap.size + {
    parentStateMap match {
      case s: OpenHashMapBasedStateMap[_, _] => s.approxSize
      case _ => 0
    }
  }

  override def toDebugString(): String = {
    val tabs = if (deltaChainLength > 0) {
      ("    " * (deltaChainLength - 1)) +"+--- "
    } else ""
    parentStateMap.toDebugString() + "\n" + deltaMap.iterator.mkString(tabs, "\n" + tabs, "")
  }

  /*
  class CompactParentOnCompletionIterator(iterator: Iterator[(K, S, Long)])
    extends CompletionIterator[(K, S, Long), Iterator[(K, S, Long)]](iterator) {

    val newParentStateMap =
      new OpenHashMapBasedStateMap[K, S](initialCapacity = approxSize, deltaChainThreshold)

    override def next(): (K, S, Long) = {
      val next = super.next()
      newParentStateMap.put(next._1, next._2, next._3)
      next
    }

    override def completion(): Unit = {
      self.parentStateMap = newParentStateMap
    }
  }
  */

  private def writeObject(outputStream: ObjectOutputStream): Unit = {

    outputStream.defaultWriteObject()

    // Write the deltaMap
    outputStream.writeInt(deltaMap.size)
    val deltaMapIterator = deltaMap.iterator
    var deltaMapCount = 0
    while (deltaMapIterator.hasNext) {
      deltaMapCount += 1
      val (key, stateInfo) = deltaMapIterator.next()
      outputStream.writeObject(key)
      outputStream.writeObject(stateInfo)
    }
    assert(deltaMapCount == deltaMap.size)

    // Write the parentStateMap while consolidating
    val doCompaction = shouldCompact
    val newParentSessionStore = if (doCompaction) {
      new OpenHashMapBasedStateMap[K, S](initialCapacity = approxSize, deltaChainThreshold)
    } else { null }

    val iterOfActiveSessions = parentStateMap.getAll()

    var parentSessionCount = 0

    outputStream.writeInt(approxSize)

    while(iterOfActiveSessions.hasNext) {
      parentSessionCount += 1

      val (key, state, updateTime) = iterOfActiveSessions.next()
      outputStream.writeObject(key)
      outputStream.writeObject(state)
      outputStream.writeLong(updateTime)

      if (doCompaction) {
        newParentSessionStore.deltaMap.update(
          key, StateInfo(state, updateTime, deleted = false))
      }
    }
    val limiterObj = new Limiter(parentSessionCount)
    outputStream.writeObject(limiterObj)
    if (doCompaction) {
      parentStateMap = newParentSessionStore
    }
  }

  private def readObject(inputStream: ObjectInputStream): Unit = {
    inputStream.defaultReadObject()

    val deltaMapSize = inputStream.readInt()
    deltaMap = new OpenHashMap[K, StateInfo[S]]()
    var deltaMapCount = 0
    while (deltaMapCount < deltaMapSize) {
      val key = inputStream.readObject().asInstanceOf[K]
      val sessionInfo = inputStream.readObject().asInstanceOf[StateInfo[S]]
      deltaMap.update(key, sessionInfo)
      deltaMapCount += 1
    }

    val parentSessionStoreSizeHint = inputStream.readInt()
    val newParentSessionStore = new OpenHashMapBasedStateMap[K, S](
      initialCapacity = parentSessionStoreSizeHint, deltaChainThreshold)

    var parentSessionLoopDone = false
    while(!parentSessionLoopDone) {
      val obj = inputStream.readObject()
      if (obj.isInstanceOf[Limiter]) {
        parentSessionLoopDone = true
        val expectedCount = obj.asInstanceOf[Limiter].num
        assert(expectedCount == newParentSessionStore.deltaMap.size)
      } else {
        val key = obj.asInstanceOf[K]
        val state = inputStream.readObject().asInstanceOf[S]
        val updateTime = inputStream.readLong()
        newParentSessionStore.deltaMap.update(
          key, StateInfo(state, updateTime, deleted = false))
      }
    }
    parentStateMap = newParentSessionStore
  }

/*
  private def writeObject(outputStream: ObjectOutputStream): Unit = {
    if (deltaChainLength > deltaChainThreshold) {
      val newParentSessionStore =
        new OpenHashMapBasedSessionStore[K, S](initialCapacity = sizeHint, deltaChainThreshold)
      val iterOfActiveSessions = parentStateMap.iterator(updatedSessionsOnly = false).filter {
        _.isActive
      }

      while (iterOfActiveSessions.hasNext) {
        val session = iterOfActiveSessions.next()
        newParentSessionStore.deltaMap.update(
          session.getKey(), SessionInfo(session.getData(), deleted = false))
      }
      parentStateMap = newParentSessionStore
    }
    outputStream.defaultWriteObject()
  }

  private def readObject(inputStream: ObjectInputStream): Unit = {
    inputStream.defaultReadObject()
  }
*/
}

class Limiter(val num: Int) extends Serializable


private[streaming] object OpenHashMapBasedStateMap {

  case class StateInfo[S](
      var data: S = null.asInstanceOf[S],
      var updateTime: Long = -1,
      var deleted: Boolean = false) {

    def markDeleted(): Unit = {
      deleted = true
    }

    def update(newData: S, newUpdateTime: Long): Unit = {
      data = newData
      updateTime = newUpdateTime
      deleted = false
    }
  }

  val DELTA_CHAIN_LENGTH_THRESHOLD = 20
}



// -----------------------------------------------
// --------------- StateRDD stuff --------------
// -----------------------------------------------

private[streaming] case class TrackStateRDDRecord[K: ClassTag, S: ClassTag, T: ClassTag](
    stateMap: StateMap[K, S], emittedRecords: Seq[T])


private[streaming] class TrackStateRDDPartition(
    idx: Int,
    @transient private var prevStateRDD: RDD[_],
    @transient private var partitionedDataRDD: RDD[_]) extends Partition {

  private[dstream] var previousSessionRDDPartition: Partition = null
  private[dstream] var partitionedDataRDDPartition: Partition = null

  override def index: Int = idx
  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    previousSessionRDDPartition = prevStateRDD.partitions(index)
    partitionedDataRDDPartition = partitionedDataRDD.partitions(index)
    oos.defaultWriteObject()
  }
}

private[streaming] class TrackStateRDD[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    _sc: SparkContext,
    private var prevStateRDD: RDD[TrackStateRDDRecord[K, S, T]],
    private var partitionedDataRDD: RDD[(K, V)],
    trackingFunction: (K, Option[V], State[S]) => Option[T],
    currentTime: Long, timeoutThresholdTime: Option[Long]
  ) extends RDD[TrackStateRDDRecord[K, S, T]](
    _sc,
    List(
      new OneToOneDependency[TrackStateRDDRecord[K, S, T]](prevStateRDD),
      new OneToOneDependency(partitionedDataRDD))
  ) {

  @volatile private var doFullScan = false

  require(partitionedDataRDD.partitioner.nonEmpty)
  require(partitionedDataRDD.partitioner == prevStateRDD.partitioner)

  override val partitioner = prevStateRDD.partitioner

  override def checkpoint(): Unit = {
    super.checkpoint()
    doFullScan = true
  }

  override def compute(
      partition: Partition, context: TaskContext): Iterator[TrackStateRDDRecord[K, S, T]] = {

    val stateRDDPartition = partition.asInstanceOf[TrackStateRDDPartition]
    val prevStateRDDIterator = prevStateRDD.iterator(
      stateRDDPartition.previousSessionRDDPartition, context)
    val dataIterator = partitionedDataRDD.iterator(
      stateRDDPartition.partitionedDataRDDPartition, context)
    if (!prevStateRDDIterator.hasNext) {
      throw new SparkException(s"Could not find state map in previous state RDD")
    }

    val newStateMap = prevStateRDDIterator.next().stateMap.copy()
    val emittedRecords = new ArrayBuffer[T]

    val stateWrapper = new StateImpl[S]()

    dataIterator.foreach { case (key, value) =>
      stateWrapper.wrap(newStateMap.get(key))
      val emittedRecord = trackingFunction(key, Some(value), stateWrapper)
      if (stateWrapper.isRemoved()) {
        newStateMap.remove(key)
      } else if (stateWrapper.isUpdated()) {
        newStateMap.put(key, stateWrapper.get(), currentTime)
      }
      emittedRecords ++= emittedRecord
    }

    if (doFullScan) {
      if (timeoutThresholdTime.isDefined) {
        newStateMap.getByTime(timeoutThresholdTime.get).foreach { case (key, state, _) =>
          stateWrapper.wrapTiminoutState(state)
          val emittedRecord = trackingFunction(key, None, stateWrapper)
          emittedRecords ++= emittedRecord
        }
      }
    }

    Iterator(TrackStateRDDRecord(newStateMap, emittedRecords))
  }

  override protected def getPartitions: Array[Partition] = {
    Array.tabulate(prevStateRDD.partitions.length) { i =>
      new TrackStateRDDPartition(i, prevStateRDD, partitionedDataRDD)}
  }

  override def clearDependencies() {
    super.clearDependencies()
    prevStateRDD = null
    partitionedDataRDD = null
  }
}

private[streaming] object TrackStateRDD {
  def createFromPairRDD[K: ClassTag, S: ClassTag, T: ClassTag](
      pairRDD: RDD[(K, S)],
      partitioner: Partitioner,
      updateTime: Long): RDD[TrackStateRDDRecord[K, S, T]] = {

    val createRecord = (iterator: Iterator[(K, S)]) => {
      val stateMap = StateMap.create[K, S](SparkEnv.get.conf)
      iterator.foreach { case (key, state) => stateMap.put(key, state, updateTime) }
      Iterator(TrackStateRDDRecord(stateMap, Seq.empty[T]))
    }
    pairRDD.partitionBy(partitioner).mapPartitions[TrackStateRDDRecord[K, S, T]](
      createRecord, true)
  }
}


// -----------------------------------------------
// ---------------- SessionDStream ---------------
// -----------------------------------------------


private[streaming] class TrackStateDStream[K: ClassTag, V: ClassTag, S: ClassTag, T: ClassTag](
    parent: DStream[(K, V)], spec: TrackStateSpecImpl[K, V, S, T])
  extends DStream[TrackStateRDDRecord[K, S, T]](parent.context) {

  persist(StorageLevel.MEMORY_ONLY)

  private val partitioner = spec.getPartitioner().getOrElse(
    new HashPartitioner(ssc.sc.defaultParallelism))

  private val trackingFunction = spec.getFunction()

  override def slideDuration: Duration = parent.slideDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override val mustCheckpoint = true

  /** Method that generates a RDD for the given time */
  override def compute(validTime: Time): Option[RDD[TrackStateRDDRecord[K, S, T]]] = {
    val prevStateRDD = getOrCompute(validTime - slideDuration).getOrElse {
      TrackStateRDD.createFromPairRDD[K, S, T](
        spec.getInitialStateRDD().getOrElse(new EmptyRDD[(K, S)](ssc.sparkContext)),
        partitioner,
        validTime.milliseconds
      )
    }
    val newDataRDD = parent.getOrCompute(validTime).get
    val partitionedDataRDD = newDataRDD.partitionBy(partitioner)
    val timeoutThresholdTime = spec.getTimeoutInterval().map { interval =>
      (validTime - interval).milliseconds
    }

    Some(new TrackStateRDD(
      ssc.sparkContext, prevStateRDD, partitionedDataRDD,
      trackingFunction, validTime.milliseconds, timeoutThresholdTime))
  }
}
