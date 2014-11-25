/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.{ObjectOutputStream, IOException}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.util.Utils
import org.apache.spark.util.collection._

private[spark] sealed trait JoinType[K, L, R, PAIR <: Product2[_, _]] extends Serializable {

  def flatten(i: Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))]): Iterator[(K, PAIR)]

}

private[spark] object JoinType {

  def inner[K, L, R] = new JoinType[K, L, R, (L, R)] {

    override def flatten(i: Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))]) =
      i flatMap {
        case (key, pair) => {
          if (pair._1.size < pair._2.size) {
            yieldPair(pair._1, pair._2, key, (p1: L, p2: R) => (p1, p2))
          } else {
            yieldPair(pair._2, pair._1, key, (p2: R, p1: L) => (p1, p2))
          }
        }
      }
  }

  def leftOuter[K, L, R] = new JoinType[K, L, R, (L, Option[R])] {

    override def flatten(i: Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))]) =
      i flatMap {
        case (key, pair) => {
          if (pair._2.size == 0) {
            for (chunk <- pair._1.iterator;
                 v <- chunk
            ) yield (key, (v, None)): (K, (L, Option[R]))
          }
          else if (pair._1.size < pair._2.size) {
            yieldPair(pair._1, pair._2, key, (p1: L, p2: R) => (p1, Some(p2)))
          } else {
            yieldPair(pair._2, pair._1, key, (p2: R, p1: L) => (p1, Some(p2)))
          }
        }
      }
  }

  def rightOuter[K, L, R] = new JoinType[K, L, R, (Option[L], R)] {

    override def flatten(i: Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))]) =
      i flatMap {
        case (key, pair) => {
          if (pair._1.size == 0) {
            for (chunk <- pair._2.iterator;
                 v <- chunk
            ) yield (key, (None, v)): (K, (Option[L], R))
          }
          else if (pair._1.size < pair._2.size) {
            yieldPair(pair._1, pair._2, key, (p1: L, p2: R) => (Some(p1), p2))
          } else {
            yieldPair(pair._2, pair._1, key, (p2: R, p1: L) => (Some(p1), p2))
          }
        }
      }
  }

  def fullOuter[K, L, R] = new JoinType[K, L, R, (Option[L], Option[R])] {

    override def flatten(i: Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))]) =
      i flatMap {
        case (key, pair) => {
          if (pair._1.size == 0) {
            for (chunk <- pair._2.iterator;
                 v <- chunk
            ) yield (key, (None, Some(v))): (K, (Option[L], Option[R]))
          }
          else if (pair._2.size == 0) {
            for (chunk <- pair._1.iterator;
                 v <- chunk
            ) yield (key, (Some(v), None)): (K, (Option[L], Option[R]))
          }
          else if (pair._1.size < pair._2.size) {
            yieldPair(pair._1, pair._2, key, (p1: L, p2: R) => (Some(p1), Some(p2)))
          } else {
            yieldPair(pair._2, pair._1, key, (p2: R, p1: L) => (Some(p1), Some(p2)))
          }
        }
      }
  }

  private def yieldPair[K, OUT, IN, PAIR <: Product2[_, _]](
      outer: Iterable[Chunk[OUT]], inner: Iterable[Chunk[IN]], key: K, toPair: (OUT, IN) => PAIR) =
    for (
      outerChunk <- outer.iterator;
      innerChunk <- inner.iterator;
      outerValue <- outerChunk;
      innerValue <- innerChunk
    ) yield (key, toPair(outerValue, innerValue))
}

private[spark] sealed trait JoinSplitDep extends Serializable

private[spark] case class NarrowJoinSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition) extends JoinSplitDep {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] case class ShuffleJoinSplitDep(handle: ShuffleHandle) extends JoinSplitDep

private[spark] class JoinPartition(idx: Int, val left: JoinSplitDep, val right: JoinSplitDep)
  extends Partition with Serializable {
  override val index: Int = idx

  override def hashCode(): Int = idx
}

private[spark] class BufferWrapper private(val isChunkBuffer: Boolean, buffer: Iterable[Any])
  extends Serializable {

  def this(buffer: CompactBuffer[_]) {
    this(false, buffer)
  }

  def this(buffer: ChunkBuffer[_]) {
    this(true, buffer)
  }

  def getChunkBuffer[T: ClassTag]: ChunkBuffer[T] = buffer.asInstanceOf[ChunkBuffer[T]]

  def getCompactBuffer[T: ClassTag]: CompactBuffer[T] = buffer.asInstanceOf[CompactBuffer[T]]

  def asChunkIterable[T: ClassTag]: Iterable[Chunk[T]] = {
    if (isChunkBuffer) {
      getChunkBuffer[T]
    }
    else {
      val buffer = getCompactBuffer[T]
      if (buffer.isEmpty) {
        Iterable[Chunk[T]]()
      }
      else {
        Iterable(new Chunk[T](buffer))
      }
    }
  }
}

private[spark] class SkewedJoinRDD[K, L, R, PAIR <: Product2[_, _]](
    left: RDD[(K, L)], right: RDD[(K, R)], part: Partitioner, joinType: JoinType[K, L, R, PAIR])
    (implicit kt: ClassTag[K], lt: ClassTag[L], rt: ClassTag[R], ord: Ordering[K])
  extends RDD[(K, PAIR)](left.context, Nil) with Logging {

  // Ordering is necessary. ExternalOrderingAppendOnlyMap needs it to prefetch a key without
  // loading its value to avoid OOM.
  require(ord != null, "No implicit Ordering defined for " + kt.runtimeClass)

  private type JoinValue = (Option[L], Option[R])
  private type JoinCombiner = (ChunkBuffer[L], ChunkBuffer[R])
  private type JoinIterableCombiner = (BufferWrapper, BufferWrapper)

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): SkewedJoinRDD[K, L, R, PAIR] = {
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(left, right).map { rdd: RDD[_ <: Product2[K, _]] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, JoinCombiner](rdd, part, serializer)
      }
    }
  }

  private def getJoinSplitDep(rdd: RDD[_], index: Int, dep: Dependency[_]): JoinSplitDep =
    dep match {
      case s: ShuffleDependency[_, _, _] =>
        new ShuffleJoinSplitDep(s.shuffleHandle)
      case _ =>
        new NarrowJoinSplitDep(rdd, index, rdd.partitions(index))
    }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new JoinPartition(i,
        getJoinSplitDep(left, i, dependencies(0)),
        getJoinSplitDep(right, i, dependencies(1)))
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  override def compute(s: Partition, context: TaskContext): Iterator[(K, PAIR)] = {
    joinType.flatten(internalCompute(s, context).map {
      case (key, (i1, i2)) => {
        if (i1.isInstanceOf[ChunkBuffer[_]]) {
          context.taskMetrics().diskBytesSpilled +=
            i1.asInstanceOf[ChunkBuffer[Chunk[L]]].diskBytesSpilled
        }
        if (i2.isInstanceOf[ChunkBuffer[_]]) {
          context.taskMetrics().diskBytesSpilled +=
            i2.asInstanceOf[ChunkBuffer[Chunk[R]]].diskBytesSpilled
        }
        (key, (i1, i2))
      }
    })
  }

  private def internalCompute(s: Partition, context: TaskContext):
      Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true)
    val split = s.asInstanceOf[JoinPartition]
    val leftIter = joinSplitDepToIterator[L](split.left, split.index, context)
    val rightIter = joinSplitDepToIterator[R](split.right, split.index, context)

    val parameters = new ChunkParameters
    if (!externalSorting) {
      val map = new AppendOnlyMap[K, JoinCombiner]
      val update: (Boolean, JoinCombiner) => JoinCombiner = (hadVal, oldVal) => {
        if (hadVal) oldVal else (ChunkBuffer[L](parameters), ChunkBuffer[R](parameters))
      }
      val getCombiner: K => JoinCombiner = key => {
        map.changeValue(key, update)
      }
      while (leftIter.hasNext) {
        val kv = leftIter.next()
        getCombiner(kv._1)._1 += kv._2
      }
      while (rightIter.hasNext) {
        val kv = rightIter.next()
        getCombiner(kv._1)._2 += kv._2
      }
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, (Iterable[Chunk[L]], Iterable[Chunk[R]]))]])
    } else {
      val map = createExternalMap(parameters)
      map.insertAll(leftIter.map(item => (item._1, (Some(item._2), None))))
      map.insertAll(rightIter.map(item => (item._1, (None, Some(item._2)))))
      context.taskMetrics.memoryBytesSpilled += map.memoryBytesSpilled
      context.taskMetrics.diskBytesSpilled += map.diskBytesSpilled
      new InterruptibleIterator(context,
        map.iterator.map {
          case (key, combiner) => {
            val left = combiner._1.asChunkIterable[L]
            val right = combiner._2.asChunkIterable[R]
            (key, (left, right))
          }
        })
    }
  }

  private def joinSplitDepToIterator[V](dep: JoinSplitDep, splitIndex: Int, context: TaskContext):
  Iterator[Product2[K, V]] =
    dep match {
      case NarrowJoinSplitDep(rdd, _, itsSplit) =>
        rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, V]]]
      case ShuffleJoinSplitDep(handle) =>
        SparkEnv.get.shuffleManager
          .getReader(handle, splitIndex, splitIndex + 1, context)
          .read()
    }

  private def createExternalMap(parameters: ChunkParameters)
  : ExternalOrderingAppendOnlyMap[K, JoinValue, JoinIterableCombiner] = {

    val createCombiner: JoinValue => JoinIterableCombiner =
      value => {
        val left = CompactBuffer[L]()
        value._1.foreach(left += _)
        val right = CompactBuffer[R]()
        value._2.foreach(right += _)
        (new BufferWrapper(left), new BufferWrapper(right))
      }

    val mergeValue: (JoinIterableCombiner, JoinValue) => JoinIterableCombiner =
      (combiner, value) => {
        value._1.foreach(combiner._1.getCompactBuffer[L] += _)
        value._2.foreach(combiner._2.getCompactBuffer[R] += _)
        combiner
      }

    val createExternalCombiner = () => {
      val left = new ChunkBuffer[L](parameters)
      val right = new ChunkBuffer[R](parameters)
      (new BufferWrapper(left), new BufferWrapper(right))
    }

    // Call when reading data from the disks
    // The real pattern is:
    // ((ChunkBuffer, ChunkBuffer), (CompactBuffer, CompactBuffer)) => (ChunkBuffer, ChunkBuffer)
    def mergeCombiners(combiner1: JoinIterableCombiner, combiner2: JoinIterableCombiner):
        JoinIterableCombiner = {
      assert(combiner1._1.isChunkBuffer && combiner1._2.isChunkBuffer, "left must be a ChunkBuffer")
      assert(!combiner2._1.isChunkBuffer && !combiner2._2.isChunkBuffer,
        "right must be a CompactBuffer")

      combiner1._1.getChunkBuffer[L] ++= combiner2._1.getCompactBuffer[L]
      combiner1._2.getChunkBuffer[R] ++= combiner2._2.getCompactBuffer[R]
      combiner1
    }

    new ExternalOrderingAppendOnlyMap[K, JoinValue, JoinIterableCombiner](
      createCombiner, mergeValue, createExternalCombiner, mergeCombiners)
  }
}
