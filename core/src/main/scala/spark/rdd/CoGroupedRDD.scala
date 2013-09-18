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

package spark.rdd

import java.io.{ObjectOutputStream, IOException}
import java.util.{HashMap => JHashMap}

import scala.collection.JavaConversions
import scala.collection.mutable.ArrayBuffer

import spark.{Aggregator, Partition, Partitioner, RDD, SparkEnv, TaskContext}
import spark.{Dependency, OneToOneDependency, ShuffleDependency}


private[spark] sealed trait CoGroupSplitDep extends Serializable

private[spark] case class NarrowCoGroupSplitDep(
    rdd: RDD[_],
    splitIndex: Int,
    var split: Partition
  ) extends CoGroupSplitDep {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

private[spark] case class ShuffleCoGroupSplitDep(shuffleId: Int) extends CoGroupSplitDep

private[spark]
class CoGroupPartition(idx: Int, val deps: Array[CoGroupSplitDep])
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

private[spark] class CoGroupAggregator
  extends Aggregator[Any, Any, ArrayBuffer[Any]](
    { x => ArrayBuffer(x) },
    { (b, x) => b += x },
    { (b1, b2) => b1 ++ b2 })
  with Serializable


/**
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output.
 * @param mapSideCombine flag indicating whether to merge values before shuffle step. If the flag
 *                       is on, Spark does an extra pass over the data on the map side to merge
 *                       all values belonging to the same key together. This can reduce the amount
 *                       of data shuffled if and only if the number of distinct keys is very small,
 *                       and the ratio of key size to value size is also very small.
 */
class CoGroupedRDD[K](
  @transient var rdds: Seq[RDD[(K, _)]],
  part: Partitioner,
  val mapSideCombine: Boolean = false,
  val serializerClass: String = null)
  extends RDD[(K, Seq[Seq[_]])](rdds.head.context, Nil) {

  private val aggr = new CoGroupAggregator

  override def getDependencies: Seq[Dependency[_]] = {
    rdds.map { rdd =>
      if (rdd.partitioner == Some(part)) {
        logInfo("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        logInfo("Adding shuffle dependency with " + rdd)
        if (mapSideCombine) {
          val mapSideCombinedRDD = rdd.mapPartitions(aggr.combineValuesByKey(_), true)
          new ShuffleDependency[Any, ArrayBuffer[Any]](mapSideCombinedRDD, part, serializerClass)
        } else {
          new ShuffleDependency[Any, Any](rdd.asInstanceOf[RDD[(Any, Any)]], part, serializerClass)
        }
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _] =>
            new ShuffleCoGroupSplitDep(s.shuffleId)
          case _ =>
            new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i))
        }
      }.toArray)
    }
    array
  }

  override val partitioner = Some(part)

  override def compute(s: Partition, context: TaskContext): Iterator[(K, Seq[Seq[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size
    // e.g. for `(k, a) cogroup (k, b)`, K -> Seq(ArrayBuffer as, ArrayBuffer bs)
    val map = new JHashMap[K, Seq[ArrayBuffer[Any]]]

    def getSeq(k: K): Seq[ArrayBuffer[Any]] = {
      val seq = map.get(k)
      if (seq != null) {
        seq
      } else {
        val seq = Array.fill(numRdds)(new ArrayBuffer[Any])
        map.put(k, seq)
        seq
      }
    }

    val ser = SparkEnv.get.serializerManager.get(serializerClass)
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
        // Read them from the parent
        for ((k, v) <- rdd.iterator(itsSplit, context)) {
          getSeq(k.asInstanceOf[K])(depNum) += v
        }
      }
      case ShuffleCoGroupSplitDep(shuffleId) => {
        // Read map outputs of shuffle
        val fetcher = SparkEnv.get.shuffleFetcher
        if (mapSideCombine) {
          // With map side combine on, for each key, the shuffle fetcher returns a list of values.
          fetcher.fetch[K, Seq[Any]](shuffleId, split.index, context.taskMetrics, ser).foreach {
            case (key, values) => getSeq(key)(depNum) ++= values
          }
        } else {
          // With map side combine off, for each key the shuffle fetcher returns a single value.
          fetcher.fetch[K, Any](shuffleId, split.index, context.taskMetrics, ser).foreach {
            case (key, value) => getSeq(key)(depNum) += value
          }
        }
      }
    }
    JavaConversions.mapAsScalaMap(map).iterator
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
