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

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.util.collection.CompactBuffer

private[spark] case class ShuffleJoinSplitDep(handle: ShuffleHandle) extends Serializable

private[spark] class JoinPartition(idx: Int, val left: ShuffleJoinSplitDep,
                                   val right: ShuffleJoinSplitDep)
  extends Partition with Serializable {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

private[spark] sealed trait JoinType[K: ClassTag, L: ClassTag, R: ClassTag, PAIR <: Product2[_, _]] extends Serializable {

  private[spark] var joinType: Int = 0

  def flatten(iterators: Iterator[(K, (Iterator[L], Iterator[R]))]): Iterator[(K, PAIR)]

  def mergeIterators(leftIter: Iterator[L], rightIter: Iterator[R])(implicit lt: ClassTag[L: Iterator[(L, R)] = {
    val buffer = new CompactBuffer[L]()
    while(leftIter.hasNext){
      buffer += leftIter.next
    }
    var index = buffer.size
    var rightElem: R = null.asInstanceOf[R]
    new Iterator[(L, R)] {
      def hasNext: Boolean = {
        if (index == buffer.size) {
          if (rightIter.hasNext) {
            rightElem = rightIter.next
            index = 0
            true
          } else {
            false
          }
        } else {
          true
        }
      }

      def next(): (L, R) = {
        val joinRow: (L, R) = (buffer(index), rightElem)
        index += 1
        joinRow
      }
    }
  }
}

private[spark] object JoinType {

  private[spark] final val INNER: Int = 1
  private[spark] final val LEFTOUTER: Int = 2
  private[spark] final val RIGHTOUTER: Int = 3

  def inner[K, L, R] = new JoinType[K, L, R, (L, R)] {

    joinType = JoinType.INNER

    override def flatten(iterators: Iterator[(K, (Iterator[L], Iterator[R]))]) = {
      iterators flatMap {
        case (key, pair) => {
          mergeIterators(pair._1, pair._2).map(value => (key, (value._1, value._2)))
        }
      }
    }
  }

  def leftOuter[K, L, R] = new JoinType[K, L, R, (L, Option[R])] {

    joinType = JoinType.LEFTOUTER

    override def flatten(iterators: Iterator[(K, (Iterator[L], Iterator[R]))]) = {
      iterators flatMap {
        case (key, pair) => {
          if (pair._2.isEmpty) {
            pair._1.map(v => (key, (v, None)))
          } else {
            mergeIterators(pair._1, pair._2).map(value => (key, (value._1, Some(value._2))))
          }
        }
      }
    }
  }

  def rightOuter[K, L, R] = new JoinType[K, L, R, (Option[L], R)] {

    joinType = JoinType.RIGHTOUTER

    override def flatten(iterators: Iterator[(K, (Iterator[L], Iterator[R]))]) = {
      iterators flatMap {
        case (key, pair) => {
          if (pair._1.isEmpty) {
            pair._2.map(w => (key, (None, w)))
          } else {
            mergeIterators(pair._1, pair._2).map(value => (key, (Some(value._1), value._2)))
          }
        }
      }
    }
  }
}

private[spark] class SortMergeJoinRDD[K, L, R, PAIR <: Product2[_, _]](
    left: RDD[(K, L)],
    right: RDD[(K, R)],
    part: Partitioner,
    join: JoinType[K, L, R, PAIR])
    (implicit kt: ClassTag[K], lt: ClassTag[L], rt: ClassTag[R], keyOrdering: Ordering[K])
  extends RDD[(K, PAIR)](left.context, Nil) with Logging {

  // Ordering is necessary. SortMergeJoin needs to Sort by Key
  require(keyOrdering != null, "No implicit Ordering defined for " + kt.runtimeClass)

  private var serializer: Option[Serializer] = None

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): SortMergeJoinRDD[K, L, R, PAIR] = {
    this.serializer = Option(serializer)
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(left, right).map { rdd: RDD[_ <: Product2[K, _]] =>
      logDebug("Adding shuffle dependency with " + rdd)
      new ShuffleDependency[K, Any, Any](rdd, part, serializer, Some(keyOrdering))
    }
  }

  private def getJoinSplitDep(dep: Dependency[_]): ShuffleJoinSplitDep =
    dep match {
      case s: ShuffleDependency[_, _, _] =>
        new ShuffleJoinSplitDep(s.shuffleHandle)
    }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.size) {
      array(i) = new JoinPartition(i, getJoinSplitDep(dependencies(0)),
        getJoinSplitDep(dependencies(1)))
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  private def internalCompute[V, W](leftIter: Iterator[(K, V)], rightIter: Iterator[(K, W)])
  : Iterator[(K, (Iterator[V], Iterator[W]))] = {
    new Iterator[(K, (Iterator[V], Iterator[W]))] {

      var leftNext: Option[(K, V)] = None
      var rightNext: Option[(K, W)] = None

      def hasNext: Boolean = {
        if (leftNext.isDefined) {
          true
        } else if (leftIter.hasNext) {
          leftNext = Some(leftIter.next())
          true
        } else {
          false
        }
      }

      def next(): (K, (Iterator[V], Iterator[W])) = {
        val currentKey: K = leftNext.get._1
        val leftIterResult = new Iterator[V] {
          def hasNext: Boolean = {
            var flag: Boolean = true
            if (leftNext.isDefined) {
              flag = true
            } else if (leftIter.hasNext) {
              leftNext = Some(leftIter.next())
              if (keyOrdering.compare(currentKey, leftNext.get._1) == 0) {
                flag = true
              } else {
                flag = false
              }
            } else {
              flag = false
            }
            flag
          }

          def next(): V = {
            val leftVal = leftNext.get._2
            leftNext = None
            leftVal
          }
        }

        val rightIterResult = new Iterator[W] {
          def hasNext: Boolean = {
            var flag: Boolean = true
            if (! rightNext.isDefined) {
              if (rightIter.hasNext) {
                rightNext = Some(rightIter.next())
              } else {
                flag = false
              }
            }

            if (flag) {
              var comp = keyOrdering.compare(currentKey, rightNext.get._1)
              while (comp > 0 && flag) {
                if (rightIter.hasNext) {
                  rightNext = Some(rightIter.next())
                  comp = keyOrdering.compare(currentKey, rightNext.get._1)
                } else {
                  flag = false
                }
              }
              if (comp == 0) {
                flag = true
              } else {
                flag = false
              }
            }
            flag
          }

          def next(): W = {
            val rightVal = rightNext.get._2
            rightNext = None
            rightVal
          }
        }
        (currentKey, (leftIterResult, rightIterResult))
      }
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(K, PAIR)] = {
    val split = s.asInstanceOf[JoinPartition]
    val leftIter = SparkEnv.get.shuffleManager
      .getReader(split.left.handle, split.index, split.index + 1, context)
      .read().asInstanceOf[Iterator[(K, L)]]
    val rightIter = SparkEnv.get.shuffleManager
      .getReader(split.right.handle, split.index, split.index + 1, context)
      .read().asInstanceOf[Iterator[(K, R)]]
    val mergeIterators: Iterator[(K, (Iterator[L], Iterator[R]))] =
      if (join.joinType == JoinType.RIGHTOUTER) {
        internalCompute[R, L](rightIter, leftIter).map{
          case (key, pair) => (key, (pair._2, pair._1))
        }
      } else {
        internalCompute[L, R](leftIter, rightIter)
      }
    join.flatten(mergeIterators)
  }
}
