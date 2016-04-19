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

package org.apache.spark.streaming.dstream

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{CoGroupedRDD, PartitionerAwareUnionRDD, RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, Time}
import org.apache.spark.{Accumulator, Partitioner, SparkContext}



import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag



private[streaming]
class KeyedPatternMatchDStream[K: ClassTag, V: ClassTag](
    parent: DStream[(K, V)],
    pattern: scala.util.matching.Regex,
    predicates: Map[String, (V, WindowState) => Boolean],
    valuesOrderedBy: V => Long,
    _windowDuration: Duration,
    _slideDuration: Duration,
    partitioner: Partitioner
    ) extends DStream[(K, List[V])](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of PatternMatchedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of PatternMatchedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )


  private def aggregateByKey(in: DStream[(K, V)]) : DStream[(K, KeyedWithPrev[V])] = {
    val aggregated = in.transform(rdd => {
      val zero = new KeyedAggregator[V]()
      rdd.aggregateByKey(zero)(
        (set, v) => set.append(v),
        (set1, set2) => set1 ++= set2)
    })
    aggregated.transform(rdd => {
      val valuesOrderedByCopy = valuesOrderedBy
      rdd.mapValues(x => {
        x.sortedWithPrev(valuesOrderedByCopy)
      })
    })
  }

  val keyedWithPrevStream = aggregateByKey(parent)

  super.persist(StorageLevel.MEMORY_ONLY_SER)
  keyedWithPrevStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(keyedWithPrevStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def persist(storageLevel: StorageLevel): DStream[(K, List[V])] = {
    super.persist(storageLevel)
    keyedWithPrevStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[(K, List[V])] = {
    super.checkpoint(interval)
    this
  }

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[(K, List[V])]] = {

    val predicatesCopy = predicates
    val patternCopy = pattern;
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val previousWindow = currentWindow - slideDuration

    val newRDDs = keyedWithPrevStream.slice(previousWindow.endTime + parent.slideDuration,
        currentWindow.endTime)

    val lastValidRDDs =
      keyedWithPrevStream.slice(currentWindow.beginTime, previousWindow.endTime)


    // Make the list of RDDs that needs to cogrouped together for pattern matching
    val allRDDs = new ArrayBuffer[RDD[(K, KeyedWithPrev[V])]]()  ++= lastValidRDDs ++= newRDDs


    // Cogroup the reduced RDDs and merge the reduced values
    val cogroupedRDD = new CoGroupedRDD[K](allRDDs.toSeq.asInstanceOf[Seq[RDD[(K, _)]]],
      partitioner)

    val totalValues = lastValidRDDs.size + newRDDs.size

    val mergeValues = (arrayOfValues:
        Array[org.apache.spark.util.collection.CompactBuffer[KeyedWithPrev[V]]]) => {

      val compactBuffervalues =
        (0 to totalValues-1).map(i => arrayOfValues(i)).filter(!_.isEmpty).map(_.head)
      val values = compactBuffervalues.flatMap(y => y.list)

      import scala.collection.mutable.ListBuffer
      if (!values.isEmpty) {
        val minVal = values(0)
        val pattern = values.map(x => {
          var isMatch = false
          var matchName = "NAN"
          for (predicate <- predicatesCopy if !isMatch) {
            isMatch = predicate._2(x._1, WindowState(minVal._1, x._2.getOrElse(minVal._1)))
            if (isMatch) {
              matchName = predicate._1
            }
          }
          (matchName, x._1)
        })

        val builder = new scala.collection.mutable.StringBuilder()
        val map = scala.collection.mutable.HashMap[Long, V]()
        var curLen = 1L
        for (i <- pattern.iterator) {
          builder.append(" " + i._1)
          map.put(curLen, i._2)
          curLen += i._1.length + 1
        }

        val matchIt = patternCopy.findAllIn(builder.toString())
        val o = ListBuffer[List[V]]()
        for (one <- matchIt) {
          var len = 0
          var ctr = 0
          val list = ListBuffer[V]()
          one.split(" ").map(sub => {
            val id = map.get(matchIt.start + len)
            list += id.get
            len += sub.length + 1
          })
          o += list.toList
        }
        o.toList.asInstanceOf[List[V]]
      }
      else {
        ListBuffer[List[V]]().toList.asInstanceOf[List[V]]
      }
    }

    val mergedValuesRDD = cogroupedRDD.asInstanceOf[RDD[(K,
      Array[org.apache.spark.util.collection.CompactBuffer[KeyedWithPrev[V]]])]]
      .mapValues(mergeValues)

    Some(mergedValuesRDD.filter{case(k, v) => !v.isEmpty })
  }
}

import scala.collection.mutable.ListBuffer

private[streaming]
case class KeyedWithPrev[V: ClassTag](val list: List[(V, Option[V])])

private[streaming]
class KeyedAggregator[V: ClassTag](val list: ListBuffer[V]) extends Serializable {

  def this() = { this(ListBuffer[V]() ) }

  def append(in: V): KeyedAggregator[V] = {
    list += in
    this
  }

  def ++=(other: KeyedAggregator[V]): KeyedAggregator[V] = {
    new KeyedAggregator(list ++= other.list)
  }

  def sortedWithPrev(f: V => Long) : KeyedWithPrev[V] = {
    val sortedList = list.sortBy(e => f(e))
    val it = sortedList.iterator

    var start = true
    val n = sortedList.map(x => {
      (x, if (start) {
        start = false
        None: Option[V]
      } else {
        Some(it.next())
      })
    })
    KeyedWithPrev[V](n.toList)
  }
}

