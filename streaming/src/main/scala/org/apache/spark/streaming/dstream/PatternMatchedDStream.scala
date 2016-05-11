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
import org.apache.spark.rdd.{PartitionerAwareUnionRDD, RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Duration, Interval, Time}
import org.apache.spark.{HashPartitioner, Accumulator, Partitioner, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by agsachin on 26/02/16.
  */

object PatternMatchedDStream {
  private var tracker: Accumulator[Long] = null

  def getInstance(sc: SparkContext): Accumulator[Long] = {
    if (tracker == null) {
      synchronized {
        if (tracker == null) {
          tracker = sc.accumulator(0L, "PatternMatchedDStream")
        }
      }
    }
    tracker
  }
}

case class WindowState(first: Any, prev: Any) {}

private[streaming]
class PatternMatchedDStream[K: ClassTag, V: ClassTag](
    parent: DStream[(K, V)],
    pattern: scala.util.matching.Regex,
    predicates: Map[String, (V, WindowState) => Boolean],
    _windowDuration: Duration,
    _slideDuration: Duration,
    partitioner: Partitioner)
    (implicit ord: Ordering[K]
  ) extends DStream[List[V]](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of PatternMatchedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of PatternMatchedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  var brdcst = ssc.sparkContext.broadcast((0L, None: Option[V]))

  private def keyWithPreviousEvent(in: DStream[(K, V)]) : DStream[(Long, (V, Option[V]))] = {
    val sortedStream = in.transform(rdd => rdd.sortByKey(true, rdd.partitions.size))
      .map{case(k, v) => v}
    val keyedRdd = sortedStream.transform((rdd, time) => {
      val brdcstCopy = brdcst
      rdd.zipWithIndex().map(i => {
        (i._2 + brdcstCopy.value._1 + 1, i._1)})
    })

    val previousEventRdd = keyedRdd.map(x => { (x._1 + 1, x._2)})
    keyedRdd.leftOuterJoin(previousEventRdd, partitioner).transform(rdd => rdd.sortByKey())
      .transform(rdd => {
        val brdcstCopy = brdcst
        rdd.map(x => {
          if (x._2._2 == None) {
            (x._1, (x._2._1, brdcstCopy.value._2))
          }
          else { x }
        })
      })
  }
  val sortedprevMappedStream = keyWithPreviousEvent(parent)

  private def getMaxIdAndElemStream(in: DStream[(Long, (V, Option[V]))]) : DStream[(Long, V)] = {
    in.reduce((e1, e2) => {
      if (e1._1 > e2._1) {
        e1
      } else {
        e2
      }
    }).map(max => (max._1, max._2._1))
  }
  val maxIdAndElemStream = getMaxIdAndElemStream(sortedprevMappedStream)

  super.persist(StorageLevel.MEMORY_ONLY_SER)
  sortedprevMappedStream.persist(StorageLevel.MEMORY_ONLY_SER)
  maxIdAndElemStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(sortedprevMappedStream, maxIdAndElemStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def persist(storageLevel: StorageLevel): DStream[List[V]] = {
    super.persist(storageLevel)
    sortedprevMappedStream.persist(storageLevel)
    maxIdAndElemStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[List[V]] = {
    super.checkpoint(interval)
    this
  }

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  import scala.collection.mutable.ListBuffer

  /**
    * This algo applies the regex within each partition rather than globally.
    * Benefits may include better latency when NO matches across partition boundaries.
    * However to account for the latter, need a separate algo.
    */
  val applyRegexPerPartition = (rdd: RDD[(Long, (String, V))]) => {
    val patternCopy = pattern
    val temp = rdd.mapPartitionsWithIndex{ (index, itr) => itr.toList.map(
      x => (index, x)).iterator  }
    val zero = new Aggregator[V]()
    val t = temp.aggregateByKey(zero)(
      (set, v) => set.append(v._2._1, v._2._2),
      (set1, set2) => set1 ++= set2)

    t.flatMap(x => {
      val it = patternCopy.findAllIn(x._2.builder.toString())
      val o = ListBuffer[List[V]]()
      for (one <- it) {
        var len = 0
        var ctr = 0
        val list = ListBuffer[V]()
        one.split(" ").map(sub => {
          val id = x._2.map.get(it.start + len)
          list += id.get
          // Todo not implemented window break when match found.
          len += sub.length + 1
        })
        o += list.toList
      }
      o.toList
    })
  }

  /**
    * This algo applies the regex globally by simply moving ALL
    * data to a single partition.
    */
  val applyRegex = (rdd: RDD[(Long, (String, V))]) => {
    val patternCopy = pattern
    val stream = rdd.groupBy(_ => "all")
      .map(_._2)
      .map(x => {
        val it = x.iterator
        val builder = new scala.collection.mutable.StringBuilder()
        val map = scala.collection.mutable.HashMap[Long, Tuple2[Long, V]] ()
        var curLen = 1L
        while (it.hasNext) {
          val i = it.next()
          builder.append(" " + i._2._1)
          map.put(curLen, (i._1, i._2._2))
          curLen += i._2._1.length + 1
        }
        (builder.toString(), map)
      })
    val tracker = PatternMatchedDStream.getInstance(this.ssc.sparkContext)

    stream.flatMap(y => {
      val it = patternCopy.findAllIn(y._1)
      val o = ListBuffer[List[V]] ()
      for (one <- it) {
        var len = 0
        var ctr = 0
        val list = ListBuffer[V] ()
        one.split(" ").map(sub => {
          val id = y._2.get(it.start + len)
          list += id.get._2
          // publish last seen match, so that dont repeat in a sliding window
          if (tracker.toString.toLong < id.get._1) {
            tracker.setValue(id.get._1)
          }
          len += sub.length + 1
        })
        o += list.toList
      }
      o.toList
    })
  }

  private def getLastMaxIdAndElem(rdds: Seq[RDD[(Long, V)]]): (Long, Option[V]) = {
    if (rdds.length > 0) {
      (0 to rdds.size-1).map(i => {
        if (!rdds(rdds.size-1-i).isEmpty()) {
          val r = rdds(rdds.size-1-i).reduce((e1, e2) => { if (e1._1 > e2._1) e1 else e2 })
          return (r._1, Some(r._2))
        }
      })
    }
    (0L, None: Option[V])
  }

  override def compute(validTime: Time): Option[RDD[List[V]]] = {
    val predicatesCopy = predicates
    val patternCopy = pattern;
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val previousWindow = currentWindow - slideDuration


    // first get the row with max key in the last window and broadcast it
    // IMP: Dont touch current window till after this broadcast
    val oldMaxStreamRDDs = maxIdAndElemStream.slice(previousWindow)
    brdcst = ssc.sparkContext.broadcast(getLastMaxIdAndElem(oldMaxStreamRDDs))

    val rddsInWindow = sortedprevMappedStream.slice(currentWindow)

    val windowRDD = if (rddsInWindow.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, rddsInWindow)
    } else {
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, rddsInWindow)
    }

    if (!windowRDD.isEmpty()) {
      // we dont want to report old matched patterns in current window if any
      // so skip to the ID maintained in tracker
      val tracker = PatternMatchedDStream.getInstance(this.ssc.sparkContext)
      val shift = if (tracker.toString.toLong > 0L) {
        val copy = tracker.toString.toLong
        tracker.setValue(0L)
        copy
      } else {
        0L
      }
      val newRDD = windowRDD.filter(x => x._1 > shift)

      // loop through and get the predicate string for each event.
      val first = newRDD.first()
      val taggedRDD = newRDD.map(x => {
        var isMatch = false
        var matchName = "NAN"
        for (predicate <- predicatesCopy if !isMatch) {
          isMatch = predicate._2(x._2._1, WindowState(first._2._1, x._2._2.getOrElse(first._2._1)))
          if (isMatch) {
            matchName = predicate._1
          }
        }
        (x._1, (matchName, x._2._1))
      })
      Some(applyRegex(taggedRDD))
    }
    else {
      None
    }
  }
}

private[streaming]
class Aggregator[T: ClassTag](
    val builder: scala.collection.mutable.StringBuilder,
    val map: scala.collection.mutable.HashMap[Long, T]
  ) extends Serializable {

  var curLen = 1L

  def this() = { this( new scala.collection.mutable.StringBuilder(),
    scala.collection.mutable.HashMap[Long, T]() ) }

  def append(in: String, elem: T ): Aggregator[T] = {
    builder.append(" " + in)
    map.put(curLen, elem)
    curLen += in.length + 1
    this
  }

  def ++=(other: Aggregator[T]): Aggregator[T] = {
    builder.append(other.builder.toString())
    map ++= other.map
    new Aggregator(builder, map)
  }
}