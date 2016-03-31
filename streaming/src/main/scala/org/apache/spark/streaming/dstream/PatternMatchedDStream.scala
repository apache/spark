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
import org.apache.spark.streaming.{Duration, Interval, Time}
import org.apache.spark.{Accumulator, Partitioner, SparkContext}

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
class PatternMatchedDStream[T: ClassTag](
    parent: DStream[T],
    pattern: scala.util.matching.Regex,
    predicates: Map[String, (T, WindowState) => Boolean],
    _windowDuration: Duration,
    _slideDuration: Duration
    ) extends DStream[List[T]](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of PatternMatchedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of PatternMatchedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  private def maxTStream(in: DStream[(Long, (T, Option[T]))]) : DStream[(Long, T)] = {
    in.reduce((e1, e2) => {
      if (e1._1 > e2._1) {
        e1
      } else {
        e2
      }
    }).map(max => (max._1, max._2._1))
  }

  private def keyWithPreviousEvent(in: DStream[T]) : DStream[(Long, (T, Option[T]))] = {
    val keyedRdd = in.transform((rdd, time) => {
      val offset = System.nanoTime()
      rdd.zipWithIndex().map(i => {
        (i._2 + offset, i._1)})
    })
    val previousEventRdd = keyedRdd.map(x => (x._1 + 1, x._2) )
    keyedRdd.leftOuterJoin(previousEventRdd).transform(rdd => {rdd.sortByKey()} )
  }

  val sortedprevMappedStream = keyWithPreviousEvent(parent)
  val maxKeyStream = maxTStream(sortedprevMappedStream)

  super.persist(StorageLevel.MEMORY_ONLY_SER)
  sortedprevMappedStream.persist(StorageLevel.MEMORY_ONLY_SER)
  maxKeyStream.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(sortedprevMappedStream, maxKeyStream)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def persist(storageLevel: StorageLevel): DStream[List[T]] = {
    super.persist(storageLevel)
    sortedprevMappedStream.persist(storageLevel)
    maxKeyStream.persist(storageLevel)
    this
  }

  override def checkpoint(interval: Duration): DStream[List[T]] = {
    super.checkpoint(interval)
    this
  }

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  val applyRegex = (rdd: RDD[(Long, (String, T))]) => {
    val patternCopy = pattern
    import scala.collection.mutable.ListBuffer
    val stream = rdd.groupBy(_ => "all")
      .map(_._2)
      .map(x => {
        val it = x.iterator
        val builder = new scala.collection.mutable.StringBuilder()
        val map = scala.collection.mutable.HashMap[Long, Tuple2[Long, T]] ()
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
      val o = ListBuffer[List[T]] ()
      for (one <- it) {
        var len = 0
        var ctr = 0
        val list = ListBuffer[T] ()
        one.split(" ").map(sub => {
          val id = y._2.get(it.start + len)
          list += id.get._2
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

  private def getLastMaxKey(rdds: Seq[RDD[(Long, T)]]): Option[T] = {
    if (rdds.length > 0) {
      (0 to rdds.size-1).map(i => {
        if (!rdds(rdds.size-1-i).isEmpty()) {
          return Some(rdds(rdds.size-1-i).reduce((e1, e2) => { if (e1._1 > e2._1) e1 else e2 })._2)
        }
      })
    }
    None: Option[T]
  }

  override def compute(validTime: Time): Option[RDD[List[T]]] = {
    val predicatesCopy = predicates
    val patternCopy = pattern;
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val previousWindow = currentWindow - slideDuration


    // first get the row with max key in the last window and broadcast it
    val oldMaxStreamRDDs = maxKeyStream.slice(previousWindow)
    val brdcst = ssc.sparkContext.broadcast(getLastMaxKey(oldMaxStreamRDDs))

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

      val first = newRDD.first()
      val taggedRDD = newRDD.map(x => {
        var isMatch = false
        var matchName = "NAN"
        for (predicate <- predicatesCopy if !isMatch) {
          val prev = x._2._2 match {
            case Some(i) => i.asInstanceOf[T]
            case None => {
              if (first._1 == x._1) { // this is the first in window
                first._2._1
              }
              else { // this is a batchInterval boundary
                brdcst.value.get
              }
            }
          }
          isMatch = predicate._2(x._2._1, WindowState(first._2._1, x._2._2.getOrElse(prev)))
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

