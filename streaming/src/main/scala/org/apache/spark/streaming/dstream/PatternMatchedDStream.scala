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

case class WindowMetric(first: Any, prev: Any) {}

private[streaming]
class PatternMatchedDStream[T: ClassTag](
                                      parent: DStream[T],
                                      pattern: scala.util.matching.Regex,
                                      predicates: Map[String, (T, WindowMetric) => Boolean],
                                      _windowDuration: Duration,
                                      _slideDuration: Duration,
                                      partitioner: Partitioner
                                      ) extends DStream[List[T]](parent.ssc) {

  require(_windowDuration.isMultipleOf(parent.slideDuration),
    "The window duration of ReducedWindowedDStream (" + _windowDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  require(_slideDuration.isMultipleOf(parent.slideDuration),
    "The slide duration of ReducedWindowedDStream (" + _slideDuration + ") " +
      "must be multiple of the slide duration of parent DStream (" + parent.slideDuration + ")"
  )

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  def windowDuration: Duration = _windowDuration

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = _slideDuration

  override val mustCheckpoint = true

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  val applyRegex = (rdd: RDD[(Long, (String, T))]) => {
    val patternCopy = pattern
    import scala.collection.mutable.ListBuffer
    val stream = rdd.groupBy(_ => "all")
      .map(_._2)
      .map(x => {
        val it = x.iterator
        val builder = new scala.collection.mutable.StringBuilder()
        val map = scala.collection.mutable.HashMap[Long, Tuple2[Long,T]] ()
        var curLen = 1L
        while (it.hasNext) {
          val i = it.next()
          builder.append(" " + i._2._1)
          map.put(curLen, (i._1, i._2._2))
          curLen += i._2._1.length + 1
        }
        //println(builder.toString(), map)
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
          //println("id="+id.get._1)
          if (tracker.toString.toLong < id.get._1) {
            tracker.setValue(id.get._1)
          }
          len += sub.length + 1
        })
        o += list.toList
      }
      // o.toList.foreach(println)
      o.toList
    })
  }

  override def compute(validTime: Time): Option[RDD[List[T]]] = {
    val predicatesCopy = predicates
    val patternCopy = pattern;
    val currentWindow = new Interval(validTime - windowDuration + parent.slideDuration, validTime)
    val previousWindow = currentWindow - slideDuration

    val tracker = PatternMatchedDStream.getInstance(this.ssc.sparkContext)

    val oldRDDs =
      parent.slice(previousWindow.beginTime, currentWindow.beginTime - parent.slideDuration)

    val oldRDD = if (oldRDDs.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, oldRDDs)
    } else {
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, oldRDDs)
    }

    val increment = oldRDD.count()

    val rddsInWindow = parent.slice(currentWindow)
    val windowRDD = if (rddsInWindow.flatMap(_.partitioner).distinct.length == 1) {
      logDebug("Using partition aware union for windowing at " + validTime)
      new PartitionerAwareUnionRDD(ssc.sc, rddsInWindow)
    } else {
      logDebug("Using normal union for windowing at " + validTime)
      new UnionRDD(ssc.sc, rddsInWindow)
    }

    if (!windowRDD.isEmpty()) {
      //println(windowRDD.count)
      val first = windowRDD.first()
      var shift = 0L
      if (tracker.toString.toLong > 0L) {
        shift = tracker.toString.toLong - increment
        tracker.setValue(0L)
      }

      val zippedRDD = windowRDD.zipWithIndex().map(x => (x._2, x._1)).sortByKey()
      val selectedRDD = zippedRDD.filter(x => x._1 > shift)

      val prevKeyRdd = selectedRDD.map(i => {
        (i._1 + 1, i._2)
      })

      val sortedprevMappedRdd = selectedRDD.leftOuterJoin(prevKeyRdd).sortByKey()

      val taggedRDD = sortedprevMappedRdd.map(x => {
        var isMatch = false
        var matchName = "NAN"
        for (predicate <- predicatesCopy if !isMatch) {
          isMatch = predicate._2(x._2._1, WindowMetric(first, x._2._2.getOrElse(first)))
          if (isMatch) {
            matchName = predicate._1
          }
        }
        // println(x._1, (matchName, x._2._1))
        (x._1, (matchName, x._2._1))
      })

      Some(applyRegex(taggedRDD))
    }
    else
      None
  }
}