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

import scala.reflect.ClassTag

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Time}

private[streaming]
class StateDStream[K: ClassTag, V: ClassTag, S: ClassTag](
    parent: DStream[(K, V)],
    updateFunc: (Time, Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)],
    partitioner: Partitioner,
    preservePartitioning: Boolean,
    initialRDD: Option[RDD[(K, S)]]
  ) extends DStream[(K, S)](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY_SER)

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  private [this] def computeUsingPreviousRDD(
      batchTime: Time,
      parentRDD: RDD[(K, V)],
      prevStateRDD: RDD[(K, S)]) = {
    // Define the function for the mapPartition operation on cogrouped RDD;
    // first map the cogrouped tuple to tuples of required type,
    // and then apply the update function
    val updateFuncLocal = updateFunc
    val finalFunc = (iterator: Iterator[(K, (Iterable[V], Iterable[S]))]) => {
      val i = iterator.map { t =>
        val itr = t._2._2.iterator
        val headOption = if (itr.hasNext) Some(itr.next()) else None
        (t._1, t._2._1.toSeq, headOption)
      }
      updateFuncLocal(batchTime, i)
    }
    val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
    val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
    Some(stateRDD)
  }

  override def compute(validTime: Time): Option[RDD[(K, S)]] = {

    // Try to get the previous state RDD
    getOrCompute(validTime - slideDuration) match {

      case Some(prevStateRDD) =>    // If previous state RDD exists
        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>    // If parent RDD exists, then compute as usual
            computeUsingPreviousRDD (validTime, parentRDD, prevStateRDD)
          case None =>     // If parent RDD does not exist
            // Re-apply the update function to the old state RDD
            val updateFuncLocal = updateFunc
            val finalFunc = (iterator: Iterator[(K, S)]) => {
              val i = iterator.map(t => (t._1, Seq.empty[V], Option(t._2)))
              updateFuncLocal(validTime, i)
            }
            val stateRDD = prevStateRDD.mapPartitions(finalFunc, preservePartitioning)
            Some(stateRDD)
        }

      case None =>    // If previous session RDD does not exist (first input data)
        // Try to get the parent RDD
        parent.getOrCompute(validTime) match {
          case Some(parentRDD) =>   // If parent RDD exists, then compute as usual
            initialRDD match {
              case None =>
                // Define the function for the mapPartition operation on grouped RDD;
                // first map the grouped tuple to tuples of required type,
                // and then apply the update function
                val updateFuncLocal = updateFunc
                val finalFunc = (iterator: Iterator[(K, Iterable[V])]) => {
                  updateFuncLocal (validTime,
                    iterator.map (tuple => (tuple._1, tuple._2.toSeq, None)))
                }

                val groupedRDD = parentRDD.groupByKey(partitioner)
                val sessionRDD = groupedRDD.mapPartitions(finalFunc, preservePartitioning)
                // logDebug("Generating state RDD for time " + validTime + " (first)")
                Some (sessionRDD)
              case Some (initialStateRDD) =>
                computeUsingPreviousRDD(validTime, parentRDD, initialStateRDD)
            }
          case None => // If parent RDD does not exist, then nothing to do!
            // logDebug("Not generating state RDD (no previous state, no parent)")
            None
        }
    }
  }
}
