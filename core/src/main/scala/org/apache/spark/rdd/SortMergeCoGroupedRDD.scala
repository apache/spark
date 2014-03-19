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

import scala.collection.mutable

import org.apache.spark.InterruptibleIterator
import org.apache.spark.Partition
import org.apache.spark.Partitioner
import org.apache.spark.SparkEnv
import org.apache.spark.TaskContext
import org.apache.spark.serializer.Serializer

class SortMergeCoGroupedRDD[K: Ordering](
    @transient rdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends CoGroupedRDD(rdds, part) {

  private type CoGroup = mutable.ArrayBuffer[Any]
  private type CoGroupCombiner = Seq[CoGroup]

  private var serializer: Serializer = null

  override def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    super.setSerializer(serializer)
    this.serializer = serializer
    this
  }

  override def compute(s: Partition, context: TaskContext): Iterator[(K, CoGroupCombiner)] = {

    val split = s.asInstanceOf[CoGroupPartition]

    val itrs = for (dep <- split.deps) yield {
      dep match {
        case NarrowCoGroupSplitDep(rdd, _, itsSplit) => {
          // Read them from the parent
          rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]]
        }
        case ShuffleCoGroupSplitDep(shuffleId) => {
          // Read map outputs of shuffle
          val fetcher = SparkEnv.get.sortMergeShuffleFetcher
          val ser = Serializer.getSerializer(serializer)
          fetcher.fetch(shuffleId, split.index, context, ser)
        }
      }
    }.buffered

    new InterruptibleIterator(
      context,
      Iterator.continually {
        ((None: Option[K]) /: itrs) {
          case (opt, itr) if itr.hasNext =>
            opt.map { key =>
              implicitly[Ordering[K]].min(key, itr.head._1)
            }.orElse(Some(itr.head._1))
          case (opt, _) => opt
        }
      }.takeWhile(_.isDefined).map(_.get).map { key =>
        (key -> itrs.map { itr =>
          Iterator.continually {
            if (itr.hasNext && implicitly[Ordering[K]].equiv(itr.head._1, key)) {
              Some(itr.next._2)
            } else None
          }.takeWhile(_.isDefined).map(_.get).to[mutable.ArrayBuffer]
        })
      })
  }
}
