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
import org.apache.spark.storage.{StorageLevel, RDDBlockId}

private[spark] class CachePointRDDPartition(val index: Int) extends Partition {}

private[spark] class CachePointRDD[T: ClassTag](sc: SparkContext, numPartitions: Int) extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    Array.tabulate(numPartitions)(i => new CachePointRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val key = RDDBlockId(this.id, split.index)
    SparkEnv.get.blockManager.get(key) match {
      case Some(values) =>
        new InterruptibleIterator(context, values.asInstanceOf[Iterator[T]])

      case None =>
        new InterruptibleIterator(context, Iterator[T]())
    }
  }

  override def persist(newLevel: StorageLevel): this.type = {
    this
  }

  override def checkpoint() {
  }
}
