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

import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
 * An RDD that has no partitions and no elements.
 *
 * @param numPartitions optional number of (empty) partitions within the empty RDD
 */
private[spark] class EmptyRDD[T: ClassTag](sc: SparkContext, numPartitions: Int = 0)
  extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] =
    Array.tabulate(numPartitions)(index => new EmptyPartition(index))

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }
}

private[spark] class EmptyPartition(val index: Int) extends Partition {

  // hashCode defined in parent

  override def equals(other: Any): Boolean = other match {
    case ep: EmptyPartition => ep.index == this.index
    case _ => false
  }

}
