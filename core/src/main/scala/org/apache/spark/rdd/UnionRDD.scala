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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Dependency, RangeDependency, SparkContext, Partition, TaskContext}

import java.io.{ObjectOutputStream, IOException}

private[spark] class UnionPartition[T: ClassTag](idx: Int, rdd: RDD[T], splitIndex: Int)
  extends Partition {

  var split: Partition = rdd.partitions(splitIndex)

  def iterator(context: TaskContext) = rdd.iterator(split, context)

  def preferredLocations() = rdd.preferredLocations(split)

  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream) {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

class UnionRDD[T: ClassTag](
    sc: SparkContext,
    @transient var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdds.map(_.partitions.size).sum)
    var pos = 0
    for (rdd <- rdds; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.size)
      pos += rdd.partitions.size
    }
    deps
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] =
    s.asInstanceOf[UnionPartition[T]].iterator(context)

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()
}
