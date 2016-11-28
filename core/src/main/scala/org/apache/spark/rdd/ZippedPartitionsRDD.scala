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

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.util.Utils

private[spark] class ZippedPartitionsPartition(
    private var idx: Int,
    @transient private val rdds: Seq[RDD[_]],
    @transient val preferredLocations: Seq[String])
  extends Partition with KryoSerializable {

  override def index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))
  def partitions: Seq[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    oos.defaultWriteObject()
  }

  override def write(kryo: Kryo, output: Output): Unit = {
    // Update the reference to parent split at the time of task serialization
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    output.writeVarInt(idx, true)
    output.writeVarInt(partitionValues.length, true)
    for (p <- partitionValues) {
      kryo.writeClassAndObject(output, p)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    idx = input.readVarInt(true)
    var numPartitions = input.readVarInt(true)
    val partitionBuilder = Seq.newBuilder[Partition]
    while (numPartitions > 0) {
      partitionBuilder += kryo.readClassAndObject(input).asInstanceOf[Partition]
      numPartitions -= 1
    }
    partitionValues = partitionBuilder.result()
  }
}

private[spark] abstract class ZippedPartitionsBaseRDD[V: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[_]],
    preservesPartitioning: Boolean = false)
  extends RDD[V](sc, rdds.map(x => new OneToOneDependency(x))) {

  override val partitioner =
    if (preservesPartitioning) firstParent[Any].partitioner else None

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}")
    }
    Array.tabulate[Partition](numParts) { i =>
      val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i)))
      // Check whether there are any hosts that match all RDDs; otherwise return the union
      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
      new ZippedPartitionsPartition(i, rdds, locs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[ZippedPartitionsPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

private[spark] class ZippedPartitionsRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    f = null
  }
}

private[spark] class ZippedPartitionsRDD3
  [A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    f = null
  }
}

private[spark] class ZippedPartitionsRDD4
  [A: ClassTag, B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag](
    sc: SparkContext,
    var f: (Iterator[A], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V],
    var rdd1: RDD[A],
    var rdd2: RDD[B],
    var rdd3: RDD[C],
    var rdd4: RDD[D],
    preservesPartitioning: Boolean = false)
  extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2, rdd3, rdd4), preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    f(rdd1.iterator(partitions(0), context),
      rdd2.iterator(partitions(1), context),
      rdd3.iterator(partitions(2), context),
      rdd4.iterator(partitions(3), context))
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    rdd4 = null
    f = null
  }
}
