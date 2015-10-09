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

package org.apache.spark.sql.execution

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark.util.Utils
import org.apache.spark.{TaskContext, Partition, OneToOneDependency, SparkContext}
import org.apache.spark.rdd.{MapPartitionsWithPreparationRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow

private[spark] class CoPartitionedPartition(
    idx: Int,
    @transient private val rdds: Array[RDD[InternalRow]],
    @transient val preferredLocations: Array[String])
  extends Partition {

  override val index: Int = idx
  var partitionValues = rdds.map(rdd => rdd.partitions(idx))
  def partitions: Array[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    partitionValues = rdds.map(rdd => rdd.partitions(idx))
    oos.defaultWriteObject()
  }
}

class CoPartitionedRDD(
    sc: SparkContext,
    var f: (Array[Iterator[InternalRow]]) => Iterator[InternalRow],
    var rdds: Array[RDD[InternalRow]])
  extends RDD[InternalRow](sc, rdds.map(x => new OneToOneDependency(x))) {

  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        "Can't co-partitioning RDDs with unequal numbers of partitions")
    }
    Array.tabulate[Partition](numParts) { i =>
      val prefs = rdds.map(rdd => rdd.preferredLocations(rdd.partitions(i)))
      // Check whether there are any hosts that match all RDDs; otherwise return the union
      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y)).toArray
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
      new CoPartitionedPartition(i, rdds, locs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[CoPartitionedPartition].preferredLocations
  }

  /**
   * Call the prepare method of every parent that has one.
   * This is needed for reserving execution memory in advance.
   */
  protected def tryPrepareParents(): Unit = {
    rdds.collect {
      case rdd: MapPartitionsWithPreparationRDD[_, _, _] => rdd.prepare()
    }
  }

  override def compute(s: Partition, context: TaskContext): Iterator[InternalRow] = {
    tryPrepareParents()
    val partitions = s.asInstanceOf[CoPartitionedPartition].partitions
    val iterators = new Array[Iterator[InternalRow]](rdds.length)
    var i = 0
    while (i < rdds.length) {
      iterators(i) = rdds(i).iterator(partitions(i), context)
      i += 1
    }
    f(iterators)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
    f = null
  }
}
