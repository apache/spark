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

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.util.Utils

/**
 * Class representing partitions of PartitionerAwareUnionRDD, which maintains the list of
 * corresponding partitions of parent RDDs.
 */
private[spark]
class PartitionerAwareUnionRDDPartition(
    @transient val rdds: Seq[RDD[_]],
    val idx: Int
  ) extends Partition {
  var parents = rdds.map(_.partitions(idx)).toArray

  override val index = idx
  override def hashCode(): Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = rdds.map(_.partitions(index)).toArray
    oos.defaultWriteObject()
  }
}

/**
 * Class representing an RDD that can take multiple RDDs partitioned by the same partitioner and
 * unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
 * will be unified to a single RDD with p partitions and the same partitioner. The preferred
 * location for each partition of the unified RDD will be the most common preferred location
 * of the corresponding partitions of the parent RDDs. For example, location of partition 0
 * of the unified RDD will be where most of partition 0 of the parent RDDs are located.
 */
private[spark]
class PartitionerAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]]
  ) extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.length > 0)
  require(rdds.flatMap(_.partitioner).toSet.size == 1,
    "Parent RDDs have different partitioners: " + rdds.flatMap(_.partitioner))

  override val partitioner = rdds.head.partitioner

  override def getPartitions: Array[Partition] = {
    val numPartitions = partitioner.get.numPartitions
    (0 until numPartitions).map(index => {
      new PartitionerAwareUnionRDDPartition(rdds, index)
    }).toArray
  }

  // Get the location where most of the partitions of parent RDDs are located
  override def getPreferredLocations(s: Partition): Seq[String] = {
    logDebug("Finding preferred location for " + this + ", partition " + s.index)
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    val locations = rdds.zip(parentPartitions).flatMap {
      case (rdd, part) => {
        val parentLocations = currPrefLocs(rdd, part)
        logDebug("Location of " + rdd + " partition " + part.index + " = " + parentLocations)
        parentLocations
      }
    }
    val location = if (locations.isEmpty) {
      None
    } else  {
      // Find the location that maximum number of parent partitions prefer
      Some(locations.groupBy(x => x).maxBy(_._2.length)._1)
    }
    logDebug("Selected location for " + this + ", partition " + s.index + " = " + location)
    location.toSeq
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[PartitionerAwareUnionRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }

  // Get the *current* preferred locations from the DAGScheduler (as opposed to the static ones)
  private def currPrefLocs(rdd: RDD[_], part: Partition): Seq[String] = {
    rdd.context.getPreferredLocs(rdd, part.index).map(tl => tl.host)
  }
}
