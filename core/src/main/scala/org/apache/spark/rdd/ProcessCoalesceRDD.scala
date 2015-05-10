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

import java.io.ObjectOutputStream
import java.net.InetAddress

import org.apache.spark.{NarrowDependency, Dependency, TaskContext, Partition}
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, HostTaskLocation}

import scala.reflect.ClassTag

/**
 * An RDD which can combine small cached partition in the same executor
 * into only one partition to reduce the use of cpu resource.
 */
private[spark] class ProcessCoalesceRDD [T: ClassTag](@transient var prev: RDD[T])
  extends RDD[T](prev.context, Nil) {
  // Nil since we implement getDependencies

  override def getPartitions: Array[Partition] = {
    // TODO: check boundary
    val parentLocs = prev.partitions.map(p =>
      (p.index, prev.sparkContext.getPreferredLocs(prev, p.index)(0)))

    val parentLocHostMap = parentLocs.map { place =>
      place._2 match {
        case location: HostTaskLocation =>
          (place._1, InetAddress.getByName(location.host).getHostName)
        case location: ExecutorCacheTaskLocation =>
          (place._1, "Executor_" + location.host + "_" + location.executorId)
      }
    }.toMap

    val localtionList: Iterable[Array[Int]] = parentLocs.groupBy(pp =>
      pp._2.toString()).map(_._2.map(_._1))
    val result = localtionList.zipWithIndex.map {
      case (parentParts, i) => new ProcessCoalescePartition(i, prev,
        parentParts, parentLocHostMap(parentParts(0)))
    }.toArray
    logDebug(s"ProcessCoalesceRDD.getPartitions=${result.toList}")
    result.asInstanceOf[Array[Partition]]
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    partition.asInstanceOf[ProcessCoalescePartition].parents.iterator.flatMap {
      parentPartition =>
        firstParent[T].iterator(parentPartition, context)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[ProcessCoalescePartition].parentsIndices
    })
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  /**
   * Returns the preferred machine and the specific executor for the partition.
   * @param partition
   * @return the executor most preferred by split
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val loc = partition.asInstanceOf[ProcessCoalescePartition].preferredLocation
    logDebug(s"ProcessCoalesceRDD.getPreferredLocations called for" +
      s" ${partition.index}, ret=$loc")
    List(loc)
  }
}

/**
 * A location where a task should run. This can either be a host or a (host, executorID) pair.
 */
private[spark] case class ProcessCoalescePartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int],
    @transient preferredLocation: String = ""
    ) extends Partition {
  var parents: Seq[Partition] = parentsIndices.map(rdd.partitions(_))

  private def writeObject(oos: ObjectOutputStream): Unit = {
    // Update the reference to parent partition at the time of task serialization
    parents = parentsIndices.map(rdd.partitions(_))
    oos.defaultWriteObject()
  }

  override def toString = "ProcessCoalescePartition"  +
    s"($index, ${parentsIndices.toList}, $preferredLocation)"
}
