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

package org.apache.spark.rdd.util

import scala.collection.Set
import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.PeriodicCheckpointer


/**
 * This class helps with persisting and checkpointing RDDs.
 * Specifically, it automatically handles persisting and (optionally) checkpointing, as well as
 * unpersisting and removing checkpoint files.
 *
 * Users should call update() when a new RDD has been created,
 * before the RDD has been materialized.  After updating [[PeriodicRDDCheckpointer]], users are
 * responsible for materializing the RDD to ensure that persisting and checkpointing actually
 * occur.
 *
 * When update() is called, this does the following:
 *  - Persist new RDD (if not yet persisted), and put in queue of persisted RDDs.
 *  - Unpersist RDDs from queue until there are at most 3 persisted RDDs.
 *  - If using checkpointing and the checkpoint interval has been reached,
 *     - Checkpoint the new RDD, and put in a queue of checkpointed RDDs.
 *     - Remove older checkpoints.
 *
 * WARNINGS:
 *  - This class should NOT be copied (since copies may conflict on which RDDs should be
 *    checkpointed).
 *  - This class removes checkpoint files once later RDDs have been checkpointed.
 *    However, references to the older RDDs will still return isCheckpointed = true.
 *
 * Example usage:
 * {{{
 *  val (rdd1, rdd2, rdd3, ...) = ...
 *  val cp = new PeriodicRDDCheckpointer(2, sc)
 *  cp.update(rdd1)
 *  rdd1.count();
 *  // persisted: rdd1
 *  cp.update(rdd2)
 *  rdd2.count();
 *  // persisted: rdd1, rdd2
 *  // checkpointed: rdd2
 *  cp.update(rdd3)
 *  rdd3.count();
 *  // persisted: rdd1, rdd2, rdd3
 *  // checkpointed: rdd2
 *  cp.update(rdd4)
 *  rdd4.count();
 *  // persisted: rdd2, rdd3, rdd4
 *  // checkpointed: rdd4
 *  cp.update(rdd5)
 *  rdd5.count();
 *  // persisted: rdd3, rdd4, rdd5
 *  // checkpointed: rdd4
 * }}}
 *
 * @param checkpointInterval  RDDs will be checkpointed at this interval
 * @tparam T  RDD element type
 */
private[spark] class PeriodicRDDCheckpointer[T](
    checkpointInterval: Int,
    sc: SparkContext)
  extends PeriodicCheckpointer[RDD[T]](checkpointInterval, sc) {

  override protected def checkpoint(data: RDD[T]): Unit = data.checkpoint()

  override protected def isCheckpointed(data: RDD[T]): Boolean = data.isCheckpointed

  override protected def persist(data: RDD[T]): Unit = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      data.persist()
    }
  }

  override protected def unpersist(data: RDD[T]): Unit = data.unpersist(blocking = false)

  override protected def getCheckpointFiles(data: RDD[T]): Iterable[String] = {
    data.getCheckpointFile.map(x => x)
  }

  override protected def haveCommonCheckpoint(newData: RDD[T], oldData: RDD[T]): Boolean = {
    PeriodicRDDCheckpointer.haveCommonCheckpoint(Set(newData), Set(oldData))
  }

}

private[spark] object PeriodicRDDCheckpointer {

  def rddDeps(rdd: RDD[_]): Set[RDD[_]] = {
    val parents = new mutable.HashSet[RDD[_]]
    def visit(rdd: RDD[_]) {
      parents.add(rdd)
      rdd.dependencies.foreach(dep => visit(dep.rdd))
    }
    visit(rdd)
    parents
  }

  def haveCommonCheckpoint(rdds1: Set[_ <: RDD[_]], rdds2: Set[_ <: RDD[_]]): Boolean = {
    val deps1 = rdds1.foldLeft(new mutable.HashSet[RDD[_]]()) { (set, rdd) =>
      set ++= rddDeps(rdd)
    }
    val deps2 = rdds2.foldLeft(new mutable.HashSet[RDD[_]]()) { (set, rdd) =>
      set ++= rddDeps(rdd)
    }
    deps1.intersect(deps2).exists(_.isCheckpointed)
  }
}
