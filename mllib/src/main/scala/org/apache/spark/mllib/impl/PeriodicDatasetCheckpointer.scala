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

package org.apache.spark.mllib.impl

import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel


/**
 * This class helps with persisting and checkpointing Datasets.
 * Specifically, it automatically handles persisting and (optionally) checkpointing, as well as
 * unpersisting and removing checkpoint files.
 *
 * Users should call update() when a new Dataset has been created and before the Dataset has been
 * materialized. Because we call Dataset.checkout() with eager as true, it means it always performs
 * a RDD.count() after calling RDD.checkpoint() on the underlying RDD of the Dataset, so different
 * than [[PeriodicRDDCheckpointer]], after updating [[PeriodicDatasetCheckpointer]], users do not
 * need to materialize the Dataset to ensure that persisting and checkpointing actually occur.
 *
 * When update() is called, this does the following:
 *  - Persist new Dataset (if not yet persisted), and put in queue of persisted Datasets.
 *  - Unpersist Datasets from queue until there are at most 3 persisted Datasets.
 *  - If using checkpointing and the checkpoint interval has been reached,
 *     - Checkpoint the new Dataset, and put in a queue of checkpointed Datasets.
 *     - Remove older checkpoints.
 *
 * WARNINGS:
 *  - This class should NOT be copied (since copies may conflict on which Datasets should be
 *    checkpointed).
 *  - This class removes checkpoint files once later other Datasets have been checkpointed.
 *    However, references to the RDDs of the older Datasets will still return isCheckpointed = true.
 *
 * Example usage:
 * {{{
 *  val (ds1, ds2, ds3, ...) = ...
 *  val cp = new PeriodicDatasetCheckpointer(2, sc)
 *  cp.update(ds1)
 *  // persisted: rdd1
 *  cp.update(ds2)
 *  // persisted: ds1, ds2
 *  // checkpointed: ds2
 *  cp.update(ds3)
 *  // persisted: ds1, ds2, ds3
 *  // checkpointed: ds2
 *  cp.update(ds4)
 *  // persisted: ds2, ds3, ds4
 *  // checkpointed: ds4
 *  cp.update(ds5)
 *  // persisted: ds3, ds4, ds5
 *  // checkpointed: ds4
 * }}}
 *
 * @param checkpointInterval  Datasets will be checkpointed at this interval
 *
 * TODO: Move this out of MLlib?
 */
private[spark] class PeriodicDatasetCheckpointer(
    checkpointInterval: Int,
    sc: SparkContext)
  extends PeriodicCheckpointer[Dataset[_]](checkpointInterval, sc) {

  override protected def checkpoint(data: Dataset[_]): Unit = data.checkpoint(eager = true)

  override protected def isCheckpointed(data: Dataset[_]): Boolean = data.isCheckpoint

  override protected def persist(data: Dataset[_]): Unit = {
    if (data.storageLevel == StorageLevel.NONE) {
      data.persist()
    }
  }

  override protected def unpersist(data: Dataset[_]): Unit = data.unpersist()

  override protected def getCheckpointFiles(data: Dataset[_]): Iterable[String] = {
    data.queryExecution.toRdd.getCheckpointFile.map(x => x)
  }
}
