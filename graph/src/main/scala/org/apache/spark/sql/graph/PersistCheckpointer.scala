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

package org.apache.spark.sql.graph

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.graph.GraphCheckpointer.Checkpoint
import org.apache.spark.storage.StorageLevel

/**
 * A `GraphCheckpointer` that stores each checkpoint with `persist`.
 *
 * Corner cases:
 *   - Reading a removed checkpoint, or losing its blocks, recomputes it from the lineage. If the
 *     lineage is nondeterministic, for example uses `rand`, the new values can differ.
 *   - Spark caches equal plans once. If `df` is already cached, the checkpoint reuses that entry
 *     and never unpersists it. So removing the first of two equal checkpoints also uncaches the
 *     second one; its data stays correct but is recomputed.
 *
 * @param spark
 *   the session of the processed graph
 * @param storageLevel
 *   the storage level of the checkpoints
 */
private[spark] class PersistCheckpointer(spark: SparkSession, storageLevel: StorageLevel)
    extends GraphCheckpointer(spark) {

  override protected def store(
      df: DataFrame,
      name: String,
      layout: Option[BucketSpec]): Checkpoint = {
    val data = layout.fold(df) { spec =>
      df.sortWithinPartitions(col(quoteIdentifier(spec.sortColumnNames.head)))
    }
    val owned = spark.sharedState.cacheManager.lookupCachedData(data).isEmpty
    if (owned) {
      data.persist(storageLevel)
    }
    try {
      data.count()
    } catch {
      case e: Throwable =>
        if (owned) {
          data.unpersist()
        }
        throw e
    }
    Checkpoint(name, data, () => if (owned) data.unpersist())
  }
}

private[spark] object PersistCheckpointer {

  /**
   * Creates a checkpointer with the storage level from `spark.graph.storageLevel`.
   *
   * @param spark
   *   the session of the processed graph
   * @return
   *   a new checkpointer
   */
  def apply(spark: SparkSession): PersistCheckpointer =
    new PersistCheckpointer(spark, GraphConf.storageLevel(spark))
}
