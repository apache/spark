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

import org.apache.spark.internal.config.ConfigBindingPolicy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.apache.spark.storage.{StorageLevel, StorageLevelMapper}

/**
 * Session configurations of the graph module.
 *
 * The object lives in the `org.apache.spark.sql` package to read typed values with
 * `RuntimeConfig.get(entry)`. Values are read from the session of the processed graph, not from
 * the active session.
 */
private[spark] object GraphConf {

  /**
   * The mode of lineage cuts in iterative algorithms. The "local" mode is often faster but not
   * reliable: the checkpointed data is lost together with the executors that store it, for
   * example on executor failure or preemption, and the job fails. The "reliable" mode is slower
   * and requires the checkpoint directory of the `SparkContext` to be set, but it survives the
   * loss of executors.
   */
  object CheckpointMode extends Enumeration {
    val LOCAL, RELIABLE = Value
  }

  /**
   * Where iterative algorithms keep the data that they read in every iteration, such as the
   * edges. The "memory" mode uses `persist` and is generally faster, but only while the data fits
   * into memory. The "disk" mode saves the data into Parquet files in the working directory. It
   * is generally slower, but recommended for large graphs because it reduces memory pressure and
   * spills.
   */
  object StaticDataMode extends Enumeration {
    val MEMORY, DISK = Value
  }

  val WORK_DIR = buildConf("spark.graph.workDir")
    .doc("The working directory for temporary files and intermediate results of iterative " +
      "graph algorithms, for example the files of the DISK mode of " +
      "spark.graph.staticDataMode. When not set, the checkpoint directory of the " +
      "SparkContext is used.")
    .version("4.5.0")
    .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
    .stringConf
    .createOptional

  val CHECKPOINT_MODE = buildConf("spark.graph.checkpointMode")
    .doc("The mode of lineage cuts in iterative graph algorithms. LOCAL uses local " +
      "checkpoints: it is often faster, but the checkpointed data is lost together with the " +
      "executors that store it, and the job fails. RELIABLE uses reliable checkpoints: it is " +
      "slower and requires the checkpoint directory of the SparkContext to be set, but it " +
      "survives the loss of executors.")
    .version("4.5.0")
    .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
    .enumConf(CheckpointMode)
    .createWithDefault(CheckpointMode.LOCAL)

  val STATIC_DATA_MODE = buildConf("spark.graph.staticDataMode")
    .doc("Where iterative graph algorithms keep the data that they read in every iteration, " +
      "such as the edges. MEMORY persists the data with spark.graph.storageLevel and is " +
      "generally faster, but only while the data fits into memory. DISK saves the data into " +
      "Parquet files in spark.graph.workDir. It is generally slower, but recommended for " +
      "large graphs because it reduces memory pressure and spills.")
    .version("4.5.0")
    .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
    .enumConf(StaticDataMode)
    .createWithDefault(StaticDataMode.DISK)

  val CHECKPOINT_INTERVAL = buildConf("spark.graph.checkpointInterval")
    .doc("The default checkpoint interval of iterative graph algorithms, in iterations. " +
      "Checkpointing is not free, but a growing lineage is expensive as well. This " +
      "configuration tunes the balance between frequent checkpoints with a small computation " +
      "graph and less frequent checkpoints with faster iterations but a bigger computation " +
      "graph.")
    .version("4.5.0")
    .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
    .intConf
    .createWithDefault(2)

  val STORAGE_LEVEL = buildConf("spark.graph.storageLevel")
    .doc("The storage level of the data that graph algorithms persist, for example in " +
      "GraphFrame.validate(), in local checkpoints and in the MEMORY mode of " +
      "spark.graph.staticDataMode. When not set, spark.sql.defaultCacheStorageLevel is used.")
    .version("4.5.0")
    .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
    .enumConf(classOf[StorageLevelMapper])
    .createOptional

  def workDir(spark: SparkSession): Option[String] = spark.conf.get(WORK_DIR)

  def checkpointMode(spark: SparkSession): CheckpointMode.Value =
    spark.conf.get(CHECKPOINT_MODE)

  def staticDataMode(spark: SparkSession): StaticDataMode.Value =
    spark.conf.get(STATIC_DATA_MODE)

  def checkpointInterval(spark: SparkSession): Int = spark.conf.get(CHECKPOINT_INTERVAL)

  /**
   * The storage level for intermediate results: `spark.graph.storageLevel` if set, otherwise
   * `spark.sql.defaultCacheStorageLevel`.
   */
  def storageLevel(spark: SparkSession): StorageLevel = {
    val level = spark.conf
      .get(STORAGE_LEVEL)
      .getOrElse(spark.conf.get(SQLConf.DEFAULT_CACHE_STORAGE_LEVEL))
    StorageLevel.fromString(level.name())
  }
}
