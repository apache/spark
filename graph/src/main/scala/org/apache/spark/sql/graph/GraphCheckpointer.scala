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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.functions.col

/**
 * An ordered stack of intermediate results of one run of an iterative graph algorithm. This is a
 * backbone of most of iterative graph algorithms and especially pregel-based. This tool exists to
 * allow write edges once pre-sorted and pre-bucketed by the edges SRC and then read and join with
 * vertex state on each of Pregel iterations without both Sort and Exchange on edges (typically
 * x5-100 of vertex state size). There are tow implementations tighted to the corresponding values
 * of `org.apache.spark.sql.graph.GraphConf.STATIC_DATA_MODE`: `MEMORY` matches the persist-based
 * implementation and `DISK` matches the parquet-files based implementation.
 *
 * Rules for callers (!!!):
 *   - Create one instance per run and call `close()` in a `finally` block.
 *   - Every push is eager: the data is fully stored when the method returns.
 *   - Before removing a checkpoint, materialize everything that was derived from it. Lazy
 *     `DataFrame`s still read it after the removal, and what happens then depends on the
 *     implementation.
 *   - Do not return a checkpoint as the result of an algorithm: `close()` releases it.
 *   - Do not use it to cut the lineage of state that is rebuilt from itself in a loop, whatever
 *     the implementation: use `Dataset.checkpoint` or `Dataset.localCheckpoint` for that. The
 *     persist-based implementation does not cut the lineage at all.
 *
 * The class is not thread-safe! Please, do not use outside of `graph` namespace: this tool
 * targets a very specific usecase, not a generic iterative checkpointer.
 *
 * @param spark
 *   the session of the processed graph
 */
private[spark] abstract class GraphCheckpointer(protected val spark: SparkSession)
    extends AutoCloseable
    with Logging {
  import GraphCheckpointer.Checkpoint

  private val stored = mutable.ArrayDeque.empty[Checkpoint]
  private var closed = false

  /** The number of stored checkpoints. */
  def size: Int = stored.length

  /**
   * Stores `df` on top of the stack.
   *
   * @param df
   *   the data to store
   * @param name
   *   a name for logs and paths; letters, digits, `_` and `-` only. It does not have to be unique
   * @return
   *   the stored data with the schema of `df`
   */
  def push(df: DataFrame, name: String): DataFrame = {
    val checkpoint = create(df, name, None)
    stored.append(checkpoint)
    checkpoint.data
  }

  /**
   * Stores `df` hash-partitioned and sorted by `key`, so that a join or an aggregation on `key`
   * neither shuffles nor sorts this side. Use it for data that is written once and read many
   * times.
   *
   * Select only the needed columns before the push (!!!): the Parquet implementation cannot prune
   * columns or push filters down when it reads this data.
   *
   * The number of partitions is fixed here: a join shuffles its other side to this number,
   * whatever `spark.sql.shuffle.partitions` is later. The other side must have the same key type.
   *
   * @param df
   *   the data to store
   * @param name
   *   a name for logs and paths; see `push`
   * @param key
   *   the name of a top-level column
   * @param numPartitions
   *   the number of partitions, by default `spark.sql.shuffle.partitions`
   * @return
   *   the stored data with the schema of `df`
   */
  def pushPartitioned(
      df: DataFrame,
      name: String,
      key: String,
      numPartitions: Int = spark.sessionState.conf.numShufflePartitions): DataFrame = {
    // The name as in the schema: the writer and the scan look up bucket columns by exact name.
    // An unknown key is reported by repartition.
    val keyName = df.columns.find(spark.sessionState.conf.resolver(_, key)).getOrElse(key)
    val layout = BucketSpec(numPartitions, Seq(keyName), Seq(keyName))
    // One shuffle partition per bucket gives one file per bucket.
    val repartitioned = df.repartition(numPartitions, col(quoteIdentifier(keyName)))
    val checkpoint = create(repartitioned, name, Some(layout))
    stored.append(checkpoint)
    checkpoint.data
  }

  /**
   * Stores `df`, then removes the `n` checkpoints below it. `df` may read those checkpoints. If
   * the write fails, the stack does not change.
   *
   * @param n
   *   the number of checkpoints to replace, from 1 to `size`
   * @param df
   *   the data to store
   * @param name
   *   a name for logs and paths; see `push`
   * @return
   *   the stored data with the schema of `df`
   */
  def replaceLast(n: Int, df: DataFrame, name: String): DataFrame = {
    checkInternal(
      n > 0 && n <= stored.length,
      s"Cannot replace $n of ${stored.length} checkpoints.")
    // Store first: df usually reads the checkpoints that it replaces.
    val checkpoint = create(df, name, None)
    val replaced = stored.takeRight(n).toSeq
    stored.dropRightInPlace(n)
    stored.append(checkpoint)
    replaced.foreach(release)
    checkpoint.data
  }

  /**
   * Removes the `n` oldest checkpoints. If `n` is greater than `size`, removes all of them.
   *
   * @param n
   *   the number of checkpoints to remove, not negative
   */
  def evict(n: Int): Unit = {
    checkInternal(n >= 0, s"The number of checkpoints must not be negative, but got $n.")
    val evicted = stored.take(n).toSeq
    stored.dropInPlace(n)
    evicted.foreach(release)
  }

  /**
   * Removes all the checkpoints except the `n` newest. Does nothing if `n` is at least `size`.
   *
   * @param n
   *   the number of checkpoints to keep, not negative
   */
  def evictAllButLastN(n: Int): Unit = {
    checkInternal(n >= 0, s"The number of checkpoints must not be negative, but got $n.")
    evict((stored.length - n).max(0))
  }

  /**
   * Removes the `n` newest checkpoints. If `n` is greater than `size`, removes all of them.
   *
   * @param n
   *   the number of checkpoints to remove, not negative
   */
  def removeLast(n: Int): Unit = {
    checkInternal(n >= 0, s"The number of checkpoints must not be negative, but got $n.")
    val removed = stored.takeRight(n).toSeq
    stored.dropRightInPlace(n)
    removed.foreach(release)
  }

  /** Removes all the checkpoints. The instance stays usable. */
  def purge(): Unit = evict(stored.length)

  /**
   * Removes all the checkpoints and everything else the instance stored. It is safe to call this
   * method more than once. New pushes fail after it.
   */
  override def close(): Unit = {
    if (!closed) {
      closed = true
      purge()
      quietly("all")(cleanup())
    }
  }

  /**
   * Stores `df` eagerly.
   *
   * @param df
   *   the data to store; already repartitioned if `layout` is defined
   * @param name
   *   a validated name
   * @param layout
   *   the partitioning and the sort order to keep
   * @return
   *   the stored data and how to release it
   */
  protected def store(df: DataFrame, name: String, layout: Option[BucketSpec]): Checkpoint

  /** Releases what is not tracked as a checkpoint. Called once, by `close()`. */
  protected def cleanup(): Unit = ()

  private def create(df: DataFrame, name: String, layout: Option[BucketSpec]): Checkpoint = {
    checkInternal(!closed, "The checkpointer is closed.")
    checkInternal(name.matches("[A-Za-z0-9_-]+"), s"Invalid checkpoint name: $name.")
    store(df, name, layout)
  }

  // The class is internal: a failed check is a bug in the calling algorithm;
  // A drop-in replacement of `require`, internal usage only.
  private def checkInternal(condition: Boolean, message: => String): Unit = {
    if (!condition) {
      throw SparkException.internalError(message)
    }
  }

  private def release(checkpoint: Checkpoint): Unit =
    quietly(checkpoint.name)(checkpoint.release())

  // A failed cleanup must not fail the algorithm.
  private def quietly(name: String)(body: => Unit): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to release the checkpoint ${MDC(LogKeys.NAME, name)}", e)
    }
  }
}

private[spark] object GraphCheckpointer {

  /**
   * A stored result.
   *
   * @param name
   *   the name for logs
   * @param data
   *   the stored data
   * @param release
   *   frees the storage of `data`
   */
  private[graph] case class Checkpoint(name: String, data: DataFrame, release: () => Unit)
}
