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

package org.apache.spark.sql.internal.connector

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext, TaskKilledException}
import org.apache.spark.executor.Executor
import org.apache.spark.internal.config.KILL_ON_FATAL_ERROR_DEPTH
import org.apache.spark.util.Utils

/**
 * The errors a reader applying a storage filter must not absorb by giving the filter up. The
 * file-source Parquet reader and [[StorageFilterImpl]], which a data source v2 reader evaluates,
 * follow this one rule.
 */
object StorageFilterErrors {

  /**
   * Rethrows an error the reader met while applying a storage filter, unless it may give the filter
   * up on it. In the executor's order:
   *  - a fatal error goes out as it is;
   *  - a pending task kill goes out wrapped when the executor would report the error as a kill,
   *    and as it is otherwise, see [[throwIfKilled]], since the error may be the kill's interrupt
   *    surfacing through a UDF;
   *  - a fatal error deeper in the cause chain, as deep as the executor looks, goes out as itself.
   *
   * An `InternalError` from the first or the last case goes out in an `UnknownError`, another fatal
   * error. A bare one would be taken for a corrupt file under `ignoreCorruptFiles`, and a
   * `NonFatal` wrapper would let the executor run on under a pending kill.
   *
   * `fromRead` says the error came from decoding the key pages, which only the file-source Parquet
   * reader does. Such an error differs in three ways:
   *  - `FileScanRDD` wraps a `NonFatal` one before the executor looks, so a fatal cause has to
   *    sit one level shallower to count;
   *  - an `InternalError` is corrupt data, as `DataSourceUtils.shouldIgnoreCorruptFileException`
   *    takes it;
   *  - a key vector that cannot grow is the reader's own extra work, so giving it up answers it.
   *
   * Anything else falls back. The post-scan Filter raises an evaluation error again for a row it
   * evaluates, and the plain read of the same pages meets a decoding error again.
   */
  def rethrowIfMustPropagate(e: Throwable, fromRead: Boolean): Unit = {
    def corrupt(t: Throwable): Boolean = fromRead && t.isInstanceOf[InternalError]
    def out(fatal: Throwable): Throwable = fatal match {
      case internal: InternalError =>
        new UnknownError("A storage filter met a fatal error it must not absorb")
          .initCause(internal)
      case other => other
    }
    if (Executor.isFatalError(e, 1) && !corrupt(e)) throw out(e)
    throwIfKilled(e)
    if (fromRead && isVectorGrowthFailure(e)) return
    val depth = if (fromRead && NonFatal(e)) fatalErrorDepth - 1 else fatalErrorDepth
    if (Executor.isFatalError(e, depth)) {
      // The one `Executor.isFatalError` found, which no SparkOutOfMemoryError can precede.
      val fatal = Iterator.iterate(e)(_.getCause).find(Utils.isFatalError).get
      if (!corrupt(fatal)) throw out(fatal)
    }
  }

  /**
   * Whether `e` is how `WritableColumnVector.reserve` reports a vector that ran out of memory to
   * grow. The one without a cause, for a capacity too large, has no fatal error to find, so the
   * reader falls back on it anyway.
   */
  private def isVectorGrowthFailure(e: Throwable): Boolean =
    e.getClass == classOf[RuntimeException] && e.getCause.isInstanceOf[OutOfMemoryError]

  /**
   * Throws a pending task kill, for the reader's loops that can run long without returning to
   * `FileScanRDD`, which is where a plain read meets one.
   */
  def throwIfKilled(): Unit = throwIfKilled(null)

  /**
   * Throws a pending task kill, wrapped in a checked [[SparkException]] around `surfaced`, the
   * error the kill surfaced as, if any. Checked, since under `ignoreCorruptFiles` `FileScanRDD`, or
   * a data source v2 `FilePartitionReader`, would take a `RuntimeException` or `IOException` for a
   * corrupt file and go on to the split's remaining files. A kill that surfaces as an `IOException`
   * from parquet's own reads is not covered, as for a plain read.
   *
   * The executor reports a task as killed only for an `InterruptedException` or a `NonFatal`
   * error, so a `surfaced` error of any other type goes out as it is, the way a plain read raises
   * it.
   */
  private def throwIfKilled(surfaced: Throwable): Unit = {
    val context = TaskContext.get()
    if (context != null && context.isInterrupted()) {
      val cause = surfaced match {
        case null => new TaskKilledException(context.getKillReason().get)
        case _: InterruptedException | NonFatal(_) => surfaced
        case other => throw other
      }
      throw new SparkException("A storage filter met a task kill it must not absorb", cause)
    }
  }

  // Read the way `SparkUncaughtExceptionHandler` reads it, since there may be no SparkEnv.
  private def fatalErrorDepth: Int = Option(SparkEnv.get).map(_.conf.get(KILL_ON_FATAL_ERROR_DEPTH))
    .getOrElse(KILL_ON_FATAL_ERROR_DEPTH.defaultValue.get)
}
