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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.StructType


/**
 * A factory that produces [[OutputWriter]]s.  A new [[OutputWriterFactory]] is created on driver
 * side for each write job issued when writing to a [[HadoopFsRelation]], and then gets serialized
 * to executor side to create actual [[OutputWriter]]s on the fly.
 */
abstract class OutputWriterFactory extends Serializable {
  /**
   * When writing to a [[HadoopFsRelation]], this method gets called by each task on executor side
   * to instantiate new [[OutputWriter]]s.
   *
   * @param stagingDir Base path (directory) of the file to which this [[OutputWriter]] is supposed
   *                   to write.  Note that this may not point to the final output file.  For
   *                   example, `FileOutputFormat` writes to temporary directories and then merge
   *                   written files back to the final destination.  In this case, `path` points to
   *                   a temporary output file under the temporary directory.
   * @param fileNamePrefix Prefix of the file name. The returned OutputWriter must make sure this
   *                       prefix is used in the actual file name. For example, if the prefix is
   *                       "part-1-2-3", then the file name must start with "part_1_2_3" but can
   *                       end in arbitrary extension.
   * @param dataSchema Schema of the rows to be written. Partition columns are not included in the
   *        schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   * @since 1.4.0
   */
  def newInstance(
      stagingDir: String,
      fileNamePrefix: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter

  /**
   * Returns a new instance of [[OutputWriter]] that will write data to the given path.
   * This method gets called by each task on executor to write InternalRows to
   * format-specific files. Compared to the other `newInstance()`, this is a newer API that
   * passes only the path that the writer must write to. The writer must write to the exact path
   * and not modify it (do not add subdirectories, extensions, etc.). All other
   * file-format-specific information needed to create the writer must be passed
   * through the [[OutputWriterFactory]] implementation.
   * @since 2.0.0
   */
  def newWriter(path: String): OutputWriter = {
    throw new UnsupportedOperationException("newInstance with just path not supported")
  }
}


/**
 * [[OutputWriter]] is used together with [[HadoopFsRelation]] for persisting rows to the
 * underlying file system.  Subclasses of [[OutputWriter]] must provide a zero-argument constructor.
 * An [[OutputWriter]] instance is created and initialized when a new output file is opened on
 * executor side.  This instance is used to persist rows to this single output file.
 */
abstract class OutputWriter {
  /**
   * Persists a single row.  Invoked on the executor side.  When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in rows to be written.
   *
   * @since 1.4.0
   */
  def write(row: Row): Unit

  /**
   * Closes the [[OutputWriter]]. Invoked on the executor side after all rows are persisted, before
   * the task output is committed.
   *
   * @since 1.4.0
   */
  def close(): Unit

  private var converter: InternalRow => Row = _

  protected[sql] def initConverter(dataSchema: StructType) = {
    converter =
      CatalystTypeConverters.createToScalaConverter(dataSchema).asInstanceOf[InternalRow => Row]
  }

  protected[sql] def writeInternal(row: InternalRow): Unit = {
    write(converter(row))
  }
}
