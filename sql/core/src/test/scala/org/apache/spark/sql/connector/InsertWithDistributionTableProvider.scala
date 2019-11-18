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

package org.apache.spark.sql.connector

import java.io.IOException
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{RequiresTableWriteDistribution, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.read.partitioning.{Distribution, SparkHashClusteredDistribution}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class InsertWithDistributionTableProvider extends TableProvider with DataSourceRegister  {
  /**
   * Return a {@link Table} instance to do read/write with user-specified options.
   *
   * @param options the user-specified options that can identify a table, e.g. file path, Kafka
   *                topic name, etc. It's an immutable case-insensitive string-to-string map.
   */
  override def getTable(options: CaseInsensitiveStringMap): Table = new InsertWithDistributionTable

  /**
   * The string that represents the format that this data source provider uses. This is
   * overridden by children to provide a nice alias for the data source. For example:
   *
   * {{{
   *   override def shortName(): String = "parquet"
   * }}}
   *
   * @since 1.5.0
   */
  override def shortName(): String = "test-table-w-dist"
}

class InsertWithDistributionTable extends Table with RequiresTableWriteDistribution {

  override def requiredDistribution(): Distribution =
    new SparkHashClusteredDistribution(Array("id"), 11)

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder =
    new InsertWithDistributionWriteBuilder

  override def name(): String = "table_name"

  override def schema(): StructType = StructType(Seq(StructField("id", IntegerType)))

  import collection.JavaConverters._
  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_WRITE,
    TableCapability.TRUNCATE).asJava
}

class InsertWithDistributionWriteBuilder extends WriteBuilder with SupportsTruncate {
  override def buildForBatch(): BatchWrite =
    new InsertWithDistributionBatchWrite

  /**
   * Configures a write to replace all existing data with data committed in the write.
   *
   * @return this write builder for method chaining
   */
  override def truncate(): WriteBuilder = this
}

class InsertWithDistributionBatchWrite extends BatchWrite {
  /**
   * Creates a writer factory which will be serialized and sent to executors.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  override def createBatchWriterFactory(): DataWriterFactory =
    new InsertWithDistributionDataWriterFactory

  /**
   * Commits this writing job with a list of commit messages. The commit messages are collected from
   * successful data writers and are produced by {@link DataWriter#commit()}.
   *
   * If this method fails (by throwing an exception), this writing job is considered to to have been
   * failed, and {@link #abort(WriterCommitMessage[])} would be called. The state of the destination
   * is undefined and @{@link #abort(WriterCommitMessage[])} may not be able to deal with it.
   *
   * Note that speculative execution may cause multiple tasks to run for a partition. By default,
   * Spark uses the commit coordinator to allow at most one task to commit. Implementations can
   * disable this behavior by overriding {@link #useCommitCoordinator()}. If disabled, multiple
   * tasks may have committed successfully and one successful commit message per task will be
   * passed to this commit method. The remaining commit messages are ignored by Spark.
   */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  /**
   * Aborts this writing job because some data writers are failed and keep failing when retry,
   * or the Spark job fails with some unknown reasons,
   * or {@link #onDataWriterCommit(WriterCommitMessage)} fails,
   * or {@link #commit(WriterCommitMessage[])} fails.
   *
   * If this method fails (by throwing an exception), the underlying data source may require manual
   * cleanup.
   *
   * Unless the abort is triggered by the failure of commit, the given messages should have some
   * null slots as there maybe only a few data writers that are committed before the abort
   * happens, or some data writers were committed but their commit messages haven't reached the
   * driver when the abort is triggered. So this is just a "best effort" for data sources to
   * clean up the data left by data writers.
   */
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class InsertWithDistributionDataWriterFactory extends DataWriterFactory {
  /**
   * Returns a data writer to do the actual writing work. Note that, Spark will reuse the same data
   * object instance when sending data to the data writer, for better performance. Data writers
   * are responsible for defensive copies if necessary, e.g. copy the data before buffer it in a
   * list.
   *
   * If this method fails (by throwing an exception), the corresponding Spark write task would fail
   * and get retried until hitting the maximum retry times.
   *
   * @param partitionId A unique id of the RDD partition that the returned writer will process.
   *                    Usually Spark processes many RDD partitions at the same time,
   *                    implementations should use the partition id to distinguish writers for
   *                    different partitions.
   * @param taskId      The task id returned by { @link TaskContext#taskAttemptId()}. Spark may run
   *                                                    multiple tasks for the same partition (due to speculation or task failures,
   *                                                    for example).
   */
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new NoOpDataWrite
}

class NoOpDataWrite extends DataWriter[InternalRow] {
  /**
   * Writes one record.
   *
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  override def write(record: InternalRow): Unit = {}

  /**
   * Commits this writer after all records are written successfully, returns a commit message which
   * will be sent back to driver side and passed to
   * {@link BatchWrite#commit(WriterCommitMessage[])}.
   *
   * The written data should only be visible to data source readers after
   * {@link BatchWrite#commit(WriterCommitMessage[])} succeeds, which means this method
   * should still "hide" the written data and ask the {@link BatchWrite} at driver side to
   * do the final commit via {@link WriterCommitMessage}.
   *
   * If this method fails (by throwing an exception), {@link #abort()} will be called and this
   * data writer is considered to have been failed.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  override def commit(): WriterCommitMessage = {
    new WriterCommitMessage() {}
  }

  /**
   * Aborts this writer if it is failed. Implementations should clean up the data for already
   * written records.
   *
   * This method will only be called if there is one record failed to write, or {@link #commit()}
   * failed.
   *
   * If this method fails(by throwing an exception), the underlying data source may have garbage
   * that need to be cleaned by {@link BatchWrite#abort(WriterCommitMessage[])} or manually,
   * but these garbage should not be visible to data source readers.
   *
   * @throws IOException if failure happens during disk/network IO like writing files.
   */
  override def abort(): Unit = {}
}
