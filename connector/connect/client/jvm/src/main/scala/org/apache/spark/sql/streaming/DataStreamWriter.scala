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

package org.apache.spark.sql.streaming

import java.util.Locale
import java.util.concurrent.TimeoutException

import scala.jdk.CollectionConverters._

import com.google.protobuf.ByteString

import org.apache.spark.annotation.Evolving
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.Command
import org.apache.spark.connect.proto.WriteStreamOperationStart
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, ForeachWriter}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, ForeachWriterPacket, UdfUtils}
import org.apache.spark.sql.execution.streaming.AvailableNowTrigger
import org.apache.spark.sql.execution.streaming.ContinuousTrigger
import org.apache.spark.sql.execution.streaming.OneTimeTrigger
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.types.NullType
import org.apache.spark.util.SparkSerDeUtils

/**
 * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this.
 *
 * @since 3.5.0
 */
@Evolving
final class DataStreamWriter[T] private[sql] (ds: Dataset[T]) extends Logging {

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink. <ul> <li>
   * `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be written
   * to the sink.</li> <li> `OutputMode.Complete()`: all the rows in the streaming
   * DataFrame/Dataset will be written to the sink every time there are some updates.</li> <li>
   * `OutputMode.Update()`: only the rows that were updated in the streaming DataFrame/Dataset
   * will be written to the sink every time there are some updates. If the query doesn't contain
   * aggregations, it will be equivalent to `OutputMode.Append()` mode.</li> </ul>
   *
   * @since 3.5.0
   */
  def outputMode(outputMode: OutputMode): DataStreamWriter[T] = {
    sinkBuilder.setOutputMode(outputMode.toString.toLowerCase(Locale.ROOT))
    this
  }

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink. <ul> <li>
   * `append`: only the new rows in the streaming DataFrame/Dataset will be written to the
   * sink.</li> <li> `complete`: all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time there are some updates.</li> <li> `update`: only the rows that were
   * updated in the streaming DataFrame/Dataset will be written to the sink every time there are
   * some updates. If the query doesn't contain aggregations, it will be equivalent to `append`
   * mode.</li> </ul>
   *
   * @since 3.5.0
   */
  def outputMode(outputMode: String): DataStreamWriter[T] = {
    sinkBuilder.setOutputMode(outputMode)
    this
  }

  /**
   * Set the trigger for the stream query. The default value is `ProcessingTime(0)` and it will
   * run the query as fast as possible.
   *
   * Scala Example:
   * {{{
   *   df.writeStream.trigger(ProcessingTime("10 seconds"))
   *
   *   import scala.concurrent.duration._
   *   df.writeStream.trigger(ProcessingTime(10.seconds))
   * }}}
   *
   * Java Example:
   * {{{
   *   df.writeStream().trigger(ProcessingTime.create("10 seconds"))
   *
   *   import java.util.concurrent.TimeUnit
   *   df.writeStream().trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 3.5.0
   */
  def trigger(trigger: Trigger): DataStreamWriter[T] = {
    trigger match {
      case ProcessingTimeTrigger(intervalMs) =>
        sinkBuilder.setProcessingTimeInterval(s"$intervalMs milliseconds")
      case AvailableNowTrigger =>
        sinkBuilder.setAvailableNow(true)
      case OneTimeTrigger =>
        sinkBuilder.setOnce(true)
      case ContinuousTrigger(intervalMs) =>
        sinkBuilder.setContinuousCheckpointInterval(s"$intervalMs milliseconds")
    }
    this
  }

  /**
   * Specifies the name of the [[StreamingQuery]] that can be started with `start()`. This name
   * must be unique among all the currently active queries in the associated SQLContext.
   *
   * @since 3.5.0
   */
  def queryName(queryName: String): DataStreamWriter[T] = {
    sinkBuilder.setQueryName(queryName)
    this
  }

  /**
   * Specifies the underlying output data source.
   *
   * @since 3.5.0
   */
  def format(source: String): DataStreamWriter[T] = {
    sinkBuilder.setFormat(source)
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   *
   * <ul> <li> year=2016/month=01/</li> <li> year=2016/month=02/</li> </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout. It
   * provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number of
   * distinct values in each column should typically be less than tens of thousands.
   *
   * @since 3.5.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataStreamWriter[T] = {
    sinkBuilder.clearPartitioningColumnNames()
    sinkBuilder.addAllPartitioningColumnNames(colNames.asJava)
    this
  }

  /**
   * Clusters the output by the given columns. If specified, the output is laid out such that
   * records with similar values on the clustering column are grouped together in the same file.
   *
   * Clustering improves query efficiency by allowing queries with predicates on the clustering
   * columns to skip unnecessary data. Unlike partitioning, clustering can be used on very high
   * cardinality columns.
   *
   * @since 4.0.0
   */
  @scala.annotation.varargs
  def clusterBy(colNames: String*): DataStreamWriter[T] = {
    sinkBuilder.clearClusteringColumnNames()
    sinkBuilder.addAllClusteringColumnNames(colNames.asJava)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: String): DataStreamWriter[T] = {
    sinkBuilder.putOptions(key, value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: Boolean): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: Long): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 3.5.0
   */
  def option(key: String, value: Double): DataStreamWriter[T] = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 3.5.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamWriter[T] = {
    this.options(options.asJava)
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * @since 3.5.0
   */
  def options(options: java.util.Map[String, String]): DataStreamWriter[T] = {
    sinkBuilder.putAllOptions(options)
    this
  }

  /**
   * Sets the output of the streaming query to be processed using the provided writer object.
   * object. See [[org.apache.spark.sql.ForeachWriter]] for more details on the lifecycle and
   * semantics.
   * @since 3.5.0
   */
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] = {
    val serialized = SparkSerDeUtils.serialize(ForeachWriterPacket(writer, ds.agnosticEncoder))
    val scalaWriterBuilder = proto.ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(serialized))
    sinkBuilder.getForeachWriterBuilder.setScalaFunction(scalaWriterBuilder)
    this
  }

  /**
   * :: Experimental ::
   *
   * (Scala-specific) Sets the output of the streaming query to be processed using the provided
   * function. This is supported only in the micro-batch execution modes (that is, when the
   * trigger is not continuous). In every micro-batch, the provided function will be called in
   * every micro-batch with (i) the output rows as a Dataset and (ii) the batch identifier. The
   * batchId can be used to deduplicate and transactionally write the output (that is, the
   * provided Dataset) to external systems. The output Dataset is guaranteed to be exactly the
   * same for the same batchId (assuming all operations are deterministic in the query).
   *
   * @since 3.5.0
   */
  @Evolving
  def foreachBatch(function: (Dataset[T], Long) => Unit): DataStreamWriter[T] = {
    val serializedFn = SparkSerDeUtils.serialize(function)
    sinkBuilder.getForeachBatchBuilder.getScalaFunctionBuilder
      .setPayload(ByteString.copyFrom(serializedFn))
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(NullType)) // Unused.
      .setNullable(true) // Unused.
    this
  }

  /**
   * :: Experimental ::
   *
   * (Java-specific) Sets the output of the streaming query to be processed using the provided
   * function. This is supported only in the micro-batch execution modes (that is, when the
   * trigger is not continuous). In every micro-batch, the provided function will be called in
   * every micro-batch with (i) the output rows as a Dataset and (ii) the batch identifier. The
   * batchId can be used to deduplicate and transactionally write the output (that is, the
   * provided Dataset) to external systems. The output Dataset is guaranteed to be exactly the
   * same for the same batchId (assuming all operations are deterministic in the query).
   *
   * @since 3.5.0
   */
  @Evolving
  def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): DataStreamWriter[T] = {
    foreachBatch(UdfUtils.foreachBatchFuncToScalaFunc(function))
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given path as new data arrives. The returned [[StreamingQuery]] object can be used to
   * interact with the stream.
   *
   * @since 3.5.0
   */
  def start(path: String): StreamingQuery = {
    sinkBuilder.setPath(path)
    start()
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given path as new data arrives. The returned [[StreamingQuery]] object can be used to
   * interact with the stream. Throws a `TimeoutException` if the following conditions are met:
   *   - Another run of the same streaming query, that is a streaming query sharing the same
   *     checkpoint location, is already active on the same Spark Driver
   *   - The SQL configuration `spark.sql.streaming.stopActiveRunOnRestart` is enabled
   *   - The active run cannot be stopped within the timeout controlled by the SQL configuration
   *     `spark.sql.streaming.stopTimeout`
   *
   * @since 3.5.0
   */
  @throws[TimeoutException]
  def start(): StreamingQuery = {
    val startCmd = Command
      .newBuilder()
      .setWriteStreamOperationStart(sinkBuilder.build())
      .build()

    val resp = ds.sparkSession.execute(startCmd).head
    if (resp.getWriteStreamOperationStartResult.hasQueryStartedEventJson) {
      val event = QueryStartedEvent.fromJson(
        resp.getWriteStreamOperationStartResult.getQueryStartedEventJson)
      ds.sparkSession.streams.streamingQueryListenerBus.postToAll(event)
    }
    RemoteStreamingQuery.fromStartCommandResponse(ds.sparkSession, resp)
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given table as new data arrives. The returned [[StreamingQuery]] object can be used to
   * interact with the stream.
   *
   * For v1 table, partitioning columns provided by `partitionBy` will be respected no matter the
   * table exists or not. A new table will be created if the table not exists.
   *
   * For v2 table, `partitionBy` will be ignored if the table already exists. `partitionBy` will
   * be respected only if the v2 table does not exist. Besides, the v2 table created by this API
   * lacks some functionalities (e.g., customized properties, options, and serde info). If you
   * need them, please create the v2 table manually before the execution to avoid creating a table
   * with incomplete information.
   *
   * @since 3.5.0
   */
  @Evolving
  @throws[TimeoutException]
  def toTable(tableName: String): StreamingQuery = {
    sinkBuilder.setTableName(tableName)
    start()
  }

  private val sinkBuilder = WriteStreamOperationStart
    .newBuilder()
    .setInput(ds.plan.getRoot)
}
