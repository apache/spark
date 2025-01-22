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
import org.apache.spark.sql.{api, Dataset, ForeachWriter}
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, ForeachWriterPacket}
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
final class DataStreamWriter[T] private[sql] (ds: Dataset[T]) extends api.DataStreamWriter[T] {
  override type DS[U] = Dataset[U]

  /** @inheritdoc */
  def outputMode(outputMode: OutputMode): this.type = {
    sinkBuilder.setOutputMode(outputMode.toString.toLowerCase(Locale.ROOT))
    this
  }

  /** @inheritdoc */
  def outputMode(outputMode: String): this.type = {
    sinkBuilder.setOutputMode(outputMode)
    this
  }

  /** @inheritdoc */
  def trigger(trigger: Trigger): this.type = {
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

  /** @inheritdoc */
  def queryName(queryName: String): this.type = {
    sinkBuilder.setQueryName(queryName)
    this
  }

  /** @inheritdoc */
  def format(source: String): this.type = {
    sinkBuilder.setFormat(source)
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): this.type = {
    sinkBuilder.clearPartitioningColumnNames()
    sinkBuilder.addAllPartitioningColumnNames(colNames.asJava)
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def clusterBy(colNames: String*): this.type = {
    sinkBuilder.clearClusteringColumnNames()
    sinkBuilder.addAllClusteringColumnNames(colNames.asJava)
    this
  }

  /** @inheritdoc */
  def option(key: String, value: String): this.type = {
    sinkBuilder.putOptions(key, value)
    this
  }

  /** @inheritdoc */
  def options(options: scala.collection.Map[String, String]): this.type = {
    this.options(options.asJava)
    this
  }

  /** @inheritdoc */
  def options(options: java.util.Map[String, String]): this.type = {
    sinkBuilder.putAllOptions(options)
    this
  }

  /** @inheritdoc */
  def foreach(writer: ForeachWriter[T]): this.type = {
    val serialized = SparkSerDeUtils.serialize(ForeachWriterPacket(writer, ds.agnosticEncoder))
    val scalaWriterBuilder = proto.ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(serialized))
    sinkBuilder.getForeachWriterBuilder.setScalaFunction(scalaWriterBuilder)
    this
  }

  /** @inheritdoc */
  @Evolving
  def foreachBatch(function: (Dataset[T], Long) => Unit): this.type = {
    // SPARK-50661: the client should send the encoder for the input dataset together with the
    //  function to the server.
    val serializedFn =
      SparkSerDeUtils.serialize(ForeachWriterPacket(function, ds.agnosticEncoder))
    sinkBuilder.getForeachBatchBuilder.getScalaFunctionBuilder
      .setPayload(ByteString.copyFrom(serializedFn))
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(NullType)) // Unused.
      .setNullable(true) // Unused.
    this
  }

  /** @inheritdoc */
  def start(path: String): StreamingQuery = {
    sinkBuilder.setPath(path)
    start()
  }

  /** @inheritdoc */
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

  /** @inheritdoc */
  @Evolving
  @throws[TimeoutException]
  def toTable(tableName: String): StreamingQuery = {
    sinkBuilder.setTableName(tableName)
    start()
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Covariant Overrides
  ///////////////////////////////////////////////////////////////////////////////////////

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  @Evolving
  override def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): this.type =
    super.foreachBatch(function)

  private val sinkBuilder = WriteStreamOperationStart
    .newBuilder()
    .setInput(ds.plan.getRoot)
}
