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
package org.apache.spark.sql.api

import _root_.java
import _root_.java.util.concurrent.TimeoutException

import org.apache.spark.annotation.Evolving
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql.{ForeachWriter, WriteConfigMethods}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
abstract class DataStreamWriter[T] extends WriteConfigMethods[DataStreamWriter[T]] {
  type DS[U] <: Dataset[U]

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink. <ul> <li>
   * `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be written
   * to the sink.</li> <li> `OutputMode.Complete()`: all the rows in the streaming
   * DataFrame/Dataset will be written to the sink every time there are some updates.</li> <li>
   * `OutputMode.Update()`: only the rows that were updated in the streaming DataFrame/Dataset
   * will be written to the sink every time there are some updates. If the query doesn't contain
   * aggregations, it will be equivalent to `OutputMode.Append()` mode.</li> </ul>
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: OutputMode): this.type

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink. <ul> <li>
   * `append`: only the new rows in the streaming DataFrame/Dataset will be written to the
   * sink.</li> <li> `complete`: all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time there are some updates.</li> <li> `update`: only the rows that were
   * updated in the streaming DataFrame/Dataset will be written to the sink every time there are
   * some updates. If the query doesn't contain aggregations, it will be equivalent to `append`
   * mode.</li> </ul>
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: String): this.type

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
   * @since 2.0.0
   */
  def trigger(trigger: Trigger): this.type

  /**
   * Specifies the name of the [[org.apache.spark.sql.api.StreamingQuery]] that can be started
   * with `start()`. This name must be unique among all the currently active queries in the
   * associated SparkSession.
   *
   * @since 2.0.0
   */
  def queryName(queryName: String): this.type

  /**
   * Sets the output of the streaming query to be processed using the provided writer object.
   * object. See [[org.apache.spark.sql.ForeachWriter]] for more details on the lifecycle and
   * semantics.
   *
   * @since 2.0.0
   */
  def foreach(writer: ForeachWriter[T]): this.type

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
   * @since 2.4.0
   */
  @Evolving
  def foreachBatch(function: (DS[T], Long) => Unit): this.type

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
   * @since 2.4.0
   */
  @Evolving
  def foreachBatch(function: VoidFunction2[DS[T], java.lang.Long]): this.type = {
    foreachBatch((batchDs: DS[T], batchId: Long) => function.call(batchDs, batchId))
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given path as new data arrives. The returned [[org.apache.spark.sql.api.StreamingQuery]]
   * object can be used to interact with the stream.
   *
   * @since 2.0.0
   */
  def start(path: String): StreamingQuery

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given path as new data arrives. The returned [[org.apache.spark.sql.api.StreamingQuery]]
   * object can be used to interact with the stream. Throws a `TimeoutException` if the following
   * conditions are met:
   *   - Another run of the same streaming query, that is a streaming query sharing the same
   *     checkpoint location, is already active on the same Spark Driver
   *   - The SQL configuration `spark.sql.streaming.stopActiveRunOnRestart` is enabled
   *   - The active run cannot be stopped within the timeout controlled by the SQL configuration
   *     `spark.sql.streaming.stopTimeout`
   *
   * @since 2.0.0
   */
  @throws[TimeoutException]
  def start(): StreamingQuery

  /**
   * Starts the execution of the streaming query, which will continually output results to the
   * given table as new data arrives. The returned [[org.apache.spark.sql.api.StreamingQuery]]
   * object can be used to interact with the stream.
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
   * @since 3.1.0
   */
  @Evolving
  @throws[TimeoutException]
  def toTable(tableName: String): StreamingQuery

  ///////////////////////////////////////////////////////////////////////////////////////
  // Covariant Overrides
  ///////////////////////////////////////////////////////////////////////////////////////
  override def option(key: String, value: Boolean): this.type =
    super.option(key, value).asInstanceOf[this.type]
  override def option(key: String, value: Long): this.type =
    super.option(key, value).asInstanceOf[this.type]
  override def option(key: String, value: Double): this.type =
    super.option(key, value).asInstanceOf[this.type]
}
