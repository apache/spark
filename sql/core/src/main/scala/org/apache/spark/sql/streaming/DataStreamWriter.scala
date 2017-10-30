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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.{AnalysisException, Dataset, ForeachWriter}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{ForeachSink, MemoryPlan, MemorySink}

/**
 * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this.
 *
 * @since 2.0.0
 */
@InterfaceStability.Evolving
final class DataStreamWriter[T] private[sql](ds: Dataset[T]) {

  private val df = ds.toDF()

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   *   - `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be
   *                            written to the sink
   *   - `OutputMode.Complete()`: all the rows in the streaming DataFrame/Dataset will be written
   *                              to the sink every time these is some updates
   *   - `OutputMode.Update()`: only the rows that were updated in the streaming DataFrame/Dataset
   *                            will be written to the sink every time there are some updates. If
   *                            the query doesn't contain aggregations, it will be equivalent to
   *                            `OutputMode.Append()` mode.
   *
   * @since 2.0.0
   */
  def outputMode(outputMode: OutputMode): DataStreamWriter[T] = {
    this.outputMode = outputMode
    this
  }

  /**
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   *   - `append`:   only the new rows in the streaming DataFrame/Dataset will be written to
   *                 the sink
   *   - `complete`: all the rows in the streaming DataFrame/Dataset will be written to the sink
   *                 every time these is some updates
   *   - `update`:   only the rows that were updated in the streaming DataFrame/Dataset will
   *                 be written to the sink every time there are some updates. If the query doesn't
   *                 contain aggregations, it will be equivalent to `append` mode.
   * @since 2.0.0
   */
  def outputMode(outputMode: String): DataStreamWriter[T] = {
    this.outputMode = InternalOutputModes(outputMode)
    this
  }

  /**
   * Set the trigger for the stream query. The default value is `ProcessingTime(0)` and it will run
   * the query as fast as possible.
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
  def trigger(trigger: Trigger): DataStreamWriter[T] = {
    this.trigger = trigger
    this
  }

  /**
   * Specifies the name of the [[StreamingQuery]] that can be started with `start()`.
   * This name must be unique among all the currently active queries in the associated SQLContext.
   *
   * @since 2.0.0
   */
  def queryName(queryName: String): DataStreamWriter[T] = {
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  /**
   * Specifies the underlying output data source.
   *
   * @since 2.0.0
   */
  def format(source: String): DataStreamWriter[T] = {
    this.source = source
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   *
   *   - year=2016/month=01/
   *   - year=2016/month=02/
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataStreamWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to format timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def option(key: String, value: String): DataStreamWriter[T] = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataStreamWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataStreamWriter[T] = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to format timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: scala.collection.Map[String, String]): DataStreamWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * You can set the following option(s):
   * <ul>
   * <li>`timeZone` (default session local timezone): sets the string that indicates a timezone
   * to be used to format timestamps in the JSON/CSV datasources or partition values.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  def options(options: java.util.Map[String, String]): DataStreamWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  def start(path: String): StreamingQuery = {
    option("path", path).start()
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  def start(): StreamingQuery = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw new AnalysisException("Hive data source can only be used with tables, you can not " +
        "write files of Hive data source directly.")
    }

    if (source == "memory") {
      assertNotPartitioned("memory")
      if (extraOptions.get("queryName").isEmpty) {
        throw new AnalysisException("queryName must be specified for memory sink")
      }
      val sink = new MemorySink(df.schema, outputMode)
      val resultDf = Dataset.ofRows(df.sparkSession, new MemoryPlan(sink))
      val chkpointLoc = extraOptions.get("checkpointLocation")
      val recoverFromChkpoint = outputMode == OutputMode.Complete()
      val query = df.sparkSession.sessionState.streamingQueryManager.startQuery(
        extraOptions.get("queryName"),
        chkpointLoc,
        df,
        sink,
        outputMode,
        useTempCheckpointLocation = true,
        recoverFromCheckpointLocation = recoverFromChkpoint,
        trigger = trigger)
      resultDf.createOrReplaceTempView(query.name)
      query
    } else if (source == "foreach") {
      assertNotPartitioned("foreach")
      val sink = new ForeachSink[T](foreachWriter)(ds.exprEnc)
      df.sparkSession.sessionState.streamingQueryManager.startQuery(
        extraOptions.get("queryName"),
        extraOptions.get("checkpointLocation"),
        df,
        sink,
        outputMode,
        useTempCheckpointLocation = true,
        trigger = trigger)
    } else {
      val (useTempCheckpointLocation, recoverFromCheckpointLocation) =
        if (source == "console") {
          (true, false)
        } else {
          (false, true)
        }
      val dataSource =
        DataSource(
          df.sparkSession,
          className = source,
          options = extraOptions.toMap,
          partitionColumns = normalizedParCols.getOrElse(Nil))
      df.sparkSession.sessionState.streamingQueryManager.startQuery(
        extraOptions.get("queryName"),
        extraOptions.get("checkpointLocation"),
        df,
        dataSource.createSink(outputMode),
        outputMode,
        useTempCheckpointLocation = useTempCheckpointLocation,
        recoverFromCheckpointLocation = recoverFromCheckpointLocation,
        trigger = trigger)
    }
  }

  /**
   * Starts the execution of the streaming query, which will continually send results to the given
   * `ForeachWriter` as new data arrives. The `ForeachWriter` can be used to send the data
   * generated by the `DataFrame`/`Dataset` to an external system.
   *
   * Scala example:
   * {{{
   *   datasetOfString.writeStream.foreach(new ForeachWriter[String] {
   *
   *     def open(partitionId: Long, version: Long): Boolean = {
   *       // open connection
   *     }
   *
   *     def process(record: String) = {
   *       // write string to connection
   *     }
   *
   *     def close(errorOrNull: Throwable): Unit = {
   *       // close the connection
   *     }
   *   }).start()
   * }}}
   *
   * Java example:
   * {{{
   *  datasetOfString.writeStream().foreach(new ForeachWriter<String>() {
   *
   *    @Override
   *    public boolean open(long partitionId, long version) {
   *      // open connection
   *    }
   *
   *    @Override
   *    public void process(String value) {
   *      // write string to connection
   *    }
   *
   *    @Override
   *    public void close(Throwable errorOrNull) {
   *      // close the connection
   *    }
   *  }).start();
   * }}}
   *
   * @since 2.0.0
   */
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] = {
    this.source = "foreach"
    this.foreachWriter = if (writer != null) {
      ds.sparkSession.sparkContext.clean(writer)
    } else {
      throw new IllegalArgumentException("foreach writer cannot be null")
    }
    this
  }

  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map { cols =>
    cols.map(normalize(_, "Partition"))
  }

  /**
   * The given column name may not be equal to any of the existing column names if we were in
   * case-insensitive context. Normalize the given column name to the real one so that we don't
   * need to care about case sensitivity afterwards.
   */
  private def normalize(columnName: String, columnType: String): String = {
    val validColumnNames = df.logicalPlan.output.map(_.name)
    validColumnNames.find(df.sparkSession.sessionState.analyzer.resolver(_, columnName))
      .getOrElse(throw new AnalysisException(s"$columnType column $columnName not found in " +
        s"existing columns (${validColumnNames.mkString(", ")})"))
  }

  private def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw new AnalysisException(s"'$operation' does not support partitioning")
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sparkSession.sessionState.conf.defaultDataSourceName

  private var outputMode: OutputMode = OutputMode.Append

  private var trigger: Trigger = Trigger.ProcessingTime(0L)

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  private var foreachWriter: ForeachWriter[T] = null

  private var partitioningColumns: Option[Seq[String]] = None
}
