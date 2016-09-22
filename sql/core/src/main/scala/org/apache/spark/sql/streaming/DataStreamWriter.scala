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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, ForeachWriter}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{ForeachSink, MemoryPlan, MemorySink}

/**
 * :: Experimental ::
 * Interface used to write a streaming [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[Dataset.writeStream]] to access this.
 *
 * @since 2.0.0
 */
@Experimental
final class DataStreamWriter[T] private[sql](ds: Dataset[T]) {

  private val df = ds.toDF()

  /**
   * :: Experimental ::
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   *   - `OutputMode.Append()`: only the new rows in the streaming DataFrame/Dataset will be
   *                            written to the sink
   *   - `OutputMode.Complete()`: all the rows in the streaming DataFrame/Dataset will be written
   *                              to the sink every time these is some updates
   *
   * @since 2.0.0
   */
  @Experimental
  def outputMode(outputMode: OutputMode): DataStreamWriter[T] = {
    this.outputMode = outputMode
    this
  }


  /**
   * :: Experimental ::
   * Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.
   *   - `append`:   only the new rows in the streaming DataFrame/Dataset will be written to
   *                 the sink
   *   - `complete`: all the rows in the streaming DataFrame/Dataset will be written to the sink
   *                 every time these is some updates
   *
   * @since 2.0.0
   */
  @Experimental
  def outputMode(outputMode: String): DataStreamWriter[T] = {
    this.outputMode = outputMode.toLowerCase match {
      case "append" =>
        OutputMode.Append
      case "complete" =>
        OutputMode.Complete
      case _ =>
        throw new IllegalArgumentException(s"Unknown output mode $outputMode. " +
          "Accepted output modes are 'append' and 'complete'")
    }
    this
  }

  /**
   * :: Experimental ::
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
  @Experimental
  def trigger(trigger: Trigger): DataStreamWriter[T] = {
    this.trigger = trigger
    this
  }


  /**
   * :: Experimental ::
   * Specifies the name of the [[StreamingQuery]] that can be started with `start()`.
   * This name must be unique among all the currently active queries in the associated SQLContext.
   *
   * @since 2.0.0
   */
  @Experimental
  def queryName(queryName: String): DataStreamWriter[T] = {
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  /**
   * :: Experimental ::
   * Specifies the underlying output data source. Built-in options include "parquet" for now.
   *
   * @since 2.0.0
   */
  @Experimental
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
   * This was initially applicable for Parquet but in 1.5+ covers JSON, text, ORC and avro as well.
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataStreamWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * :: Experimental ::
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: String): DataStreamWriter[T] = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * :: Experimental ::
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: Boolean): DataStreamWriter[T] = option(key, value.toString)

  /**
   * :: Experimental ::
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: Long): DataStreamWriter[T] = option(key, value.toString)

  /**
   * :: Experimental ::
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def option(key: String, value: Double): DataStreamWriter[T] = option(key, value.toString)

  /**
   * :: Experimental ::
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def options(options: scala.collection.Map[String, String]): DataStreamWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
   * :: Experimental ::
   * Adds output options for the underlying data source.
   *
   * @since 2.0.0
   */
  @Experimental
  def options(options: java.util.Map[String, String]): DataStreamWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
   * :: Experimental ::
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  @Experimental
  def start(path: String): StreamingQuery = {
    option("path", path).start()
  }

  /**
   * :: Experimental ::
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[StreamingQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  @Experimental
  def start(): StreamingQuery = {
    if (source == "memory") {
      assertNotPartitioned("memory")
      if (extraOptions.get("queryName").isEmpty) {
        throw new AnalysisException("queryName must be specified for memory sink")
      }

      val sink = new MemorySink(df.schema, outputMode)
      val resultDf = Dataset.ofRows(df.sparkSession, new MemoryPlan(sink))
      val query = df.sparkSession.sessionState.streamingQueryManager.startQuery(
        extraOptions.get("queryName"),
        extraOptions.get("checkpointLocation"),
        df,
        sink,
        outputMode,
        useTempCheckpointLocation = true,
        recoverFromCheckpointLocation = false,
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
        useTempCheckpointLocation = true,
        recoverFromCheckpointLocation = true,
        trigger = trigger)
    }
  }

  /**
   * :: Experimental ::
   * Starts the execution of the streaming query, which will continually send results to the given
   * [[ForeachWriter]] as as new data arrives. The [[ForeachWriter]] can be used to send the data
   * generated by the [[DataFrame]]/[[Dataset]] to an external system.
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
  @Experimental
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

  private var trigger: Trigger = ProcessingTime(0L)

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  private var foreachWriter: ForeachWriter[T] = null

  private var partitioningColumns: Option[Seq[String]] = None
}
