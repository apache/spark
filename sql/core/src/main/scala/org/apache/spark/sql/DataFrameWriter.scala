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

package org.apache.spark.sql

import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, Project}
import org.apache.spark.sql.execution.datasources.{BucketSpec, CreateTableUsingAsSelect, DataSource, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.streaming.{MemoryPlan, MemorySink, StreamExecution}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * :: Experimental ::
 * Interface used to write a [[DataFrame]] to external storage systems (e.g. file systems,
 * key-value stores, etc) or data streams. Use [[DataFrame.write]] to access this.
 *
 * @since 1.4.0
 */
@Experimental
final class DataFrameWriter private[sql](df: DataFrame) {

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `SaveMode.Overwrite`: overwrite the existing data.
   *   - `SaveMode.Append`: append the data.
   *   - `SaveMode.Ignore`: ignore the operation (i.e. no-op).
   *   - `SaveMode.ErrorIfExists`: default option, throw an exception at runtime.
   *
   * @since 1.4.0
   */
  def mode(saveMode: SaveMode): DataFrameWriter = {
    // mode() is used for non-continuous queries
    // outputMode() is used for continuous queries
    assertNotStreaming("mode() can only be called on non-continuous queries")
    this.mode = saveMode
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include:
   *   - `overwrite`: overwrite the existing data.
   *   - `append`: append the data.
   *   - `ignore`: ignore the operation (i.e. no-op).
   *   - `error`: default option, throw an exception at runtime.
   *
   * @since 1.4.0
   */
  def mode(saveMode: String): DataFrameWriter = {
    // mode() is used for non-continuous queries
    // outputMode() is used for continuous queries
    assertNotStreaming("mode() can only be called on non-continuous queries")
    this.mode = saveMode.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "ignore" => SaveMode.Ignore
      case "error" | "default" => SaveMode.ErrorIfExists
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
        "Accepted modes are 'overwrite', 'append', 'ignore', 'error'.")
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
   *   df.write.trigger(ProcessingTime("10 seconds"))
   *
   *   import scala.concurrent.duration._
   *   df.write.trigger(ProcessingTime(10.seconds))
   * }}}
   *
   * Java Example:
   * {{{
   *   df.write.trigger(ProcessingTime.create("10 seconds"))
   *
   *   import java.util.concurrent.TimeUnit
   *   df.write.trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   *
   * @since 2.0.0
   */
  @Experimental
  def trigger(trigger: Trigger): DataFrameWriter = {
    assertStreaming("trigger() can only be called on continuous queries")
    this.trigger = trigger
    this
  }

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameWriter = {
    this.source = source
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameWriter = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataFrameWriter = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataFrameWriter = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataFrameWriter = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameWriter = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameWriter = {
    this.options(options.asScala)
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
  def partitionBy(colNames: String*): DataFrameWriter = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Buckets the output by the given columns. If specified, the output is laid out on the file
   * system similar to Hive's bucketing scheme.
   *
   * This is applicable for Parquet, JSON and ORC.
   *
   * @since 2.0
   */
  @scala.annotation.varargs
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter = {
    this.numBuckets = Option(numBuckets)
    this.bucketColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Sorts the output in each bucket by the given columns.
   *
   * This is applicable for Parquet, JSON and ORC.
   *
   * @since 2.0
   */
  @scala.annotation.varargs
  def sortBy(colName: String, colNames: String*): DataFrameWriter = {
    this.sortColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Saves the content of the [[DataFrame]] at the specified path.
   *
   * @since 1.4.0
   */
  def save(path: String): Unit = {
    this.extraOptions += ("path" -> path)
    save()
  }

  /**
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * @since 1.4.0
   */
  def save(): Unit = {
    assertNotBucketed()
    assertNotStreaming("save() can only be called on non-continuous queries")
    val dataSource = DataSource(
      df.sparkSession,
      className = source,
      partitionColumns = partitioningColumns.getOrElse(Nil),
      bucketSpec = getBucketSpec,
      options = extraOptions.toMap)

    dataSource.write(mode, df)
  }

  /**
   * Specifies the name of the [[ContinuousQuery]] that can be started with `startStream()`.
   * This name must be unique among all the currently active queries in the associated SQLContext.
   *
   * @since 2.0.0
   */
  def queryName(queryName: String): DataFrameWriter = {
    assertStreaming("queryName() can only be called on continuous queries")
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[ContinuousQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  def startStream(path: String): ContinuousQuery = {
    option("path", path).startStream()
  }

  /**
   * Starts the execution of the streaming query, which will continually output results to the given
   * path as new data arrives. The returned [[ContinuousQuery]] object can be used to interact with
   * the stream.
   *
   * @since 2.0.0
   */
  def startStream(): ContinuousQuery = {
    assertNotBucketed
    assertStreaming("startStream() can only be called on continuous queries")

    if (source == "memory") {
      val queryName =
        extraOptions.getOrElse(
          "queryName", throw new AnalysisException("queryName must be specified for memory sink"))
      val checkpointLocation = extraOptions.get("checkpointLocation").map { userSpecified =>
        new Path(userSpecified).toUri.toString
      }.orElse {
        val checkpointConfig: Option[String] =
          df.sparkSession.conf.get(SQLConf.CHECKPOINT_LOCATION)

        checkpointConfig.map { location =>
          new Path(location, queryName).toUri.toString
        }
      }.getOrElse {
        Utils.createTempDir(namePrefix = "memory.stream").getCanonicalPath
      }

      // If offsets have already been created, we trying to resume a query.
      val checkpointPath = new Path(checkpointLocation, "offsets")
      val fs = checkpointPath.getFileSystem(df.sparkSession.sessionState.newHadoopConf())
      if (fs.exists(checkpointPath)) {
        throw new AnalysisException(
          s"Unable to resume query written to memory sink. Delete $checkpointPath to start over.")
      } else {
        checkpointPath.toUri.toString
      }

      val sink = new MemorySink(df.schema)
      val resultDf = Dataset.ofRows(df.sparkSession, new MemoryPlan(sink))
      resultDf.registerTempTable(queryName)
      val continuousQuery = df.sparkSession.sessionState.continuousQueryManager.startQuery(
        queryName,
        checkpointLocation,
        df,
        sink,
        trigger)
      continuousQuery
    } else {
      val dataSource =
        DataSource(
          df.sparkSession,
          className = source,
          options = extraOptions.toMap,
          partitionColumns = normalizedParCols.getOrElse(Nil))

      val queryName = extraOptions.getOrElse("queryName", StreamExecution.nextName)
      val checkpointLocation = extraOptions.get("checkpointLocation")
        .orElse {
          df.sparkSession.sessionState.conf.checkpointLocation.map { l =>
            new Path(l, queryName).toUri.toString
          }
        }.getOrElse {
          throw new AnalysisException("checkpointLocation must be specified either " +
            "through option() or SQLConf")
        }

      df.sparkSession.sessionState.continuousQueryManager.startQuery(
        queryName,
        checkpointLocation,
        df,
        dataSource.createSink(),
        trigger)
    }
  }

  /**
   * Inserts the content of the [[DataFrame]] to the specified table. It requires that
   * the schema of the [[DataFrame]] is the same as the schema of the table.
   *
   * Because it inserts data to an existing table, format or options will be ignored.
   *
   * @since 1.4.0
   */
  def insertInto(tableName: String): Unit = {
    insertInto(df.sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName))
  }

  private def insertInto(tableIdent: TableIdentifier): Unit = {
    assertNotBucketed()
    assertNotStreaming("insertInto() can only be called on non-continuous queries")
    val partitions = normalizedParCols.map(_.map(col => col -> (None: Option[String])).toMap)
    val overwrite = mode == SaveMode.Overwrite

    // A partitioned relation's schema can be different from the input logicalPlan, since
    // partition columns are all moved after data columns. We Project to adjust the ordering.
    // TODO: this belongs to the analyzer.
    val input = normalizedParCols.map { parCols =>
      val (inputPartCols, inputDataCols) = df.logicalPlan.output.partition { attr =>
        parCols.contains(attr.name)
      }
      Project(inputDataCols ++ inputPartCols, df.logicalPlan)
    }.getOrElse(df.logicalPlan)

    df.sparkSession.executePlan(
      InsertIntoTable(
        UnresolvedRelation(tableIdent),
        partitions.getOrElse(Map.empty[String, Option[String]]),
        input,
        overwrite,
        ifNotExists = false)).toRdd
  }

  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map { cols =>
    cols.map(normalize(_, "Partition"))
  }

  private def normalizedBucketColNames: Option[Seq[String]] = bucketColumnNames.map { cols =>
    cols.map(normalize(_, "Bucketing"))
  }

  private def normalizedSortColNames: Option[Seq[String]] = sortColumnNames.map { cols =>
    cols.map(normalize(_, "Sorting"))
  }

  private def getBucketSpec: Option[BucketSpec] = {
    if (sortColumnNames.isDefined) {
      require(numBuckets.isDefined, "sortBy must be used together with bucketBy")
    }

    for {
      n <- numBuckets
    } yield {
      require(n > 0 && n < 100000, "Bucket number must be greater than 0 and less than 100000.")

      // partitionBy columns cannot be used in bucketBy
      if (normalizedParCols.nonEmpty &&
        normalizedBucketColNames.get.toSet.intersect(normalizedParCols.get.toSet).nonEmpty) {
          throw new AnalysisException(
            s"bucketBy columns '${bucketColumnNames.get.mkString(", ")}' should not be part of " +
            s"partitionBy columns '${partitioningColumns.get.mkString(", ")}'")
      }

      BucketSpec(n, normalizedBucketColNames.get, normalizedSortColNames.getOrElse(Nil))
    }
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

  private def assertNotBucketed(): Unit = {
    if (numBuckets.isDefined || sortColumnNames.isDefined) {
      throw new IllegalArgumentException(
        "Currently we don't support writing bucketed data to this data source.")
    }
  }

  /**
   * Saves the content of the [[DataFrame]] as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   * When `mode` is `Overwrite`, the schema of the [[DataFrame]] does not need to be
   * the same as that of the existing table.
   * When `mode` is `Append`, the schema of the [[DataFrame]] need to be
   * the same as that of the existing table, and format or options will be ignored.
   *
   * When the DataFrame is created from a non-partitioned [[HadoopFsRelation]] with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @since 1.4.0
   */
  def saveAsTable(tableName: String): Unit = {
    saveAsTable(df.sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName))
  }

  private def saveAsTable(tableIdent: TableIdentifier): Unit = {
    assertNotStreaming("saveAsTable() can only be called on non-continuous queries")

    val tableExists = df.sparkSession.sessionState.catalog.tableExists(tableIdent)

    (tableExists, mode) match {
      case (true, SaveMode.Ignore) =>
        // Do nothing

      case (true, SaveMode.ErrorIfExists) =>
        throw new AnalysisException(s"Table $tableIdent already exists.")

      case _ =>
        val cmd =
          CreateTableUsingAsSelect(
            tableIdent,
            source,
            temporary = false,
            partitioningColumns.map(_.toArray).getOrElse(Array.empty[String]),
            getBucketSpec,
            mode,
            extraOptions.toMap,
            df.logicalPlan)
        df.sparkSession.executePlan(cmd).toRdd
    }
  }

  /**
   * Saves the content of the [[DataFrame]] to a external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included.
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    assertNotStreaming("jdbc() can only be called on non-continuous queries")

    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties)
    val conn = JdbcUtils.createConnectionFactory(url, props)()

    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(df, url)
        val sql = s"CREATE TABLE $table ($schema)"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } finally {
          statement.close()
        }
      }
    } finally {
      conn.close()
    }

    JdbcUtils.saveTable(df, url, table, props)
  }

  /**
   * Saves the content of the [[DataFrame]] in JSON format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * You can set the following JSON-specific option(s) for writing JSON files:
   * <li>`compression` (default `null`): compression codec to use when saving to file. This can be
   * one of the known case-insensitive shorten names (`none`, `bzip2`, `gzip`, `lz4`,
   * `snappy` and `deflate`). </li>
   *
   * @since 1.4.0
   */
  def json(path: String): Unit = {
    assertNotStreaming("json() can only be called on non-continuous queries")
    format("json").save(path)
  }

  /**
   * Saves the content of the [[DataFrame]] in Parquet format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * You can set the following Parquet-specific option(s) for writing Parquet files:
   * <li>`compression` (default `null`): compression codec to use when saving to file. This can be
   * one of the known case-insensitive shorten names(`none`, `snappy`, `gzip`, and `lzo`).
   * This will overwrite `spark.sql.parquet.compression.codec`. </li>
   *
   * @since 1.4.0
   */
  def parquet(path: String): Unit = {
    assertNotStreaming("parquet() can only be called on non-continuous queries")
    format("parquet").save(path)
  }

  /**
   * Saves the content of the [[DataFrame]] in ORC format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * You can set the following ORC-specific option(s) for writing ORC files:
   * <li>`compression` (default `null`): compression codec to use when saving to file. This can be
   * one of the known case-insensitive shorten names(`none`, `snappy`, `zlib`, and `lzo`).
   * This will overwrite `orc.compress`. </li>
   *
   * @since 1.5.0
   * @note Currently, this method can only be used together with `HiveContext`.
   */
  def orc(path: String): Unit = {
    assertNotStreaming("orc() can only be called on non-continuous queries")
    format("orc").save(path)
  }

  /**
   * Saves the content of the [[DataFrame]] in a text file at the specified path.
   * The DataFrame must have only one column that is of string type.
   * Each row becomes a new line in the output file. For example:
   * {{{
   *   // Scala:
   *   df.write.text("/path/to/output")
   *
   *   // Java:
   *   df.write().text("/path/to/output")
   * }}}
   *
   * You can set the following option(s) for writing text files:
   * <li>`compression` (default `null`): compression codec to use when saving to file. This can be
   * one of the known case-insensitive shorten names (`none`, `bzip2`, `gzip`, `lz4`,
   * `snappy` and `deflate`). </li>
   *
   * @since 1.6.0
   */
  def text(path: String): Unit = {
    assertNotStreaming("text() can only be called on non-continuous queries")
    format("text").save(path)
  }

  /**
   * Saves the content of the [[DataFrame]] in CSV format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("csv").save(path)
   * }}}
   *
   * You can set the following CSV-specific option(s) for writing CSV files:
   * <li>`sep` (default `,`): sets the single character as a separator for each
   * field and value.</li>
   * <li>`quote` (default `"`): sets the single character used for escaping quoted values where
   * the separator can be part of the value.</li>
   * <li>`escape` (default `\`): sets the single character used for escaping quotes inside
   * an already quoted value.</li>
   * <li>`header` (default `false`): writes the names of columns as the first line.</li>
   * <li>`nullValue` (default empty string): sets the string representation of a null value.</li>
   * <li>`compression` (default `null`): compression codec to use when saving to file. This can be
   * one of the known case-insensitive shorten names (`none`, `bzip2`, `gzip`, `lz4`,
   * `snappy` and `deflate`). </li>
   *
   * @since 2.0.0
   */
  def csv(path: String): Unit = {
    assertNotStreaming("csv() can only be called on non-continuous queries")
    format("csv").save(path)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sparkSession.sessionState.conf.defaultDataSourceName

  private var mode: SaveMode = SaveMode.ErrorIfExists

  private var trigger: Trigger = ProcessingTime(0L)

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

  private var partitioningColumns: Option[Seq[String]] = None

  private var bucketColumnNames: Option[Seq[String]] = None

  private var numBuckets: Option[Int] = None

  private var sortColumnNames: Option[Seq[String]] = None

  ///////////////////////////////////////////////////////////////////////////////////////
  // Helper functions
  ///////////////////////////////////////////////////////////////////////////////////////

  private def assertNotStreaming(errMsg: String): Unit = {
    if (df.isStreaming) {
      throw new AnalysisException(errMsg)
    }
  }

  private def assertStreaming(errMsg: String): Unit = {
    if (!df.isStreaming) {
      throw new AnalysisException(errMsg)
    }
  }

}
