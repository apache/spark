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

import java.util.{Locale, Properties}

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Interface used to write a [[Dataset]] to external storage systems (e.g. file systems, key-value
 * stores, etc). Use `Dataset.write` to access this.
 *
 * @since 3.4.0
 */
@Stable
final class DataFrameWriter[T] private[sql] (ds: Dataset[T]) {

  /**
   * Specifies the behavior when data or table already exists. Options include: <ul>
   * <li>`SaveMode.Overwrite`: overwrite the existing data.</li> <li>`SaveMode.Append`: append the
   * data.</li> <li>`SaveMode.Ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`SaveMode.ErrorIfExists`: throw an exception at runtime.</li> </ul> <p> The default
   * option is `ErrorIfExists`.
   *
   * @since 3.4.0
   */
  def mode(saveMode: SaveMode): DataFrameWriter[T] = {
    this.mode = saveMode
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include: <ul>
   * <li>`overwrite`: overwrite the existing data.</li> <li>`append`: append the data.</li>
   * <li>`ignore`: ignore the operation (i.e. no-op).</li> <li>`error` or `errorifexists`: default
   * option, throw an exception at runtime.</li> </ul>
   *
   * @since 3.4.0
   */
  def mode(saveMode: String): DataFrameWriter[T] = {
    saveMode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => mode(SaveMode.Overwrite)
      case "append" => mode(SaveMode.Append)
      case "ignore" => mode(SaveMode.Ignore)
      case "error" | "errorifexists" | "default" => mode(SaveMode.ErrorIfExists)
      case _ =>
        throw new IllegalArgumentException(s"Unknown save mode: $saveMode. Accepted " +
          "save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'.")
    }
  }

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   *
   * @since 3.4.0
   */
  def format(source: String): DataFrameWriter[T] = {
    this.source = Some(source)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names. If a new option
   * has the same key case-insensitively, it will override the existing option.
   *
   * @since 3.4.0
   */
  def option(key: String, value: String): DataFrameWriter[T] = {
    this.extraOptions = this.extraOptions + (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names. If a new option
   * has the same key case-insensitively, it will override the existing option.
   *
   * @since 3.4.0
   */
  def option(key: String, value: Boolean): DataFrameWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names. If a new option
   * has the same key case-insensitively, it will override the existing option.
   *
   * @since 3.4.0
   */
  def option(key: String, value: Long): DataFrameWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names. If a new option
   * has the same key case-insensitively, it will override the existing option.
   *
   * @since 3.4.0
   */
  def option(key: String, value: Double): DataFrameWriter[T] = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names. If a new option
   * has the same key case-insensitively, it will override the existing option.
   *
   * @since 3.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names. If a new option
   * has the same key case-insensitively, it will override the existing option.
   *
   * @since 3.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like: <ul>
   * <li>year=2016/month=01/</li> <li>year=2016/month=02/</li> </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout. It
   * provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number of
   * distinct values in each column should typically be less than tens of thousands.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataFrameWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Buckets the output by the given columns. If specified, the output is laid out on the file
   * system similar to Hive's bucketing scheme, but with a different bucket hash function and is
   * not compatible with Hive's bucketing.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter[T] = {
    require(numBuckets > 0, "The numBuckets should be > 0.")
    this.numBuckets = Option(numBuckets)
    this.bucketColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Sorts the output in each bucket by the given columns.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 3.4.0
   */
  @scala.annotation.varargs
  def sortBy(colName: String, colNames: String*): DataFrameWriter[T] = {
    this.sortColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Saves the content of the `DataFrame` at the specified path.
   *
   * @since 3.4.0
   */
  def save(path: String): Unit = {
    saveInternal(Some(path))
  }

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * @since 3.4.0
   */
  def save(): Unit = saveInternal(None)

  private def saveInternal(path: Option[String]): Unit = {
    executeWriteOperation(builder => path.foreach(builder.setPath))
  }

  private def executeWriteOperation(f: proto.WriteOperation.Builder => Unit): Unit = {
    val builder = proto.WriteOperation.newBuilder()

    builder.setInput(ds.plan.getRoot)

    // Set path or table
    f(builder)

    // Cannot both be set
    require(!(builder.hasPath && builder.hasTable))

    builder.setMode(mode match {
      case SaveMode.Append => proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
      case SaveMode.Overwrite => proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
      case SaveMode.Ignore => proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE
      case SaveMode.ErrorIfExists => proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
    })

    source.foreach(builder.setSource)
    sortColumnNames.foreach(names => builder.addAllSortColumnNames(names.asJava))
    partitioningColumns.foreach(cols => builder.addAllPartitioningColumns(cols.asJava))

    numBuckets.foreach(n => {
      val bucketBuilder = proto.WriteOperation.BucketBy.newBuilder()
      bucketBuilder.setNumBuckets(n)
      bucketColumnNames.foreach(names => bucketBuilder.addAllBucketColumnNames(names.asJava))
      builder.setBucketBy(bucketBuilder)
    })

    extraOptions.foreach { case (k, v) =>
      builder.putOptions(k, v)
    }

    ds.sparkSession.execute(proto.Command.newBuilder().setWriteOperation(builder).build())
  }

  /**
   * Inserts the content of the `DataFrame` to the specified table. It requires that the schema of
   * the `DataFrame` is the same as the schema of the table.
   *
   * @note
   *   Unlike `saveAsTable`, `insertInto` ignores the column names and just uses position-based
   *   resolution. For example:
   *
   * @note
   *   SaveMode.ErrorIfExists and SaveMode.Ignore behave as SaveMode.Append in `insertInto` as
   *   `insertInto` is not a table creating operation.
   *
   * {{{
   *    scala> Seq((1, 2)).toDF("i", "j").write.mode("overwrite").saveAsTable("t1")
   *    scala> Seq((3, 4)).toDF("j", "i").write.insertInto("t1")
   *    scala> Seq((5, 6)).toDF("a", "b").write.insertInto("t1")
   *    scala> sql("select * from t1").show
   *    +---+---+
   *    |  i|  j|
   *    +---+---+
   *    |  5|  6|
   *    |  3|  4|
   *    |  1|  2|
   *    +---+---+
   * }}}
   *
   * Because it inserts data to an existing table, format or options will be ignored.
   *
   * @since 3.4.0
   */
  def insertInto(tableName: String): Unit = {
    executeWriteOperation(builder => {
      builder.setTable(
        proto.WriteOperation.SaveTable
          .newBuilder()
          .setTableName(tableName)
          .setSaveMethod(
            proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO))
    })
  }

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the save mode,
   * specified by the `mode` function (default to throwing an exception). When `mode` is
   * `Overwrite`, the schema of the `DataFrame` does not need to be the same as that of the
   * existing table.
   *
   * When `mode` is `Append`, if there is an existing table, we will use the format and options of
   * the existing table. The column order in the schema of the `DataFrame` doesn't need to be same
   * as that of the existing table. Unlike `insertInto`, `saveAsTable` will use the column names
   * to find the correct column positions. For example:
   *
   * {{{
   *    scala> Seq((1, 2)).toDF("i", "j").write.mode("overwrite").saveAsTable("t1")
   *    scala> Seq((3, 4)).toDF("j", "i").write.mode("append").saveAsTable("t1")
   *    scala> sql("select * from t1").show
   *    +---+---+
   *    |  i|  j|
   *    +---+---+
   *    |  1|  2|
   *    |  4|  3|
   *    +---+---+
   * }}}
   *
   * In this method, save mode is used to determine the behavior if the data source table exists
   * in Spark catalog. We will always overwrite the underlying data of data source (e.g. a table
   * in JDBC data source) if the table doesn't exist in Spark catalog, and will always append to
   * the underlying data of data source if the table already exists.
   *
   * When the DataFrame is created from a non-partitioned `HadoopFsRelation` with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @since 3.4.0
   */
  def saveAsTable(tableName: String): Unit = {
    executeWriteOperation(builder => {
      builder.setTable(
        proto.WriteOperation.SaveTable
          .newBuilder()
          .setTableName(tableName)
          .setSaveMethod(
            proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE))
    })
  }

  /**
   * Saves the content of the `DataFrame` to an external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the save
   * mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * JDBC-specific option and parameter documentation for storing tables via JDBC in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @param table
   *   Name of the table in the external database.
   * @param connectionProperties
   *   JDBC database connection arguments, a list of arbitrary string tag/value. Normally at least
   *   a "user" and "password" property should be included. "batchsize" can be used to control the
   *   number of rows per insert. "isolationLevel" can be one of "NONE", "READ_COMMITTED",
   *   "READ_UNCOMMITTED", "REPEATABLE_READ", or "SERIALIZABLE", corresponding to standard
   *   transaction isolation levels defined by JDBC's Connection object, with default of
   *   "READ_UNCOMMITTED".
   * @since 3.4.0
   */
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    // connectionProperties should override settings in extraOptions.
    this.extraOptions ++= connectionProperties.asScala
    // explicit url and dbtable should override all
    this.extraOptions ++= Seq("url" -> url, "dbtable" -> table)
    format("jdbc").save()
  }

  /**
   * Saves the content of the `DataFrame` in JSON format (<a href="http://jsonlines.org/"> JSON
   * Lines text format or newline-delimited JSON</a>) at the specified path. This is equivalent
   * to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * You can find the JSON-specific options for writing JSON files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 3.4.0
   */
  def json(path: String): Unit = {
    format("json").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in Parquet format at the specified path. This is
   * equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * Parquet-specific option(s) for writing Parquet files can be found in <a href=
   * "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option"> Data
   * Source Option</a> in the version you use.
   *
   * @since 3.4.0
   */
  def parquet(path: String): Unit = {
    format("parquet").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in ORC format at the specified path. This is equivalent
   * to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * ORC-specific option(s) for writing ORC files can be found in <a href=
   * "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option"> Data
   * Source Option</a> in the version you use.
   *
   * @since 3.4.0
   */
  def orc(path: String): Unit = {
    format("orc").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in a text file at the specified path. The DataFrame must
   * have only one column that is of string type. Each row becomes a new line in the output file.
   * For example:
   * {{{
   *   // Scala:
   *   df.write.text("/path/to/output")
   *
   *   // Java:
   *   df.write().text("/path/to/output")
   * }}}
   * The text files will be encoded as UTF-8.
   *
   * You can find the text-specific options for writing text files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 3.4.0
   */
  def text(path: String): Unit = {
    format("text").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in CSV format at the specified path. This is equivalent
   * to:
   * {{{
   *   format("csv").save(path)
   * }}}
   *
   * You can find the CSV-specific options for writing CSV files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 3.4.0
   */
  def csv(path: String): Unit = {
    format("csv").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in XML format at the specified path. This is equivalent
   * to:
   * {{{
   *   format("xml").save(path)
   * }}}
   *
   * Note that writing a XML file from `DataFrame` having a field `ArrayType` with its element as
   * `ArrayType` would have an additional nested field for the element.
   *
   * Namely, roundtrip in writing and reading can end up in different schema structure.
   *
   * You can find the XML-specific options for writing XML files in <a
   * href="https://spark.apache.org/docs/latest/sql-data-sources-xml.html#data-source-option">
   * Data Source Option</a> in the version you use.
   *
   * @since 4.0.0
   */
  def xml(path: String): Unit = {
    format("xml").save(path)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: Option[String] = None

  private var mode: SaveMode = SaveMode.ErrorIfExists

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  private var partitioningColumns: Option[Seq[String]] = None

  private var bucketColumnNames: Option[Seq[String]] = None

  private var numBuckets: Option[Int] = None

  private var sortColumnNames: Option[Seq[String]] = None
}
