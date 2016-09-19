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

import org.apache.spark.Partition
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.execution.datasources.json.{InferSchema, JacksonParser, JSONOptions}
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use [[SparkSession.read]] to access this.
 *
 * @since 1.4.0
 */
class DataFrameReader private[sql](sparkSession: SparkSession) extends Logging {

  /**
   * Specifies the input data source format.
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameReader = {
    this.source = source
    this
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 1.4.0
   */
  def schema(schema: StructType): DataFrameReader = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameReader = {
    this.extraOptions += (key -> value)
    this
  }

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataFrameReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataFrameReader = option(key, value.toString)

  /**
   * Adds an input option for the underlying data source.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataFrameReader = option(key, value.toString)

  /**
   * (Scala-specific) Adds input options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameReader = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds input options for the underlying data source.
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameReader = {
    this.options(options.asScala)
    this
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that don't require a path (e.g. external
   * key-value stores).
   *
   * @since 1.4.0
   */
  def load(): DataFrame = {
    load(Seq.empty: _*) // force invocation of `load(...varargs...)`
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that require a path (e.g. data backed by
   * a local or distributed file system).
   *
   * @since 1.4.0
   */
  def load(path: String): DataFrame = {
    option("path", path).load(Seq.empty: _*) // force invocation of `load(...varargs...)`
  }

  /**
   * Loads input in as a [[DataFrame]], for data sources that support multiple paths.
   * Only works if the source is a HadoopFsRelationProvider.
   *
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def load(paths: String*): DataFrame = {
    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = userSpecifiedSchema,
        className = source,
        options = extraOptions.toMap).resolveRelation())
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table and connection properties.
   *
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, properties: Properties): DataFrame = {
    jdbc(url, table, JDBCRelation.columnPartition(null), properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table. Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`.
   * @param table Name of the table in the external database.
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride.
   * @param upperBound the maximum value of `columnName` used to decide partition stride.
   * @param numPartitions the number of partitions. This, along with `lowerBound` (inclusive),
   *                      `upperBound` (exclusive), form partition strides for generated WHERE
   *                      clause expressions used to split the column `columnName` evenly.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "fetchsize" can be used to control the
   *                             number of rows per fetch.
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties): DataFrame = {
    val partitioning = JDBCPartitioningInfo(columnName, lowerBound, upperBound, numPartitions)
    val parts = JDBCRelation.columnPartition(partitioning)
    jdbc(url, table, parts, connectionProperties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table using connection properties. The `predicates` parameter gives a list
   * expressions suitable for inclusion in WHERE clauses; each one defines one partition
   * of the [[DataFrame]].
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * @param url JDBC database url of the form `jdbc:subprotocol:subname`
   * @param table Name of the table in the external database.
   * @param predicates Condition in the where clause for each partition.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "fetchsize" can be used to control the
   *                             number of rows per fetch.
   * @since 1.4.0
   */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: Properties): DataFrame = {
    val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i) : Partition
    }
    jdbc(url, table, parts, connectionProperties)
  }

  private def jdbc(
      url: String,
      table: String,
      parts: Array[Partition],
      connectionProperties: Properties): DataFrame = {
    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties)
    val relation = JDBCRelation(url, table, parts, props)(sparkSession)
    sparkSession.baseRelationToDataFrame(relation)
  }

  /**
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
   * See the documentation on the overloaded `json()` method with varargs for more details.
   *
   * @since 1.4.0
   */
  def json(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    json(Seq(path): _*)
  }

  /**
   * Loads a JSON file (one object per line) and returns the result as a [[DataFrame]].
   *
   * This function goes through the input once to determine the input schema. If you know the
   * schema in advance, use the version that specifies the schema to avoid the extra scan.
   *
   * You can set the following JSON-specific options to deal with non-standard JSON files:
   * <ul>
   * <li>`primitivesAsString` (default `false`): infers all primitive values as a string type</li>
   * <li>`prefersDecimal` (default `false`): infers all floating-point values as a decimal
   * type. If the values do not fit in decimal, then it infers them as doubles.</li>
   * <li>`allowComments` (default `false`): ignores Java/C++ style comment in JSON records</li>
   * <li>`allowUnquotedFieldNames` (default `false`): allows unquoted JSON field names</li>
   * <li>`allowSingleQuotes` (default `true`): allows single quotes in addition to double quotes
   * </li>
   * <li>`allowNumericLeadingZeros` (default `false`): allows leading zeros in numbers
   * (e.g. 00012)</li>
   * <li>`allowBackslashEscapingAnyCharacter` (default `false`): allows accepting quoting of all
   * character using backslash quoting mechanism</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   * during parsing.
   *   <ul>
   *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record, and puts
   *     the malformed string into a new field configured by `columnNameOfCorruptRecord`. When
   *     a schema is set by user, it sets `null` for extra fields.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * <li>`columnNameOfCorruptRecord` (default is the value specified in
   * `spark.sql.columnNameOfCorruptRecord`): allows renaming the new field having malformed string
   * created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * </ul>
   *
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def json(paths: String*): DataFrame = format("json").load(paths : _*)

  /**
   * Loads a `JavaRDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * Unless the schema is specified using [[schema]] function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: JavaRDD[String]): DataFrame = json(jsonRDD.rdd)

  /**
   * Loads an `RDD[String]` storing JSON objects (one object per record) and
   * returns the result as a [[DataFrame]].
   *
   * Unless the schema is specified using [[schema]] function, this function goes through the
   * input once to determine the input schema.
   *
   * @param jsonRDD input RDD with one JSON object per record
   * @since 1.4.0
   */
  def json(jsonRDD: RDD[String]): DataFrame = {
    val parsedOptions: JSONOptions = new JSONOptions(extraOptions.toMap)
    val columnNameOfCorruptRecord =
      parsedOptions.columnNameOfCorruptRecord
        .getOrElse(sparkSession.sessionState.conf.columnNameOfCorruptRecord)
    val schema = userSpecifiedSchema.getOrElse {
      InferSchema.infer(
        jsonRDD,
        columnNameOfCorruptRecord,
        parsedOptions)
    }
    val parsed = jsonRDD.mapPartitions { iter =>
      val parser = new JacksonParser(schema, columnNameOfCorruptRecord, parsedOptions)
      iter.flatMap(parser.parse)
    }

    Dataset.ofRows(
      sparkSession,
      LogicalRDD(schema.toAttributes, parsed)(sparkSession))
  }

  /**
   * Loads a CSV file and returns the result as a [[DataFrame]]. See the documentation on the
   * other overloaded `csv()` method for more details.
   *
   * @since 2.0.0
   */
  def csv(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    csv(Seq(path): _*)
  }

  /**
   * Loads a CSV file and returns the result as a [[DataFrame]].
   *
   * This function will go through the input once to determine the input schema if `inferSchema`
   * is enabled. To avoid going through the entire data once, disable `inferSchema` option or
   * specify the schema explicitly using [[schema]].
   *
   * You can set the following CSV-specific options to deal with CSV files:
   * <ul>
   * <li>`sep` (default `,`): sets the single character as a separator for each
   * field and value.</li>
   * <li>`encoding` (default `UTF-8`): decodes the CSV files by the given encoding
   * type.</li>
   * <li>`quote` (default `"`): sets the single character used for escaping quoted values where
   * the separator can be part of the value. If you would like to turn off quotations, you need to
   * set not `null` but an empty string. This behaviour is different form
   * `com.databricks.spark.csv`.</li>
   * <li>`escape` (default `\`): sets the single character used for escaping quotes inside
   * an already quoted value.</li>
   * <li>`comment` (default empty string): sets the single character used for skipping lines
   * beginning with this character. By default, it is disabled.</li>
   * <li>`header` (default `false`): uses the first line as names of columns.</li>
   * <li>`inferSchema` (default `false`): infers the input schema automatically from data. It
   * requires one extra pass over the data.</li>
   * <li>`ignoreLeadingWhiteSpace` (default `false`): defines whether or not leading whitespaces
   * from values being read should be skipped.</li>
   * <li>`ignoreTrailingWhiteSpace` (default `false`): defines whether or not trailing
   * whitespaces from values being read should be skipped.</li>
   * <li>`nullValue` (default empty string): sets the string representation of a null value. Since
   * 2.0.1, this applies to all supported types including the string type.</li>
   * <li>`nanValue` (default `NaN`): sets the string representation of a non-number" value.</li>
   * <li>`positiveInf` (default `Inf`): sets the string representation of a positive infinity
   * value.</li>
   * <li>`negativeInf` (default `-Inf`): sets the string representation of a negative infinity
   * value.</li>
   * <li>`dateFormat` (default `yyyy-MM-dd`): sets the string that indicates a date format.
   * Custom date formats follow the formats at `java.text.SimpleDateFormat`. This applies to
   * date type.</li>
   * <li>`timestampFormat` (default `yyyy-MM-dd'T'HH:mm:ss.SSSZZ`): sets the string that
   * indicates a timestamp format. Custom date formats follow the formats at
   * `java.text.SimpleDateFormat`. This applies to timestamp type.</li>
   * `java.sql.Timestamp.valueOf()` and `java.sql.Date.valueOf()` or ISO 8601 format.</li>
   * <li>`maxColumns` (default `20480`): defines a hard limit of how many columns
   * a record can have.</li>
   * <li>`maxCharsPerColumn` (default `1000000`): defines the maximum number of characters allowed
   * for any given value being read.</li>
   * <li>`maxMalformedLogPerPartition` (default `10`): sets the maximum number of malformed rows
   * Spark will log for each partition. Malformed records beyond this number will be ignored.</li>
   * <li>`mode` (default `PERMISSIVE`): allows a mode for dealing with corrupt records
   *    during parsing.
   *   <ul>
   *     <li>`PERMISSIVE` : sets other fields to `null` when it meets a corrupted record. When
   *       a schema is set by user, it sets `null` for extra fields.</li>
   *     <li>`DROPMALFORMED` : ignores the whole corrupted records.</li>
   *     <li>`FAILFAST` : throws an exception when it meets corrupted records.</li>
   *   </ul>
   * </li>
   * </ul>
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def csv(paths: String*): DataFrame = format("csv").load(paths : _*)

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. See the documentation
   * on the other overloaded `parquet()` method for more details.
   *
   * @since 2.0.0
   */
  def parquet(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    parquet(Seq(path): _*)
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]].
   *
   * You can set the following Parquet-specific option(s) for reading Parquet files:
   * <ul>
   * <li>`mergeSchema` (default is the value specified in `spark.sql.parquet.mergeSchema`): sets
   * whether we should merge schemas collected from all Parquet part-files. This will override
   * `spark.sql.parquet.mergeSchema`.</li>
   * </ul>
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def parquet(paths: String*): DataFrame = {
    format("parquet").load(paths: _*)
  }

  /**
   * Loads an ORC file and returns the result as a [[DataFrame]].
   *
   * @param path input path
   * @since 1.5.0
   * @note Currently, this method can only be used after enabling Hive support.
   */
  def orc(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    orc(Seq(path): _*)
  }

  /**
   * Loads an ORC file and returns the result as a [[DataFrame]].
   *
   * @param paths input paths
   * @since 2.0.0
   * @note Currently, this method can only be used after enabling Hive support.
   */
  @scala.annotation.varargs
  def orc(paths: String*): DataFrame = format("orc").load(paths: _*)

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @since 1.4.0
   */
  def table(tableName: String): DataFrame = {
    Dataset.ofRows(sparkSession,
      sparkSession.sessionState.catalog.lookupRelation(
        sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)))
  }

  /**
   * Loads text files and returns a [[DataFrame]] whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any. See the documentation on
   * the other overloaded `text()` method for more details.
   *
   * @since 2.0.0
   */
  def text(path: String): DataFrame = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    text(Seq(path): _*)
  }

  /**
   * Loads text files and returns a [[DataFrame]] whose schema starts with a string column named
   * "value", and followed by partitioned columns if there are any.
   *
   * Each line in the text files is a new row in the resulting DataFrame. For example:
   * {{{
   *   // Scala:
   *   spark.read.text("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.read().text("/path/to/spark/README.md")
   * }}}
   *
   * @param paths input paths
   * @since 1.6.0
   */
  @scala.annotation.varargs
  def text(paths: String*): DataFrame = format("text").load(paths : _*)

  /**
   * Loads text files and returns a [[Dataset]] of String. See the documentation on the
   * other overloaded `textFile()` method for more details.
   * @since 2.0.0
   */
  def textFile(path: String): Dataset[String] = {
    // This method ensures that calls that explicit need single argument works, see SPARK-16009
    textFile(Seq(path): _*)
  }

  /**
   * Loads text files and returns a [[Dataset]] of String. The underlying schema of the Dataset
   * contains a single string column named "value".
   *
   * If the directory structure of the text files contains partitioning information, those are
   * ignored in the resulting Dataset. To include partitioning information as columns, use `text`.
   *
   * Each line in the text files is a new element in the resulting Dataset. For example:
   * {{{
   *   // Scala:
   *   spark.read.textFile("/path/to/spark/README.md")
   *
   *   // Java:
   *   spark.read().textFile("/path/to/spark/README.md")
   * }}}
   *
   * @param paths input path
   * @since 2.0.0
   */
  @scala.annotation.varargs
  def textFile(paths: String*): Dataset[String] = {
    if (userSpecifiedSchema.nonEmpty) {
      throw new AnalysisException("User specified schema not supported with `textFile`")
    }
    text(paths : _*).select("value").as[String](sparkSession.implicits.newStringEncoder)
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var extraOptions = new scala.collection.mutable.HashMap[String, String]

}
