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

package org.apache.spark.sql.classic

import java.util.Properties

import scala.jdk.CollectionConverters._

import org.apache.spark.Partition
import org.apache.spark.annotation.Stable
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.DataSourceOptions
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions, UnivocityParser}
import org.apache.spark.sql.catalyst.expressions.ExprUtils
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JacksonParser, JSONOptions}
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedDataSource
import org.apache.spark.sql.catalyst.util.FailureSafeParser
import org.apache.spark.sql.catalyst.xml.{StaxXmlParser, XmlOptions}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.datasources.csv._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRelation}
import org.apache.spark.sql.execution.datasources.json.JsonUtils.checkJsonSchema
import org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource
import org.apache.spark.sql.execution.datasources.xml.TextInputXmlDataSource
import org.apache.spark.sql.execution.datasources.xml.XmlUtils.checkXmlSchema
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.read` to access this.
 *
 * @since 1.4.0
 */
@Stable
class DataFrameReader private[sql](sparkSession: SparkSession)
  extends sql.DataFrameReader {

  format(sparkSession.sessionState.conf.defaultDataSourceName)

  /** @inheritdoc */
  override def format(source: String): this.type = super.format(source)

  /** @inheritdoc */
  override def schema(schema: StructType): this.type = super.schema(schema)

  /** @inheritdoc */
  override def schema(schemaString: String): this.type = super.schema(schemaString)

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type = super.options(options)

  /** @inheritdoc */
  override def load(): DataFrame = load(Nil: _*)

  /** @inheritdoc */
  def load(path: String): DataFrame = {
    // force invocation of `load(...varargs...)`
    if (sparkSession.sessionState.conf.legacyPathOptionBehavior) {
      option("path", path).load(Seq.empty: _*)
    } else {
      load(Seq(path): _*)
    }
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def load(paths: String*): DataFrame = {
    Dataset.ofRows(
      sparkSession,
      UnresolvedDataSource(source, userSpecifiedSchema, extraOptions, isStreaming = false, paths)
    )
  }

  /** @inheritdoc */
  override def jdbc(url: String, table: String, properties: Properties): DataFrame =
    super.jdbc(url, table, properties)

  /** @inheritdoc */
  override def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties): DataFrame =
    super.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, connectionProperties)

  /** @inheritdoc */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: Properties): DataFrame = {
    assertNoSpecifiedSchema("jdbc")
    // connectionProperties should override settings in extraOptions.
    val params = extraOptions ++ connectionProperties.asScala
    val options = new JDBCOptions(url, table, params)
    val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i): Partition
    }
    val relation = JDBCRelation(parts, options)(sparkSession)
    sparkSession.baseRelationToDataFrame(relation)
  }

  /** @inheritdoc */
  override def json(path: String): DataFrame = super.json(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def json(paths: String*): DataFrame = super.json(paths: _*)

  /** @inheritdoc */
  @deprecated("Use json(Dataset[String]) instead.", "2.2.0")
  def json(jsonRDD: JavaRDD[String]): DataFrame = json(jsonRDD.rdd)

  /** @inheritdoc */
  @deprecated("Use json(Dataset[String]) instead.", "2.2.0")
  def json(jsonRDD: RDD[String]): DataFrame = {
    json(sparkSession.createDataset(jsonRDD)(Encoders.STRING))
  }

  /** @inheritdoc */
  def json(jsonDataset: sql.Dataset[String]): DataFrame = {
    val parsedOptions = new JSONOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    userSpecifiedSchema.foreach(checkJsonSchema)
    val schema = userSpecifiedSchema.map {
      case s if !SQLConf.get.getConf(
        SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
      case other => other
    }.getOrElse {
      TextInputJsonDataSource.inferFromDataset(jsonDataset, parsedOptions)
    }

    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val createParser = CreateJacksonParser.string _
    val parsed = jsonDataset.rdd.mapPartitions { iter =>
      val rawParser = new JacksonParser(actualSchema, parsedOptions, allowArrayAsStructs = true)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input, createParser, UTF8String.fromString),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = jsonDataset.isStreaming)
  }

  /** @inheritdoc */
  override def csv(path: String): DataFrame = super.csv(path)

  /** @inheritdoc */
  def csv(csvDataset: sql.Dataset[String]): DataFrame = {
    val parsedOptions: CSVOptions = new CSVOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    val filteredLines: Dataset[String] =
      CSVUtils.filterCommentAndEmpty(csvDataset, parsedOptions)

    // For performance, short-circuit the collection of the first line when it won't be used:
    //   - TextInputCSVDataSource - Only uses firstLine to infer an unspecified schema
    //   - CSVHeaderChecker       - Only uses firstLine to check header, when headerFlag is true
    //   - CSVUtils               - Only uses firstLine to filter headers, when headerFlag is true
    // (If the downstream logic grows more complicated, consider refactoring to an approach that
    //  delegates this decision to the constituent consumers themselves.)
    val maybeFirstLine: Option[String] =
      if (userSpecifiedSchema.isEmpty || parsedOptions.headerFlag) {
        filteredLines.take(1).headOption
      } else {
        None
      }

    val schema = userSpecifiedSchema.map {
      case s if !SQLConf.get.getConf(
        SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
      case other => other
    }.getOrElse {
      TextInputCSVDataSource.inferFromDataset(
        sparkSession,
        csvDataset,
        maybeFirstLine,
        parsedOptions)
    }

    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val linesWithoutHeader: RDD[String] = maybeFirstLine.map { firstLine =>
      val headerChecker = new CSVHeaderChecker(
        actualSchema,
        parsedOptions,
        source = s"CSV source: $csvDataset")
      headerChecker.checkHeaderColumnNames(firstLine)
      filteredLines.rdd.mapPartitions(CSVUtils.filterHeaderLine(_, firstLine, parsedOptions))
    }.getOrElse(filteredLines.rdd)

    val parsed = linesWithoutHeader.mapPartitions { iter =>
      val rawParser = new UnivocityParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = csvDataset.isStreaming)
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def csv(paths: String*): DataFrame = super.csv(paths: _*)

  /** @inheritdoc */
  override def xml(path: String): DataFrame = super.xml(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def xml(paths: String*): DataFrame = super.xml(paths: _*)

  /** @inheritdoc */
  def xml(xmlDataset: sql.Dataset[String]): DataFrame = {
    val parsedOptions: XmlOptions = new XmlOptions(
      extraOptions.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord)

    userSpecifiedSchema.foreach(checkXmlSchema)

    val schema = userSpecifiedSchema.map {
      case s if !SQLConf.get.getConf(
        SQLConf.LEGACY_RESPECT_NULLABILITY_IN_TEXT_DATASET_CONVERSION) => s.asNullable
      case other => other
    }.getOrElse {
      TextInputXmlDataSource.inferFromDataset(xmlDataset, parsedOptions)
    }

    ExprUtils.verifyColumnNameOfCorruptRecord(schema, parsedOptions.columnNameOfCorruptRecord)
    val actualSchema =
      StructType(schema.filterNot(_.name == parsedOptions.columnNameOfCorruptRecord))

    val parsed = xmlDataset.rdd.mapPartitions { iter =>
      val rawParser = new StaxXmlParser(actualSchema, parsedOptions)
      val parser = new FailureSafeParser[String](
        input => rawParser.parse(input),
        parsedOptions.parseMode,
        schema,
        parsedOptions.columnNameOfCorruptRecord)
      iter.flatMap(parser.parse)
    }
    sparkSession.internalCreateDataFrame(parsed, schema, isStreaming = xmlDataset.isStreaming)
  }

  /** @inheritdoc */
  override def parquet(path: String): DataFrame = super.parquet(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def parquet(paths: String*): DataFrame = super.parquet(paths: _*)

  /** @inheritdoc */
  override def orc(path: String): DataFrame = super.orc(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def orc(paths: String*): DataFrame = super.orc(paths: _*)

  /** @inheritdoc */
  def table(tableName: String): DataFrame = {
    assertNoSpecifiedSchema("table")
    val multipartIdentifier =
      sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    Dataset.ofRows(sparkSession, UnresolvedRelation(multipartIdentifier,
      new CaseInsensitiveStringMap(extraOptions.toMap.asJava)))
  }

  /** @inheritdoc */
  override def text(path: String): DataFrame = super.text(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def text(paths: String*): DataFrame = super.text(paths: _*)

  /** @inheritdoc */
  override def textFile(path: String): Dataset[String] = super.textFile(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def textFile(paths: String*): Dataset[String] = super.textFile(paths: _*)

  /** @inheritdoc */
  override protected def validateSingleVariantColumn(): Unit =
    DataSourceOptions.validateSingleVariantColumn(extraOptions, userSpecifiedSchema)

  override protected def validateJsonSchema(): Unit =
    userSpecifiedSchema.foreach(checkJsonSchema)

  override protected def validateXmlSchema(): Unit =
    userSpecifiedSchema.foreach(checkXmlSchema)
}
