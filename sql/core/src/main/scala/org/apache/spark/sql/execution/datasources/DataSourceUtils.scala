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

package org.apache.spark.sql.execution.datasources

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.{SparkException, SparkUpgradeException}
import org.apache.spark.sql.{SPARK_LEGACY_DATETIME_METADATA_KEY, SPARK_LEGACY_INT96_METADATA_KEY, SPARK_TIMEZONE_METADATA_KEY, SPARK_VERSION_METADATA_KEY}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, ExpressionSet, GetStructField, PredicateHelper}
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, SchemaUtils}
import org.apache.spark.util.Utils


object DataSourceUtils extends PredicateHelper {
  /**
   * The key to use for storing partitionBy columns as options.
   */
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"

  /**
   * The key to use for specifying partition overwrite mode when
   * INSERT OVERWRITE a partitioned data source table.
   */
  val PARTITION_OVERWRITE_MODE = "partitionOverwriteMode"

  /**
   * Utility methods for converting partitionBy columns to options and back.
   */
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def encodePartitioningColumns(columns: Seq[String]): String = {
    Serialization.write(columns)
  }

  def decodePartitioningColumns(str: String): Seq[String] = {
    Serialization.read[Seq[String]](str)
  }

  /**
   * Verify if the field name is supported in datasource. This verification should be done
   * in a driver side.
   */
  def checkFieldNames(format: FileFormat, schema: StructType): Unit = {
    schema.foreach { field =>
      if (!format.supportFieldName(field.name)) {
        throw QueryCompilationErrors.invalidColumnNameAsPathError(
          format.getClass.getSimpleName, field.name)
      }
      field.dataType match {
        case s: StructType => checkFieldNames(format, s)
        case _ =>
      }
    }
  }

  /**
   * Verify if the schema is supported in datasource. This verification should be done
   * in a driver side.
   */
  def verifySchema(format: FileFormat, schema: StructType): Unit = {
    schema.foreach { field =>
      if (!format.supportDataType(field.dataType)) {
        throw QueryCompilationErrors.dataTypeUnsupportedByDataSourceError(format.toString, field)
      }
    }
  }

  // SPARK-24626: Metadata files and temporary files should not be
  // counted as data files, so that they shouldn't participate in tasks like
  // location size calculation.
  private[sql] def isDataPath(path: Path): Boolean = isDataFile(path.getName)

  private[sql] def isDataFile(fileName: String) =
    !(fileName.startsWith("_") || fileName.startsWith("."))

  /**
   * Returns if the given relation's V1 datasource provider supports nested predicate pushdown.
   */
  private[sql] def supportNestedPredicatePushdown(relation: BaseRelation): Boolean =
    relation match {
      case hs: HadoopFsRelation =>
        val supportedDatasources =
          Utils.stringToSeq(SQLConf.get.getConf(SQLConf.NESTED_PREDICATE_PUSHDOWN_FILE_SOURCE_LIST)
            .toLowerCase(Locale.ROOT))
        supportedDatasources.contains(hs.toString)
      case _ => false
    }

  private def getRebaseSpec(
      lookupFileMeta: String => String,
      modeByConfig: String,
      minVersion: String,
      metadataKey: String): RebaseSpec = {
    val policy = if (Utils.isTesting &&
      SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      LegacyBehaviorPolicy.CORRECTED
    } else {
      // If there is no version, we return the mode specified by the config.
      Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
        // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
        // rebase the datetime values.
        // Files written by `minVersion` and latter may also need the rebase if they were written
        // with the "LEGACY" rebase mode.
        if (version < minVersion || lookupFileMeta(metadataKey) != null) {
          LegacyBehaviorPolicy.LEGACY
        } else {
          LegacyBehaviorPolicy.CORRECTED
        }
      }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
    }
    policy match {
      case LegacyBehaviorPolicy.LEGACY =>
        RebaseSpec(LegacyBehaviorPolicy.LEGACY, Option(lookupFileMeta(SPARK_TIMEZONE_METADATA_KEY)))
      case _ => RebaseSpec(policy)
    }
  }

  def datetimeRebaseSpec(
      lookupFileMeta: String => String,
      modeByConfig: String): RebaseSpec = {
    getRebaseSpec(
      lookupFileMeta,
      modeByConfig,
      "3.0.0",
      SPARK_LEGACY_DATETIME_METADATA_KEY)
  }

  def int96RebaseSpec(
      lookupFileMeta: String => String,
      modeByConfig: String): RebaseSpec = {
    getRebaseSpec(
      lookupFileMeta,
      modeByConfig,
      "3.1.0",
      SPARK_LEGACY_INT96_METADATA_KEY)
  }

  def newRebaseExceptionInRead(format: String): SparkUpgradeException = {
    val (config, option) = format match {
      case "Parquet INT96" =>
        (SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key, ParquetOptions.INT96_REBASE_MODE)
      case "Parquet" =>
        (SQLConf.PARQUET_REBASE_MODE_IN_READ.key, ParquetOptions.DATETIME_REBASE_MODE)
      case "Avro" =>
        (SQLConf.AVRO_REBASE_MODE_IN_READ.key, "datetimeRebaseMode")
      case _ => throw SparkException.internalError(s"Unrecognized format $format.")
    }
    QueryExecutionErrors.sparkUpgradeInReadingDatesError(format, config, option)
  }

  def newRebaseExceptionInWrite(format: String): SparkUpgradeException = {
    val config = format match {
      case "Parquet INT96" => SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key
      case "Parquet" => SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key
      case "Avro" => SQLConf.AVRO_REBASE_MODE_IN_WRITE.key
      case _ => throw SparkException.internalError(s"Unrecognized format $format.")
    }
    QueryExecutionErrors.sparkUpgradeInWritingDatesError(format, config)
  }

  def createDateRebaseFuncInRead(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => days: Int =>
      if (days < RebaseDateTime.lastSwitchJulianDay) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def createDateRebaseFuncInWrite(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Int => Int = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => days: Int =>
      if (days < RebaseDateTime.lastSwitchGregorianDay) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      days
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseGregorianToJulianDays
    case LegacyBehaviorPolicy.CORRECTED => identity[Int]
  }

  def createTimestampRebaseFuncInRead(
      rebaseSpec: RebaseSpec,
      format: String): Long => Long = rebaseSpec.mode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchJulianTs) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY =>
      RebaseDateTime.rebaseJulianToGregorianMicros(rebaseSpec.timeZone, _)
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }

  def createTimestampRebaseFuncInWrite(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchGregorianTs) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY =>
      val timeZone = SQLConf.get.sessionLocalTimeZone
      RebaseDateTime.rebaseGregorianToJulianMicros(timeZone, _)
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }

  def generateDatasourceOptions(
      extraOptions: CaseInsensitiveStringMap, table: CatalogTable): Map[String, String] = {
    val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    val options = table.storage.properties ++ pathOption
    if (!SQLConf.get.getConf(SQLConf.LEGACY_EXTRA_OPTIONS_BEHAVIOR)) {
      // Check the same key with different values
      table.storage.properties.foreach { case (k, v) =>
        if (extraOptions.containsKey(k) && extraOptions.get(k) != v) {
          throw QueryCompilationErrors.failToResolveDataSourceForTableError(table, k)
        }
      }
      // To keep the original key from table properties, here we filter all case insensitive
      // duplicate keys out from extra options.
      val lowerCasedDuplicatedKeys =
        table.storage.properties.keySet.map(_.toLowerCase(Locale.ROOT))
          .intersect(extraOptions.keySet.asScala)
      extraOptions.asCaseSensitiveMap().asScala.filterNot {
        case (k, _) => lowerCasedDuplicatedKeys.contains(k.toLowerCase(Locale.ROOT))
      }.toMap ++ options
    } else {
      options
    }
  }

  def getPartitionFiltersAndDataFilters(
      partitionSchema: StructType,
      normalizedFilters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    val partitionColumns = normalizedFilters.flatMap { expr =>
      expr.collect {
        case attr: AttributeReference if partitionSchema.names.contains(attr.name) =>
          attr
      }
    }
    val partitionSet = AttributeSet(partitionColumns)
    val (partitionFilters, dataFilters) = normalizedFilters.partition(f =>
      f.references.nonEmpty && f.references.subsetOf(partitionSet)
    )
    val extraPartitionFilter =
      dataFilters.flatMap(extractPredicatesWithinOutputSet(_, partitionSet))
    (ExpressionSet(partitionFilters ++ extraPartitionFilter).toSeq, dataFilters)
  }

  /**
   * Determines whether a filter should be pushed down to the data source or not.
   *
   * @param expression The filter expression to be evaluated.
   * @param isCollationPushDownSupported Whether the data source supports collation push down.
   * @return A boolean indicating whether the filter should be pushed down or not.
   */
  def shouldPushFilter(expression: Expression, isCollationPushDownSupported: Boolean): Boolean = {
    if (!expression.deterministic) return false

    isCollationPushDownSupported || !expression.exists {
      case childExpression @ (_: Attribute | _: GetStructField) =>
        // don't push down filters for types with non-binary sortable collation
        // as it could lead to incorrect results
        SchemaUtils.hasNonBinarySortableCollatedString(childExpression.dataType)

      case _ => false
    }
  }
}
