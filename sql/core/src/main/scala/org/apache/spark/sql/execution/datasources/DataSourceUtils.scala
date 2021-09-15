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

import java.math.{BigDecimal => JavaBigDecimal, BigInteger => JavaBigInteger}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.{sources, SPARK_LEGACY_DATETIME, SPARK_LEGACY_INT96, SPARK_VERSION_METADATA_KEY}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, ExpressionSet, PredicateHelper}
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, RebaseDateTime}
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse => V2AlwaysFalse, AlwaysTrue => V2AlwaysTrue, And => V2And, EqualNullSafe => V2EqualNullSafe, EqualTo => V2EqualTo, Filter => V2Filter, GreaterThan => V2GreaterThan, GreaterThanOrEqual => V2GreaterThanOrEqual, In => V2In, IsNotNull => V2IsNotNull, IsNull => V2IsNull, LessThan => V2LessThan, LessThanOrEqual => V2LessThanOrEqual, Not => V2Not, Or => V2Or, StringContains => V2StringContains, StringEndsWith => V2StringEndsWith, StringStartsWith => V2StringStartsWith}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

object DataSourceUtils extends PredicateHelper {
  /**
   * The key to use for storing partitionBy columns as options.
   */
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"

  /**
   * Utility methods for converting partitionBy columns to options and back.
   */
  private implicit val formats = Serialization.formats(NoTypeHints)

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
        throw QueryCompilationErrors.columnNameContainsInvalidCharactersError(field.name)
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
    checkFieldNames(format, schema)
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

  def datetimeRebaseMode(
      lookupFileMeta: String => String,
      modeByConfig: String): LegacyBehaviorPolicy.Value = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return LegacyBehaviorPolicy.CORRECTED
    }
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by Spark 3.0 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version < "3.0.0" || lookupFileMeta(SPARK_LEGACY_DATETIME) != null) {
        LegacyBehaviorPolicy.LEGACY
      } else {
        LegacyBehaviorPolicy.CORRECTED
      }
    }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
  }

  def int96RebaseMode(
      lookupFileMeta: String => String,
      modeByConfig: String): LegacyBehaviorPolicy.Value = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return LegacyBehaviorPolicy.CORRECTED
    }
    // If there is no version, we return the mode specified by the config.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 3.0 and earlier follow the legacy hybrid calendar and we need to
      // rebase the INT96 timestamp values.
      // Files written by Spark 3.1 and latter may also need the rebase if they were written with
      // the "LEGACY" rebase mode.
      if (version < "3.1.0" || lookupFileMeta(SPARK_LEGACY_INT96) != null) {
        LegacyBehaviorPolicy.LEGACY
      } else {
        LegacyBehaviorPolicy.CORRECTED
      }
    }.getOrElse(LegacyBehaviorPolicy.withName(modeByConfig))
  }

  def newRebaseExceptionInRead(format: String): SparkUpgradeException = {
    val (config, option) = format match {
      case "Parquet INT96" =>
        (SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key, ParquetOptions.INT96_REBASE_MODE)
      case "Parquet" =>
        (SQLConf.PARQUET_REBASE_MODE_IN_READ.key, ParquetOptions.DATETIME_REBASE_MODE)
      case "Avro" =>
        (SQLConf.AVRO_REBASE_MODE_IN_READ.key, "datetimeRebaseMode")
      case _ => throw QueryExecutionErrors.unrecognizedFileFormatError(format)
    }
    QueryExecutionErrors.sparkUpgradeInReadingDatesError(format, config, option)
  }

  def newRebaseExceptionInWrite(format: String): SparkUpgradeException = {
    val config = format match {
      case "Parquet INT96" => SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key
      case "Parquet" => SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key
      case "Avro" => SQLConf.AVRO_REBASE_MODE_IN_WRITE.key
      case _ => throw QueryExecutionErrors.unrecognizedFileFormatError(format)
    }
    QueryExecutionErrors.sparkUpgradeInWritingDatesError(format, config)
  }

  def creteDateRebaseFuncInRead(
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

  def creteDateRebaseFuncInWrite(
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

  def creteTimestampRebaseFuncInRead(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchJulianTs) {
        throw DataSourceUtils.newRebaseExceptionInRead(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseJulianToGregorianMicros
    case LegacyBehaviorPolicy.CORRECTED => identity[Long]
  }

  def creteTimestampRebaseFuncInWrite(
      rebaseMode: LegacyBehaviorPolicy.Value,
      format: String): Long => Long = rebaseMode match {
    case LegacyBehaviorPolicy.EXCEPTION => micros: Long =>
      if (micros < RebaseDateTime.lastSwitchGregorianTs) {
        throw DataSourceUtils.newRebaseExceptionInWrite(format)
      }
      micros
    case LegacyBehaviorPolicy.LEGACY => RebaseDateTime.rebaseGregorianToJulianMicros
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
      f.references.subsetOf(partitionSet)
    )
    val extraPartitionFilter =
      dataFilters.flatMap(extractPredicatesWithinOutputSet(_, partitionSet))
    (ExpressionSet(partitionFilters ++ extraPartitionFilter).toSeq, dataFilters)
  }

  def convertV1FilterToV2(v1Filter: sources.Filter): V2Filter = {
    v1Filter match {
      case _: sources.AlwaysFalse =>
        new V2AlwaysFalse
      case _: sources.AlwaysTrue =>
        new V2AlwaysTrue
      case e: sources.EqualNullSafe =>
        new V2EqualNullSafe(FieldReference(e.attribute), getLiteralValue(e.value))
      case equal: sources.EqualTo =>
        new V2EqualTo(FieldReference(equal.attribute), getLiteralValue(equal.value))
      case g: sources.GreaterThan =>
        new V2GreaterThan(FieldReference(g.attribute), getLiteralValue(g.value))
      case ge: sources.GreaterThanOrEqual =>
        new V2GreaterThanOrEqual(FieldReference(ge.attribute), getLiteralValue(ge.value))
      case in: sources.In =>
        new V2In(FieldReference(
          in.attribute), in.values.map(value => getLiteralValue(value)))
      case notNull: sources.IsNotNull =>
        new V2IsNotNull(FieldReference(notNull.attribute))
      case isNull: sources.IsNull =>
        new V2IsNull(FieldReference(isNull.attribute))
      case l: sources.LessThan =>
        new V2LessThan(FieldReference(l.attribute), getLiteralValue(l.value))
      case le: sources.LessThanOrEqual =>
        new V2LessThanOrEqual(FieldReference(le.attribute), getLiteralValue(le.value))
      case contains: sources.StringContains =>
        new V2StringContains(
          FieldReference(contains.attribute), UTF8String.fromString(contains.value))
      case ends: sources.StringEndsWith =>
        new V2StringEndsWith(FieldReference(ends.attribute), UTF8String.fromString(ends.value))
      case starts: sources.StringStartsWith =>
        new V2StringStartsWith(
          FieldReference(starts.attribute), UTF8String.fromString(starts.value))
      case and: sources.And =>
        new V2And(convertV1FilterToV2(and.left), convertV1FilterToV2(and.right))
      case or: sources.Or =>
        new V2Or(convertV1FilterToV2(or.left), convertV1FilterToV2(or.right))
      case not: sources.Not =>
        new V2Not(convertV1FilterToV2(not.child))
      case _ => throw new IllegalStateException("Invalid v1Filter: " + v1Filter)
    }
  }

  def getLiteralValue(value: Any): LiteralValue[_] = value match {
    case _: JavaBigDecimal =>
      LiteralValue(Decimal(value.asInstanceOf[JavaBigDecimal]), DecimalType.SYSTEM_DEFAULT)
    case _: JavaBigInteger =>
      LiteralValue(Decimal(value.asInstanceOf[JavaBigInteger]), DecimalType.SYSTEM_DEFAULT)
    case _: BigDecimal =>
      LiteralValue(Decimal(value.asInstanceOf[BigDecimal]), DecimalType.SYSTEM_DEFAULT)
    case _: Boolean => LiteralValue(value, BooleanType)
    case _: Byte => LiteralValue(value, ByteType)
    case _: Array[Byte] => LiteralValue(value, BinaryType)
    case _: Date =>
      val date = DateTimeUtils.fromJavaDate(value.asInstanceOf[Date])
      LiteralValue(date, DateType)
    case _: LocalDate =>
      val date = DateTimeUtils.localDateToDays(value.asInstanceOf[LocalDate])
      LiteralValue(date, DateType)
    case _: Double => LiteralValue(value, DoubleType)
    case _: Float => LiteralValue(value, FloatType)
    case _: Integer => LiteralValue(value, IntegerType)
    case _: Long => LiteralValue(value, LongType)
    case _: Short => LiteralValue(value, ShortType)
    case _: String => LiteralValue(UTF8String.fromString(value.toString), StringType)
    case _: Timestamp =>
      val ts = DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp])
      LiteralValue(ts, TimestampType)
    case _: Instant =>
      val ts = DateTimeUtils.instantToMicros(value.asInstanceOf[Instant])
      LiteralValue(ts, TimestampType)
    case _ =>
      throw new IllegalStateException(s"The value $value in v1Filter has invalid data type.")
  }

  def convertV2FilterToV1(v2Filter: V2Filter): sources.Filter = {
    v2Filter match {
      case _: V2AlwaysFalse => sources.AlwaysFalse
      case _: V2AlwaysTrue => sources.AlwaysTrue
      case e: V2EqualNullSafe => sources.EqualNullSafe(e.column.describe,
        CatalystTypeConverters.convertToScala(e.value.value, e.value.dataType))
      case equal: V2EqualTo => sources.EqualTo(equal.column.describe,
        CatalystTypeConverters.convertToScala(equal.value.value, equal.value.dataType))
      case g: V2GreaterThan => sources.GreaterThan(g.column.describe,
        CatalystTypeConverters.convertToScala(g.value.value, g.value.dataType))
      case ge: V2GreaterThanOrEqual => sources.GreaterThanOrEqual(ge.column.describe,
        CatalystTypeConverters.convertToScala(ge.value.value, ge.value.dataType))
      case in: V2In =>
        var array: Array[Any] = Array.empty
        for (value <- in.values) {
          array = array :+ CatalystTypeConverters.convertToScala(value.value, value.dataType)
        }
        sources.In(in.column.describe, array)
      case notNull: V2IsNotNull => sources.IsNotNull(notNull.column.describe)
      case isNull: V2IsNull => sources.IsNull(isNull.column.describe)
      case l: V2LessThan => sources.LessThan(l.column.describe,
        CatalystTypeConverters.convertToScala(l.value.value, l.value.dataType))
      case le: V2LessThanOrEqual => sources.LessThanOrEqual(le.column.describe,
        CatalystTypeConverters.convertToScala(le.value.value, le.value.dataType))
      case contains: V2StringContains =>
        sources.StringContains(contains.column.describe, contains.value.toString)
      case ends: V2StringEndsWith =>
        sources.StringEndsWith(ends.column.describe, ends.value.toString)
      case starts: V2StringStartsWith =>
        sources.StringStartsWith(starts.column.describe, starts.value.toString)
      case and: V2And => sources.And(convertV2FilterToV1(and.left), convertV2FilterToV1(and.right))
      case or: V2Or => sources.Or(convertV2FilterToV1(or.left), convertV2FilterToV1(or.right))
      case not: V2Not => sources.Not(convertV2FilterToV1(not.child))
      case _ => throw new IllegalStateException("Invalid v2Filter: " + v2Filter)
    }
  }
}
