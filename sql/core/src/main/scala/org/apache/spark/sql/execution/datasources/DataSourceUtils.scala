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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.{SPARK_LEGACY_DATETIME, SPARK_LEGACY_INT96, SPARK_VERSION_METADATA_KEY}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils


object DataSourceUtils {
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
   * Verify if the schema is supported in datasource. This verification should be done
   * in a driver side.
   */
  def verifySchema(format: FileFormat, schema: StructType): Unit = {
    schema.foreach { field =>
      if (!format.supportDataType(field.dataType)) {
        throw new AnalysisException(
          s"$format data source does not support ${field.dataType.catalogString} data type.")
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
    val config = format match {
      case "Parquet INT96" => SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ.key
      case "Parquet" => SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key
      case "Avro" => SQLConf.LEGACY_AVRO_REBASE_MODE_IN_READ.key
      case _ => throw new IllegalStateException("unrecognized format " + format)
    }
    new SparkUpgradeException("3.0", "reading dates before 1582-10-15 or timestamps before " +
      s"1900-01-01T00:00:00Z from $format files can be ambiguous, as the files may be written by " +
      "Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar that is " +
      "different from Spark 3.0+'s Proleptic Gregorian calendar. See more details in " +
      s"SPARK-31404. You can set $config to 'LEGACY' to rebase the datetime values w.r.t. " +
      s"the calendar difference during reading. Or set $config to 'CORRECTED' to read the " +
      "datetime values as it is.", null)
  }

  def newRebaseExceptionInWrite(format: String): SparkUpgradeException = {
    val config = format match {
      case "Parquet INT96" => SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE.key
      case "Parquet" => SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key
      case "Avro" => SQLConf.LEGACY_AVRO_REBASE_MODE_IN_WRITE.key
      case _ => throw new IllegalStateException("unrecognized format " + format)
    }
    new SparkUpgradeException("3.0", "writing dates before 1582-10-15 or timestamps before " +
      s"1900-01-01T00:00:00Z into $format files can be dangerous, as the files may be read by " +
      "Spark 2.x or legacy versions of Hive later, which uses a legacy hybrid calendar that is " +
      "different from Spark 3.0+'s Proleptic Gregorian calendar. See more details in " +
      s"SPARK-31404. You can set $config to 'LEGACY' to rebase the datetime values w.r.t. " +
      "the calendar difference during writing, to get maximum interoperability. Or set " +
      s"$config to 'CORRECTED' to write the datetime values as it is, if you are 100% sure that " +
      "the written files will only be read by Spark 3.0+ or other systems that use Proleptic " +
      "Gregorian calendar.", null)
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

  def createTimestampRebaseFuncInWrite(
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
          throw new AnalysisException(
            s"Fail to resolve data source for the table ${table.identifier} since the table " +
              s"serde property has the duplicated key $k with extra options specified for this " +
              "scan operation. To fix this, you can rollback to the legacy behavior of ignoring " +
              "the extra options by setting the config " +
              s"${SQLConf.LEGACY_EXTRA_OPTIONS_BEHAVIOR.key} to `false`, or address the " +
              s"conflicts of the same config.")
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
}
