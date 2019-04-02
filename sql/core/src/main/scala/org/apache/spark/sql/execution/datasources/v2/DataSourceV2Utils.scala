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

package org.apache.spark.sql.execution.datasources.v2

import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.TableCapability.{BATCH_READ, BATCH_WRITE}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

private[sql] object DataSourceV2Utils extends Logging {

  /**
   * Helper method that extracts and transforms session configs into k/v pairs, the k/v pairs will
   * be used to create data source options.
   * Only extract when `ds` implements [[SessionConfigSupport]], in this case we may fetch the
   * specified key-prefix from `ds`, and extract session configs with config keys that start with
   * `spark.datasource.$keyPrefix`. A session config `spark.datasource.$keyPrefix.xxx -> yyy` will
   * be transformed into `xxx -> yyy`.
   *
   * @param source a [[TableProvider]] object
   * @param conf the session conf
   * @param extraOptions extra options will append to the extracted config
   * @return an case insensitive immutable map that contains all the extracted and transformed
   *         k/v pairs.
   */
  def extractSessionConfigs(
      source: TableProvider,
      conf: SQLConf,
      extraOptions: Map[String, String]): CaseInsensitiveStringMap = {
    val extracted = source match {
      case cs: SessionConfigSupport =>
        val keyPrefix = cs.keyPrefix()
        require(keyPrefix != null, "The data source config key prefix can't be null.")

        val pattern = Pattern.compile(s"^spark\\.datasource\\.$keyPrefix\\.(.+)")

        conf.getAllConfs.flatMap { case (key, value) =>
          val m = pattern.matcher(key)
          if (m.matches() && m.groupCount() > 0) {
            Seq((m.group(1), value))
          } else {
            Seq.empty
          }
        }

      case _ => Map.empty
    }

    val options = extracted ++ extraOptions
    new CaseInsensitiveStringMap(options.asJava)
  }

  /**
   * Use to determine whether this source is a v2 source.
   * @return the class of v2 source if this is a v2 source, else return None.
   */
  def isV2Source(sparkSession: SparkSession, source: String): Option[Class[_]] = {
    val lookupCls = DataSource.lookupDataSource(source, sparkSession.sessionState.conf)
    val cls = lookupCls.newInstance() match {
      // `providingClass` is used for resolving data source relation for catalog tables.
      // As now catalog for data source V2 is under development, here we fall back all the
      // [[FileDataSourceV2]] to [[FileFormat]] to guarantee the current catalog works.
      // [[FileDataSourceV2]] will still be used if we call the load()/save() method in
      // [[DataFrameReader]]/[[DataFrameWriter]], since they use method `lookupDataSource`
      // instead of `providingClass`.
      case f: FileDataSourceV2 => f.fallBackFileFormat
      case _ => lookupCls
    }

    Some(cls).filter(classOf[TableProvider].isAssignableFrom)
  }

  def getBatchWriteTable(
      sparkSession: SparkSession,
      schema: Option[StructType],
      cls: Class[_],
      options: CaseInsensitiveStringMap): Option[SupportsWrite] = {
    val provider = cls.getConstructor().newInstance().asInstanceOf[TableProvider]
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    val table = schema match {
      case Some(schema) => provider.getTable(options, schema)
      case _ => provider.getTable(options)
    }

    table match {
      case writeTable: SupportsWrite if table.supports(BATCH_WRITE) => Some(writeTable)
      case _ => None
    }
  }

  def shouldWriteWithV2(
      sparkSession: SparkSession,
      schema: Option[StructType],
      source: String,
      extraOptions: Map[String, String]): Boolean = {
    val cls = isV2Source(sparkSession, source)
    if (cls.nonEmpty) {
      val provider = cls.get.getConstructor().newInstance().asInstanceOf[TableProvider]
      val options = extractSessionConfigs(provider, sparkSession.sessionState.conf, extraOptions)
      val table = getBatchWriteTable(sparkSession, schema, cls.get, options)
      return table.nonEmpty
    }

    false
  }

  def shouldReadWithV2(
      sparkSession: SparkSession,
      schema: Option[StructType],
      source: String,
      extraOptions: Map[String, String]): Boolean = {
    val cls = isV2Source(sparkSession, source)
    if (cls.nonEmpty) {
      val provider = cls.get.getConstructor().newInstance().asInstanceOf[TableProvider]
      val options = extractSessionConfigs(provider, sparkSession.sessionState.conf, extraOptions)
      val table = getBatchReadTable(sparkSession, schema, cls.get, options)
      return table.nonEmpty
    }

    false
  }

  def getBatchReadTable(
      sparkSession: SparkSession,
      schema: Option[StructType],
      cls: Class[_],
      options: CaseInsensitiveStringMap): Option[SupportsRead] = {
    val provider = cls.getConstructor().newInstance().asInstanceOf[TableProvider]
    val table = schema match {
      case Some(schema) => provider.getTable(options, schema)
      case _ => provider.getTable(options)
    }

    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    table match {
      case readTable: SupportsRead if table.supports(BATCH_READ) => Some(readTable)
      case _ => None
    }
  }
}
