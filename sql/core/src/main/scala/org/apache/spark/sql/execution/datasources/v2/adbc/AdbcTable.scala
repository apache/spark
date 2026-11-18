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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.util
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegralType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A relation in the database: a table or a query.
 *
 * @param inCatalog whether the table was loaded by a catalog. The options of a read or write
 *                  then can't change the database or the table.
 */
case class AdbcTable(
    override val schema: StructType,
    options: AdbcOptions,
    inCatalog: Boolean = false)
  extends Table with SupportsRead with SupportsWrite {

  override def name(): String = options.description

  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE)

  // Fail when the data is loaded, rather than when it's read.
  rangePartitioning(options)

  // The quote string the driver reports doesn't change for a table, so look it up at most once,
  // and only when a scan or write needs it. The relation keeps this table across re-plans.
  private lazy val reportedQuoteChar: Option[String] =
    AdbcUtils.withConnection(options)(AdbcUtils.getIdentifierQuoteChar)

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder = {
    val readOptions = withOptions(scanOptions)
    new AdbcScanBuilder(schema, readOptions, rangePartitioning(readOptions),
      () => readOptions.quoteChar(reportedQuoteChar))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val writeOptions = withOptions(info.options())
    // A table always has a quote string.
    val quote = writeOptions.quoteChar(reportedQuoteChar)
      .getOrElse(AdbcOptions.DEFAULT_IDENTIFIER_QUOTE_CHAR)
    new AdbcWriteBuilder(info.schema(), writeOptions, writeOptions.writeTarget(quote))
  }

  /**
   * The table's options, with the options of a read or write, which take precedence. Those that
   * select the database or the relation are ignored for a table of a catalog.
   */
  private def withOptions(extra: CaseInsensitiveStringMap): AdbcOptions = {
    val extraOptions = extra.asCaseSensitiveMap().asScala.filter { case (k, _) =>
      !inCatalog || !AdbcOptions.RELATION_OPTIONS.contains(k.toLowerCase(Locale.ROOT))
    }
    if (extraOptions.isEmpty) options else new AdbcOptions(options.parameters ++ extraOptions)
  }

  /** Returns the range partitioning of `options`, on the column of the schema it names. */
  private def rangePartitioning(options: AdbcOptions): Option[AdbcRangePartitioning] = {
    val resolver = SQLConf.get.resolver
    options.rangePartitioning.map { rp =>
      schema.find(f => resolver(f.name, rp.column)) match {
        case Some(f) if f.dataType.isInstanceOf[IntegralType] => rp.copy(column = f.name)
        case _ => throw QueryExecutionErrors.adbcInvalidPartitionColumnError(rp.column)
      }
    }
  }
}
