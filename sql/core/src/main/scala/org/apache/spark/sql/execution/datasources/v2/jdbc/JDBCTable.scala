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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, V1Scan}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JDBCRDD, JDBCRelation, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class JDBCTable(
    ident: Identifier,
    schema: StructType,
    jdbcOptions: JDBCOptions) extends Table with SupportsRead with SupportsWrite {
  assert(ident.namespace().length == 1)

  override def name(): String = ident.toString

  override def capabilities(): util.Set[TableCapability] = {
    import TableCapability._
    Set(BATCH_READ, V1_BATCH_WRITE, TRUNCATE).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedOptions = new JDBCOptions(
      jdbcOptions.parameters.originalMap ++ options.asCaseSensitiveMap().asScala)
    val session = SparkSession.active
    val resolver = session.sessionState.conf.resolver
    val timeZoneId = session.sessionState.conf.sessionLocalTimeZone
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, mergedOptions)
    new JDBCScan(JDBCRelation(schema, parts, mergedOptions)(session))
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val mergedOptions = new JdbcOptionsInWrite(
      jdbcOptions.parameters.originalMap ++ info.options.asCaseSensitiveMap().asScala)
    new JDBCWrite(schema, mergedOptions)
  }
}

class JDBCScan(relation: JDBCRelation) extends ScanBuilder with V1Scan
  with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private var pushed = Array.empty[Filter]

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val options = relation.jdbcOptions
    if (options.pushDownPredicate) {
      val dialect = JdbcDialects.get(options.url)
      val (pushed, unSupported) = filters.partition(JDBCRDD.compileFilter(_, dialect).isDefined)
      this.pushed = pushed
      unSupported
    } else {
      filters
    }
  }

  override def pushedFilters(): Array[Filter] = pushed

  private var prunedSchema = relation.schema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // JDBC doesn't support nested column pruning.
    val requiredCols = requiredSchema.map(_.name)
    prunedSchema = StructType(relation.schema.fields.filter(f => requiredCols.contains(f.name)))
  }

  override def readSchema(): StructType = prunedSchema

  override def build(): Scan = this

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context
      override def schema: StructType = prunedSchema
      override def needConversion: Boolean = relation.needConversion
      override def buildScan(): RDD[Row] = {
        relation.buildScan(prunedSchema.map(_.name).toArray, pushed)
      }
    }.asInstanceOf[T]
  }
}

class JDBCWrite(schema: StructType, options: JdbcOptionsInWrite) extends V1WriteBuilder
  with SupportsTruncate {

  private var isTruncate = false

  override def truncate(): WriteBuilder = {
    isTruncate = true
    this
  }

  override def buildForV1Write(): InsertableRelation = new InsertableRelation {
    override def insert(data: DataFrame, overwrite: Boolean): Unit = {
      // TODO: do truncate and append atomically.
      if (isTruncate) {
        val conn = JdbcUtils.createConnectionFactory(options)()
        JdbcUtils.truncateTable(conn, options)
      }
      JdbcUtils.saveTable(data, Some(schema), SQLConf.get.caseSensitiveAnalysis, options)
    }
  }
}
