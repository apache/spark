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

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

/**
 * Builds the query for an ADBC scan. Only column pruning is pushed down, and only when the
 * identifier quote string is known: see [[AdbcOptions.quoteChar]]. Otherwise the scan reads
 * every column and Spark prunes them.
 *
 * @param rangePartitioning the range partitioning of the read, on a column of the schema
 */
class AdbcScanBuilder(
    schema: StructType,
    options: AdbcOptions,
    rangePartitioning: Option[AdbcRangePartitioning],
    identifierQuoteChar: () => Option[String])
  extends ScanBuilder with SupportsPushDownRequiredColumns {

  private var requiredSchema: StructType = schema

  override def pruneColumns(requiredSchema: StructType): Unit = {
    // Nested column pruning isn't supported: keep the full type of each required column.
    val requiredCols = requiredSchema.fieldNames.toSet
    this.requiredSchema = StructType(schema.fields.filter(f => requiredCols.contains(f.name)))
  }

  override def build(): Scan = {
    val quote = identifierQuoteChar()
    // Without a quote string, the relation is a query, which isn't quoted.
    val fromClause = options.fromClause(quote.getOrElse(""))
    val rangePredicates = rangePartitioning.map { rp =>
      rp.predicates(quote.map(AdbcUtils.quoteIdentifier(rp.column, _)).getOrElse(rp.column))
    }.getOrElse(Seq.empty)
    // A range predicate is added to the query, which must then select from the relation.
    val selectAllQuery = if (rangePartitioning.isEmpty) {
      options.selectAllQuery(quote.getOrElse(""))
    } else {
      s"SELECT * FROM $fromClause"
    }
    val (readSchema, query) = if (requiredSchema.length == schema.length) {
      (schema, selectAllQuery)
    } else if (requiredSchema.isEmpty) {
      // e.g. COUNT(*): rows are needed, no columns. The reader ignores the extra column.
      (requiredSchema, s"SELECT 1 FROM $fromClause")
    } else {
      quote match {
        case Some(q) =>
          val columns = requiredSchema.fieldNames.map(AdbcUtils.quoteIdentifier(_, q))
          (requiredSchema, s"SELECT ${columns.mkString(", ")} FROM $fromClause")
        case None =>
          (schema, selectAllQuery)
      }
    }
    AdbcScan(readSchema, query, options, rangePredicates)
  }
}
