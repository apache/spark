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

package org.apache.spark.sql.pipelines.autocdc

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.classic.DataFrame
import org.apache.spark.sql.connector.catalog.{Identifier, TableInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, LongType, StructType}

/**
 * Shared test infrastructure for AutoCDC suites that exercise execution paths performing
 * v2 `MERGE INTO` operations against an in-memory catalog. Provides:
 *
 *   - A pre-configured [[InMemoryRowLevelOperationTableCatalog]] registered before each test
 *     and reset after each test.
 *   - Stable v2 [[Identifier]] and Catalyst [[TableIdentifier]] values for an auxiliary table
 *     and a target table.
 *   - Schema-agnostic primitives: table creation, microbatch [[DataFrame]] construction,
 *     and CDC metadata helpers parameterized by sequencing type.
 *
 * Suites that mix this in are responsible for defining their own schemas (auxiliary, target,
 * source) and processor / exec instances, then writing thin wrappers around [[createTable]]
 * to seed those schemas.
 */
trait AutoCdcCatalogExecutionTestBase {
  this: SharedSparkSession with BeforeAndAfter =>

  protected val catalogName: String = "cat"
  protected val namespace: String = "ns1"
  protected val auxTableName: String = "aux_table"
  protected val targetTableName: String = "target_table"

  /** Default DSv2 [[Identifier]] for the auxiliary table. */
  protected val defaultAuxIdent: Identifier = Identifier.of(Array(namespace), auxTableName)
  /** Default DSv2 [[Identifier]] for the target table. */
  protected val defaultTargetIdent: Identifier = Identifier.of(Array(namespace), targetTableName)

  /** Default catalyst three-part [[TableIdentifier]] for the auxiliary table. */
  protected val defaultAuxTableIdentifier: TableIdentifier = TableIdentifier(
    table = auxTableName,
    database = Some(namespace),
    catalog = Some(catalogName)
  )
  /** Default catalyst three-part [[TableIdentifier]] for the target table. */
  protected val defaultTargetTableIdentifier: TableIdentifier = TableIdentifier(
    table = targetTableName,
    database = Some(namespace),
    catalog = Some(catalogName)
  )

  before {
    spark.conf.set(
      s"spark.sql.catalog.$catalogName",
      classOf[InMemoryRowLevelOperationTableCatalog].getName
    )
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.unsetConf(s"spark.sql.catalog.$catalogName")
  }

  /**
   * Schema of the [[Scd1BatchProcessor.cdcMetadataColName]] struct column for a given
   * sequencing column type. Defaults to [[LongType]] because all current SCD1 tests use
   * `Long` sequencing.
   */
  protected def cdcMetadataColSchemaType(sequencingType: DataType = LongType): StructType =
    new StructType()
      .add(Scd1BatchProcessor.cdcDeleteSequenceFieldName, sequencingType)
      .add(Scd1BatchProcessor.cdcUpsertSequenceFieldName, sequencingType)

  /**
   * Build a [[Row]] matching the [[Scd1BatchProcessor.cdcMetadataColName]] struct's two fields,
   * in the order produced by [[Scd1BatchProcessor.constructCdcMetadataCol]]:
   */
  protected def cdcMetadataRow[T](deleteSeq: Option[T], upsertSeq: Option[T]): Row =
    Row(deleteSeq.getOrElse(null), upsertSeq.getOrElse(null))

  /**
   * Create a table in the test catalog under the given DSv2 [[Identifier]] using `schema`,
   * optionally seeding it with `seedRows`. Pass no rows to create an empty table.
   */
  protected def createTable(
      ident: Identifier,
      tableIdentifier: TableIdentifier,
      schema: StructType,
      seedRows: Row*): Unit = {
    spark.sessionState.catalogManager
      .catalog(catalogName)
      .asTableCatalog
      .createTable(ident, new TableInfo.Builder().withSchema(schema).build())

    if (seedRows.nonEmpty) {
      microbatchOf(schema)(seedRows: _*).writeTo(tableIdentifier.quotedString).append()
    }
  }

  /** Build a microbatch [[DataFrame]] from explicit `rows` and an explicit `schema`. */
  protected def microbatchOf(schema: StructType)(rows: Row*): DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
}
