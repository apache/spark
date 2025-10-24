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

package org.apache.spark.sql.execution.command

import java.util

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Test suite demonstrating cache invalidation bug when:
 * 1. Table is READ via V2 catalog (creates DataSourceV2Relation in cache)
 * 2. Table is WRITTEN via V1 API (InsertableRelation) without proper V2 cache invalidation
 * 3. Result: Cache becomes stale because V1 write doesn't know about V2 cached relation
 *
 * This is NOT a FileTable scenario - it's a pure catalog mismatch where reads go through
 * V2 catalog but writes fall back to V1 InsertableRelation without triggering V2 cache refresh.
 */
class CacheInvalidationV2ReadV1WriteSuite extends QueryTest with SharedSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Register a V2 catalog that exposes tables for V2 reads
    spark.conf.set(
      "spark.sql.catalog.test_catalog",
      classOf[TestV2CatalogWithV1Fallback].getName)
  }

  override def afterEach(): Unit = {
    spark.catalog.clearCache()
    TestV2CatalogWithV1Fallback.clearAllTables()
    super.afterEach()
  }

  test("BUG: V2 read + V1 InsertableRelation write without V2 cache invalidation") {
    // Create table via V2 catalog
    spark.sql(
      """CREATE TABLE test_catalog.default.users (id INT, name STRING)
        |USING test_v2_provider
        |""".stripMargin)

    // Initial insert
    spark.sql("INSERT INTO test_catalog.default.users VALUES (1, 'Alice'), (2, 'Bob')")

    // Read and cache via V2 - this creates DataSourceV2Relation in the cache
    val df = spark.table("test_catalog.default.users")
    df.cache()
    val initialCount = df.count()
    assert(initialCount == 2, "Initial data should have 2 rows")

    // Verify that cache contains the data
    assert(spark.sharedState.cacheManager.lookupCachedData(df).isDefined,
      "Table should be cached")

    // Now perform another INSERT
    // This will use InsertableRelation (V1 path) which does NOT call refreshCache
    // on the V2 DataSourceV2Relation
    spark.sql("INSERT INTO test_catalog.default.users VALUES (3, 'Charlie')")

    // BUG: Cache is STALE because:
    // 1. Cache key is DataSourceV2Relation (from V2 read)
    // 2. Write went through InsertableRelation without V2 cache callback
    // 3. No cache invalidation happened
    val cachedCount = spark.table("test_catalog.default.users").count()

    // This assertion FAILS, demonstrating the bug
    // Cache still shows 2 rows even though actual data has 3 rows
    assert(cachedCount == 2,
      "BUG DEMONSTRATION: Cache is stale, showing old count (2) even though data has 3 rows")

    // Verify the actual data has 3 rows by clearing cache
    spark.catalog.clearCache()
    val actualCount = spark.table("test_catalog.default.users").count()
    assert(actualCount == 3, s"After clearing cache, should show 3 rows, got $actualCount")
  }

  test("WORKAROUND: Explicit refreshTable invalidates stale V2 cache") {
    spark.sql(
      """CREATE TABLE test_catalog.default.users (id INT, name STRING)
        |USING test_v2_provider
        |""".stripMargin)

    spark.sql("INSERT INTO test_catalog.default.users VALUES (1, 'Alice'), (2, 'Bob')")

    val df = spark.table("test_catalog.default.users")
    df.cache()
    assert(df.count() == 2)

    // Insert more data (cache becomes stale)
    spark.sql("INSERT INTO test_catalog.default.users VALUES (3, 'Charlie')")

    // Workaround: Explicitly refresh the table to invalidate cache
    spark.catalog.refreshTable("test_catalog.default.users")

    // Now cache is properly refreshed
    val freshCount = spark.table("test_catalog.default.users").count()
    assert(freshCount == 3, s"After refreshTable, should show 3 rows, got $freshCount")
  }
}

// =================================================================================================
// Test Catalog Implementation
// =================================================================================================

/**
 * Companion object to store table data globally (accessible from driver and executors).
 */
object TestV2CatalogWithV1Fallback {
  val tableData = new util.concurrent.ConcurrentHashMap[String, util.List[Row]]()

  def clearAllTables(): Unit = {
    tableData.clear()
  }
}

/**
 * A V2 catalog that:
 * 1. Returns V2 tables for reads (SupportsRead -> DataSourceV2Relation)
 * 2. Returns tables that implement InsertableRelation for V1 write fallback
 * 3. Does NOT trigger V2 cache invalidation on writes
 *
 * This simulates the cache invalidation bug.
 */
class TestV2CatalogWithV1Fallback extends TableCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private val tables = new util.concurrent.ConcurrentHashMap[Identifier, Table]()
  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    tables.keySet().asScala.filter(_.namespace.sameElements(namespace)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    Option(tables.get(ident)).getOrElse {
      throw new NoSuchTableException(ident.asMultipartIdentifier)
    }
  }

  override def createTable(
      ident: Identifier,
      columns: Array[Column],
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    val tableName = s"${ident.namespace().mkString(".")}.${ident.name()}"

    // Initialize data storage
    TestV2CatalogWithV1Fallback.tableData.putIfAbsent(
      tableName, new util.concurrent.CopyOnWriteArrayList[Row]())

    val table = new V2TableWithV1InsertFallback(tableName, schema)
    tables.put(ident, table)
    table
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("ALTER TABLE not supported")
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tableName = s"${ident.namespace().mkString(".")}.${ident.name()}"
    TestV2CatalogWithV1Fallback.tableData.remove(tableName)
    Option(tables.remove(ident)).isDefined
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("RENAME TABLE not supported")
  }
}

/**
 * A table implementation that:
 * 1. Supports V2 reads (SupportsRead)
 * 2. Implements V1 InsertableRelation for writes
 * 3. Does NOT have V2 write capabilities (no SupportsWrite)
 *
 * This creates the scenario where:
 * - Reads create DataSourceV2Relation in cache
 * - Writes use InsertableRelation which doesn't know about DataSourceV2Relation in cache
 * - Cache invalidation fails
 */
class V2TableWithV1InsertFallback(
    tableName: String,
    tableSchema: StructType)
  extends Table
    with SupportsRead
    with InsertableRelation {

  override def name(): String = tableName

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    // Only BATCH_READ - no BATCH_WRITE or V1_BATCH_WRITE
    // This forces writes to fall back to InsertableRelation
    util.Collections.singleton(TableCapability.BATCH_READ)
  }

  override def partitioning(): Array[Transform] = Array.empty

  override def properties(): util.Map[String, String] = {
    util.Collections.singletonMap(TableCatalog.PROP_PROVIDER, "test_v2_provider")
  }

  // V2 Read implementation
  override def newScanBuilder(options: CaseInsensitiveStringMap): V2ScanBuilder = {
    new V2ScanBuilder(tableName, tableSchema)
  }

  // V1 InsertableRelation implementation
  // This is called for writes but does NOT trigger V2 cache invalidation
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val tableData = TestV2CatalogWithV1Fallback.tableData.get(tableName)
    if (tableData != null) {
      if (overwrite) {
        tableData.clear()
      }
      data.collect().foreach { row =>
        tableData.add(row)
      }
    }
  }
}

// =================================================================================================
// Read Implementation
// =================================================================================================

private[command] class V2ScanBuilder(tableName: String, schema: StructType)
  extends org.apache.spark.sql.connector.read.ScanBuilder
    with org.apache.spark.sql.connector.read.Scan
    with Serializable {

  override def build(): org.apache.spark.sql.connector.read.Scan = this
  override def readSchema(): StructType = schema

  override def toBatch: org.apache.spark.sql.connector.read.Batch = {
    new org.apache.spark.sql.connector.read.Batch {
      override def planInputPartitions():
          Array[org.apache.spark.sql.connector.read.InputPartition] = {
        val data = Option(TestV2CatalogWithV1Fallback.tableData.get(tableName))
          .map(_.asScala.toArray)
          .getOrElse(Array.empty[Row])
        Array(new V2InputPartition(data, schema))
      }

      override def createReaderFactory():
          org.apache.spark.sql.connector.read.PartitionReaderFactory = {
        new V2ReaderFactory()
      }
    }
  }
}

private[command] class V2InputPartition(data: Array[Row], schema: StructType)
  extends org.apache.spark.sql.connector.read.InputPartition
  with Serializable {

  def getData: Array[Row] = data
  def getSchema: StructType = schema
}

private[command] class V2ReaderFactory
  extends org.apache.spark.sql.connector.read.PartitionReaderFactory {

  override def createReader(
      partition: org.apache.spark.sql.connector.read.InputPartition):
      org.apache.spark.sql.connector.read.PartitionReader[
        org.apache.spark.sql.catalyst.InternalRow] = {
    val v2Partition = partition.asInstanceOf[V2InputPartition]
    new V2PartitionReader(v2Partition.getData, v2Partition.getSchema)
  }
}

private[command] class V2PartitionReader(data: Array[Row], schema: StructType)
  extends org.apache.spark.sql.connector.read.PartitionReader[
    org.apache.spark.sql.catalyst.InternalRow] {

  private var currentIndex = 0
  private var current: org.apache.spark.sql.catalyst.InternalRow = _

  override def next(): Boolean = {
    if (currentIndex < data.length) {
      val row = data(currentIndex)
      current = org.apache.spark.sql.catalyst.InternalRow.fromSeq(
        row.toSeq.map {
          case v: Int => v
          case v: String => org.apache.spark.unsafe.types.UTF8String.fromString(v)
          case v => v
        }
      )
      currentIndex += 1
      true
    } else {
      false
    }
  }

  override def get(): org.apache.spark.sql.catalyst.InternalRow = current
  override def close(): Unit = {}
}
