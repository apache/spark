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

package org.apache.spark.sql.execution

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{
  AnalysisException,
  QueryTest,
  Row
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{
  CatalogPlugin,
  Identifier,
  SupportsRead,
  Table,
  TableCapability,
  TableCatalog,
  TableChange
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Test suite for cache refresh behavior with DataSource V1 and V2 tables.
 *
 * Verifies that cache refresh operations correctly update cached data when using
 * V2 tables with immutable Table instances, as well as V1 tables and views:
 * - recacheByPlan uses the provided fresh plan for re-execution
 * - refreshTable properly invalidates and resolves fresh table metadata
 *
 * V2 tests use TestV2Table with snapshot versioning to verify that cache
 * refresh picks up the latest table state.
 */
class CacheRefreshSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  // Workaround for Scala incremental compiler false positive "unused import" warnings
  // All these imports ARE used in the test infrastructure below, but the compiler
  // doesn't see it
  locally {
    val _ = (
      classOf[CatalogPlugin], classOf[Identifier], classOf[SupportsRead],
      classOf[Table], classOf[TableCapability], classOf[TableCatalog],
      classOf[TableChange], classOf[Batch], classOf[InputPartition],
      classOf[PartitionReader[_]], classOf[PartitionReaderFactory],
      classOf[Scan], classOf[ScanBuilder]
    )
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.testcat",
      classOf[TestV2CatalogWithVersioning].getName)

  override def afterEach(): Unit = {
    try {
      spark.catalog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  test("recacheByPlan correctly updates cached V2 table data") {
    testV2CacheRefresh { (tableName, cachedDf) =>
      val freshPlan = spark.table(s"testcat.$tableName").queryExecution.analyzed
      val cacheManager = spark.sharedState.cacheManager
      cacheManager.recacheByPlan(spark, freshPlan)
    }
  }

  test("refreshTable correctly updates cached V2 table data") {
    testV2CacheRefresh { (tableName, cachedDf) =>
      spark.catalog.refreshTable(s"testcat.$tableName")
    }
  }

  private def extractV2Table(plan: LogicalPlan): TestV2Table = {
    plan.collectFirst {
      case DataSourceV2Relation(table: TestV2Table, _, _, _, _, _) => table
    }.getOrElse {
      throw new IllegalStateException(
        s"No TestV2Table found in plan:\n${plan.treeString}")
    }
  }

  /**
   * Common test method to verify cache refresh behavior with V2 tables.
   *
   * Tests that when a V2 table is cached, then its data is updated, and a cache
   * refresh operation is performed, the cache correctly reflects the updated data.
   *
   * @param refreshOp The cache refresh operation to test (recacheByPlan, refreshTable, etc.)
   */
  private def testV2CacheRefresh(
      refreshOp: (String, org.apache.spark.sql.Dataset[Row]) => Unit): Unit = {
    val initialData = Seq(Row(1, "v1"), Row(2, "v1"))
    val updatedData = Seq(Row(1, "v2"), Row(2, "v2"))

    withTable("testcat.test_table") {
      // Create test table with initial data
      val catalog = spark.sessionState.catalogManager
        .catalog("testcat").asInstanceOf[TestV2CatalogWithVersioning]
      catalog.createTable(
        Identifier.of(Array("default"), "test_table"),
        schema = TestV2Table.defaultSchema,
        partitions = Array.empty,
        properties = Map.empty[String, String].asJava)
      catalog.setTableData("test_table", initialData)

      // Cache the table and verify initial data
      val df1 = spark.table("testcat.test_table")
      df1.cache()
      df1.count()
      checkAnswer(df1, initialData)

      // Get cached snapshot version
      val cacheManager = spark.sharedState.cacheManager
      val cachedData1 = cacheManager.lookupCachedData(df1)
      assert(cachedData1.isDefined, "Cache should exist")
      val cachedSnapshot1 = extractV2Table(cachedData1.get.plan).currentSnapshot

      // Update table data (new snapshot)
      catalog.setTableData("test_table", updatedData)

      val currentSnapshot = catalog.getCurrentSnapshot("test_table")
      assert(currentSnapshot > cachedSnapshot1)

      // Trigger cache refresh operation
      refreshOp("test_table", df1)

      // Verify: Cached table now has updated snapshot
      val cachedData2 = cacheManager.lookupCachedData(df1)
      assert(cachedData2.isDefined, "Cache should still exist")
      val cachedSnapshot2 = extractV2Table(cachedData2.get.plan).currentSnapshot
      assert(cachedSnapshot2 == currentSnapshot)

      // Verify: New DataFrame reads fresh data from cache
      val df2 = spark.table("testcat.test_table")
      checkAnswer(df2, updatedData)
    }
  }

  // Additional tests for V1 tables and views to ensure CacheManager changes don't break them
  test("recacheByPlan correctly updates cached V1 table data") {
    testV1TableCacheRefresh { tableName =>
      val freshPlan = spark.table(tableName).queryExecution.analyzed
      val cacheManager = spark.sharedState.cacheManager
      cacheManager.recacheByPlan(spark, freshPlan)
    }
  }

  test("refreshTable correctly updates cached V1 table data") {
    testV1TableCacheRefresh { tableName =>
      spark.catalog.refreshTable(tableName)
    }
  }

  /**
   * Common test method to verify cache refresh behavior with V1 tables.
   */
  private def testV1TableCacheRefresh(refreshOp: String => Unit): Unit = {
    val initialData = Seq((1, "v1"), (2, "v1"))
    val updatedData = Seq((1, "v1"), (2, "v1"), (3, "v2"))

    withTable("v1_table") {
      // Create V1 table with initial data
      initialData.toDF("id", "value").write.saveAsTable("v1_table")

      // Cache the table
      val df1 = spark.table("v1_table")
      df1.cache()
      df1.count()
      checkAnswer(df1, initialData.toDF("id", "value"))

      // Modify table (insert new data)
      Seq((3, "v2")).toDF("id", "value")
        .write.mode("append").saveAsTable("v1_table")

      // Trigger cache refresh operation
      refreshOp("v1_table")

      // Verify: reads updated data
      val df2 = spark.table("v1_table")
      checkAnswer(df2, updatedData.toDF("id", "value"))
    }
  }

  test("recacheByPlan correctly updates cached view data") {
    testViewCacheRefresh { viewName =>
      val freshPlan = spark.table(viewName).queryExecution.analyzed
      val cacheManager = spark.sharedState.cacheManager
      cacheManager.recacheByPlan(spark, freshPlan)
    }
  }

  test("refreshTable correctly updates cached view data") {
    testViewCacheRefresh { viewName =>
      spark.catalog.refreshTable(viewName)
    }
  }

  /**
   * Common test method to verify cache refresh behavior with views.
   */
  private def testViewCacheRefresh(refreshOp: String => Unit): Unit = {
    withTable("base_table") {
      withView("test_view") {
        // Create base table
        Seq((1, "v1"), (2, "v1")).toDF("id", "value")
          .write.saveAsTable("base_table")

        // Create view
        spark.sql("CREATE VIEW test_view AS SELECT * FROM base_table WHERE id <= 2")

        // Cache the view
        val df1 = spark.table("test_view")
        df1.cache()
        df1.count()
        checkAnswer(df1, Seq((1, "v1"), (2, "v1")).toDF("id", "value"))

        // Modify base table
        Seq((1, "v2"), (2, "v2")).toDF("id", "value")
          .write.mode("overwrite").saveAsTable("base_table")

        // Trigger cache refresh operation
        refreshOp("test_view")

        // Verify: view reads updated data
        val df2 = spark.table("test_view")
        checkAnswer(df2, Seq((1, "v2"), (2, "v2")).toDF("id", "value"))
      }
    }
  }
}

/**
 * Test V2 table that tracks snapshot versions for testing cache refresh behavior.
 *
 * NOTE: This is a case class with OVERRIDDEN equals() for custom semantic
 * equality. The semantic equality must be path-based (NOT snapshot-based) to
 * match real V2 tables.
 *
 * Key properties:
 * - Immutable (like real V2 Table implementations)
 * - Semantic equality based ONLY on table path (ignores snapshot version)
 * - Tracks current snapshot for verification
 *
 * Why custom equals() matters:
 * - Table@snapshot_v1 must equal Table@snapshot_v2 (same path, different snapshot)
 * - This allows cache lookup to succeed across versions
 * - Tests verify that cache refresh updates to the correct snapshot
 */
case class TestV2Table(
    tablePath: String,
    currentSnapshot: Long,
    private val dataSupplier: () => Seq[Row])
  extends Table with SupportsRead {

  override def name(): String = tablePath

  override def schema(): StructType = TestV2Table.defaultSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def properties(): util.Map[String, String] =
    Map("current_snapshot" -> currentSnapshot.toString).asJava

  /**
   * Semantic equality: Two tables are equal if they point to the same table
   * path. This is INDEPENDENT of snapshot version, which is crucial for cache
   * matching.
   *
   * With this implementation:
   * - Table@snapshot_v1 equals Table@snapshot_v2 (same table path)
   * - Cache lookup succeeds across snapshot versions
   * - But cache contains data from old snapshot (the bug!)
   */
  override def equals(obj: Any): Boolean = obj match {
    case that: TestV2Table =>
      this.tablePath == that.tablePath
      // Note: NOT comparing currentSnapshot - version-agnostic equality
    case _ => false
  }

  override def hashCode(): Int = tablePath.hashCode

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new TestScanBuilder(currentSnapshot, dataSupplier)
  }

  override def toString: String =
    s"TestV2Table(path=$tablePath, snapshot=v$currentSnapshot)"
}

object TestV2Table {
  val defaultSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("value", StringType, nullable = true)
  ))
}

/**
 * Test catalog that manages V2 tables with snapshot versioning.
 *
 * Simulates how real table formats (Delta, Iceberg, Hudi) work:
 * - Each table has a current snapshot version
 * - loadTable() returns a NEW Table instance with CURRENT snapshot
 * - Data can be updated, incrementing the snapshot version
 */
class TestV2CatalogWithVersioning extends TableCatalog {

  private val tables = new ConcurrentHashMap[String, TableMetadata]()

  override def name(): String = "testcat"

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // No initialization needed
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    tables.keySet().asScala.map(name =>
      Identifier.of(namespace, name)).toArray
  }

  override def loadTable(ident: Identifier): Table = {
    val tableName = ident.name()
    val metadata = Option(tables.get(tableName)).getOrElse {
      throw new NoSuchTableException(ident)
    }

    // CRITICAL: Return NEW table instance with CURRENT snapshot
    // This simulates how real V2 tables work
    // Copy the data to avoid capturing non-serializable metadata
    val currentSnapshot = metadata.snapshotVersion.get()
    val dataCopy = metadata.currentData
    new TestV2Table(tableName, currentSnapshot, () => dataCopy)
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val tableName = ident.name()

    if (tables.containsKey(tableName)) {
      throw new AnalysisException(
        errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
        messageParameters = Map("relationName" -> s"`$tableName`"))
    }

    val metadata = new TableMetadata(
      name = tableName,
      schema = schema,
      snapshotVersion = new AtomicLong(1),
      currentData = Seq.empty
    )

    tables.put(tableName, metadata)
    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException(
      "alterTable not supported in test catalog")
  }

  override def dropTable(ident: Identifier): Boolean = {
    tables.remove(ident.name()) != null
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException(
      "renameTable not supported in test catalog")
  }

  /**
   * Set table data and increment snapshot version.
   * This simulates DML operations that update the table.
   */
  def setTableData(tableName: String, data: Seq[Row]): Unit = {
    val metadata = Option(tables.get(tableName)).getOrElse {
      throw new NoSuchTableException(
        Identifier.of(Array("default"), tableName))
    }

    metadata.currentData = data
    metadata.snapshotVersion.incrementAndGet()
  }

  /**
   * Get current snapshot version for a table.
   */
  def getCurrentSnapshot(tableName: String): Long = {
    val metadata = Option(tables.get(tableName)).getOrElse {
      throw new NoSuchTableException(
        Identifier.of(Array("default"), tableName))
    }
    metadata.snapshotVersion.get()
  }

  /**
   * Internal table metadata holder.
   */
  private class TableMetadata(
      val name: String,
      val schema: StructType,
      val snapshotVersion: AtomicLong,
      var currentData: Seq[Row]) extends Serializable
}

class TestScanBuilder(
    snapshot: Long,
    dataSupplier: () => Seq[Row]) extends ScanBuilder {

  override def build(): Scan = new TestScan(snapshot, dataSupplier)
}

class TestScan(
    snapshot: Long,
    dataSupplier: () => Seq[Row]) extends Scan {

  override def readSchema(): StructType = TestV2Table.defaultSchema

  override def toBatch(): Batch = new Batch {
    override def planInputPartitions(): Array[InputPartition] = {
      Array(new TestInputPartition(snapshot, dataSupplier))
    }

    override def createReaderFactory(): PartitionReaderFactory = {
      new TestReaderFactory()
    }
  }

  override def description(): String = s"TestScan(snapshot=v$snapshot)"
}

case class TestInputPartition(
    snapshot: Long,
    dataSupplier: () => Seq[Row]) extends InputPartition

class TestReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val testPartition = partition.asInstanceOf[TestInputPartition]
    new TestPartitionReader(testPartition.dataSupplier())
  }
}

case class TestPartitionReader(data: Seq[Row])
  extends PartitionReader[InternalRow] {
  private val iterator = data.iterator
  private var current: InternalRow = _

  override def next(): Boolean = {
    if (iterator.hasNext) {
      val row = iterator.next()
      current = new GenericInternalRow(Array[Any](
        row.getInt(0),
        org.apache.spark.unsafe.types.UTF8String.fromString(row.getString(1))
      ))
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {
    // Nothing to close
  }
}

