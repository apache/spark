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

package org.apache.spark.sql

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.AsOfVersion
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.{Dataset => ClassicDataset}
import org.apache.spark.sql.execution.datasources.{
  FileIndex,
  HadoopFsRelation,
  LogicalRelation,
  PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V1TimeTravelCacheSuite extends QueryTest with SharedSparkSession {

  private val cachedTableNameParts = Seq("db", "tbl")
  private val tableSchema = StructType(Seq(StructField("id", IntegerType, nullable = false)))
  private val tableMetadata = CatalogTable(
    identifier = TableIdentifier("tbl", Some("db")),
    tableType = CatalogTableType.MANAGED,
    storage = CatalogStorageFormat.empty,
    schema = tableSchema,
    provider = Some("test"))

  override def afterEach(): Unit = {
    try {
      spark.catalog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  test("recacheTableOrView excludes a time travel V1 FileIndex") {
    assertRecacheBehavior(
      newFileRelation(isTimeTravel = true),
      includeTimeTravel = false,
      expectedToRecache = false)
  }

  test("recacheTableOrView includes a live V1 FileIndex") {
    assertRecacheBehavior(
      newFileRelation(isTimeTravel = false),
      includeTimeTravel = false,
      expectedToRecache = true)
  }

  test("recacheTableOrView includes a time travel V1 FileIndex when requested") {
    assertRecacheBehavior(
      newFileRelation(isTimeTravel = true),
      includeTimeTravel = true,
      expectedToRecache = true)
  }

  test("write-driven recacheByPath excludes a time travel V1 FileIndex") {
    assertRecacheByPathBehavior(
      isTimeTravel = true,
      writeDriven = true,
      expectedToRecache = false)
  }

  test("write-driven recacheByPath includes a live V1 FileIndex") {
    assertRecacheByPathBehavior(
      isTimeTravel = false,
      writeDriven = true,
      expectedToRecache = true)
  }

  test("explicit recacheByPath includes a time travel V1 FileIndex") {
    assertRecacheByPathBehavior(
      isTimeTravel = true,
      writeDriven = false,
      expectedToRecache = true)
  }

  test("write-driven recacheByPath rebuilds a mixed pinned and live V1 cache") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val pinnedIndex = new TestFileIndex(isTimeTravel = true, rootPaths = Seq(rootPath))
      val liveIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      val pinned = ClassicDataset.ofRows(
        spark,
        LogicalRelation(newFileRelation(pinnedIndex), tableMetadata))
      val live = ClassicDataset.ofRows(
        spark,
        LogicalRelation(newFileRelation(liveIndex), tableMetadata))
      // Keep the pinned child first to ensure traversal continues past it to the live child.
      val compound = pinned.union(live).persist()
      try {
        compound.count()
        assertCacheLoading(compound, expected = true)

        val fs = rootPath.getFileSystem(spark.sessionState.newHadoopConf())
        spark.sharedState.cacheManager.recacheByPath(
          spark,
          rootPath,
          fs,
          includeTimeTravel = false)

        assertCacheLoading(compound, expected = false)
        assert(pinnedIndex.refreshCount === 0)
        assert(liveIndex.refreshCount === 1)
      } finally {
        compound.unpersist(blocking = true)
      }
    }
  }

  test("write-driven recacheTableOrView rebuilds a pinned cache with a live subquery read") {
    withPinnedCacheWithLiveSubqueryRead { (cached, _, _, _) =>
      spark.sharedState.cacheManager.recacheTableOrView(
        spark,
        cachedTableNameParts,
        includeTimeTravel = false)

      assertCacheLoading(cached, expected = false)
    }
  }

  test("write-driven recacheByPath rebuilds a pinned cache with a live subquery read") {
    withPinnedCacheWithLiveSubqueryRead { (cached, rootPath, pinnedIndex, liveIndex) =>
      val fs = rootPath.getFileSystem(spark.sessionState.newHadoopConf())
      spark.sharedState.cacheManager.recacheByPath(
        spark,
        rootPath,
        fs,
        includeTimeTravel = false)

      assertCacheLoading(cached, expected = false)
      assert(pinnedIndex.refreshCount === 0)
      assert(liveIndex.refreshCount === 1)
    }
  }

  test("write-driven recacheByPath refreshes a main-plan and a subquery live V1 FileIndex") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val mainIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      val subqueryIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      assertWriteDrivenRecacheByPathClearsCache(
        rootPath,
        Seq("main_read" -> mainIndex, "subquery_read" -> subqueryIndex),
        "SELECT * FROM main_read WHERE id IN (SELECT id FROM subquery_read)")

      assert(mainIndex.refreshCount === 1)
      assert(subqueryIndex.refreshCount === 1)
    }
  }

  test("write-driven recacheByPath refreshes live V1 FileIndexes in two subqueries") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val pinnedIndex = new TestFileIndex(isTimeTravel = true, rootPaths = Seq(rootPath))
      val liveIndex1 = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      val liveIndex2 = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      assertWriteDrivenRecacheByPathClearsCache(
        rootPath,
        Seq("pinned" -> pinnedIndex, "live1" -> liveIndex1, "live2" -> liveIndex2),
        "SELECT * FROM pinned WHERE id IN (SELECT id FROM live1) AND " +
          "id IN (SELECT id FROM live2)")

      assert(pinnedIndex.refreshCount === 0)
      assert(liveIndex1.refreshCount === 1)
      assert(liveIndex2.refreshCount === 1)
    }
  }

  test("write-driven recacheByPath refreshes a FileIndex read in the main plan and subquery once") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val liveIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      assertWriteDrivenRecacheByPathClearsCache(
        rootPath,
        Seq("live" -> liveIndex),
        "SELECT * FROM live WHERE id IN (SELECT id FROM live)")

      assert(liveIndex.refreshCount === 1)
    }
  }

  test("write-driven recacheByPath refreshes a FileIndex shared by two cache entries once") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val indexX = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      val indexY = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      assertWriteDrivenRecacheByPathClearsCache(
        rootPath,
        Seq("x" -> indexX, "y" -> indexY),
        "SELECT * FROM x WHERE id IN (SELECT id FROM y)",
        "SELECT * FROM y")

      assert(indexX.refreshCount === 1)
      assert(indexY.refreshCount === 1)
    }
  }

  test("write-driven recacheByPath refreshes a FileIndex read by subqueries of two entries once") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val pinnedIndex = new TestFileIndex(isTimeTravel = true, rootPaths = Seq(rootPath))
      val liveIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      assertWriteDrivenRecacheByPathClearsCache(
        rootPath,
        Seq("pinned" -> pinnedIndex, "live" -> liveIndex),
        "SELECT * FROM pinned WHERE id IN (SELECT id FROM live)",
        "SELECT * FROM pinned WHERE id NOT IN (SELECT id FROM live)")

      assert(pinnedIndex.refreshCount === 0)
      assert(liveIndex.refreshCount === 1)
    }
  }

  test("write-driven recacheByPath refreshes a FileIndex read in a nested subquery") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val pinnedIndex = new TestFileIndex(isTimeTravel = true, rootPaths = Seq(rootPath))
      val liveIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      assertWriteDrivenRecacheByPathClearsCache(
        rootPath,
        Seq("pinned" -> pinnedIndex, "live" -> liveIndex),
        "SELECT * FROM pinned WHERE id IN " +
          "(SELECT id FROM pinned WHERE id IN (SELECT id FROM live))")

      assert(pinnedIndex.refreshCount === 0)
      assert(liveIndex.refreshCount === 1)
    }
  }

  test("V1 file write preserves a time travel cache and refreshes a live cache") {
    withTempPath { path =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        val dataPath = path.getCanonicalPath
        spark.range(1).write.parquet(dataPath)

        val pinnedIndex = new TestFileIndex(
          isTimeTravel = true,
          rootPaths = Seq(new Path(path.toURI)))
        val pinned = ClassicDataset.ofRows(
          spark,
          LogicalRelation(newFileRelation(pinnedIndex), tableMetadata)).persist()
        val live = spark.read.parquet(dataPath).persist()
        try {
          pinned.count()
          checkAnswer(live, Row(0L))
          assertCacheLoading(pinned, expected = true)
          assertCacheLoading(live, expected = true)

          spark.range(1, 2).write.mode("append").parquet(dataPath)

          assertCacheLoading(pinned, expected = true)
          assertCacheLoading(live, expected = false)
          assert(pinnedIndex.refreshCount === 0)
          checkAnswer(spark.read.parquet(dataPath), Seq(Row(0L), Row(1L)))
          assertCacheLoading(live, expected = true)
        } finally {
          pinned.unpersist(blocking = true)
          live.unpersist(blocking = true)
        }
      }
    }
  }

  test("V1 file write refreshes both path reads of a cached query, one inside a subquery") {
    withTempPath { path =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
        val dataPath = path.getCanonicalPath
        spark.range(2).write.parquet(dataPath)
        withTempView("main_read", "subquery_read") {
          // Each path read creates its own file index.
          spark.read.parquet(dataPath).createOrReplaceTempView("main_read")
          spark.read.parquet(dataPath).createOrReplaceTempView("subquery_read")
          val query = "SELECT id FROM main_read WHERE id IN (SELECT id FROM subquery_read)"
          val cached = spark.sql(query).persist()
          try {
            checkAnswer(cached, Seq(Row(0L), Row(1L)))

            spark.range(2, 4).write.mode("append").parquet(dataPath)
            checkAnswer(spark.sql(query), (0L until 4L).map(Row(_)))

            spark.range(10, 12).write.mode("overwrite").parquet(dataPath)
            checkAnswer(spark.sql(query), Seq(Row(10L), Row(11L)))
          } finally {
            cached.unpersist(blocking = true)
          }
        }
      }
    }
  }

  test("recacheByPath excludes a V2 time-travel relation only for write-driven refresh") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val options = CaseInsensitiveStringMap.empty()
      val table = ParquetTable(
        name = "time-travel-table",
        sparkSession = spark,
        options = options,
        paths = Seq(rootPath.toString),
        userSpecifiedSchema = Some(tableSchema),
        fallbackFileFormat = classOf[ParquetFileFormat])
      assert(!table.fileIndex.isTimeTravel)

      val relation = DataSourceV2Relation.create(
        table,
        catalog = None,
        identifier = None,
        options = options,
        timeTravelSpec = Some(AsOfVersion("v1")))
      val df = ClassicDataset.ofRows(spark, relation).persist()
      try {
        df.count()
        assertCacheLoading(df, expected = true)

        val resourcePath = table.fileIndex.rootPaths.head
        val fs = resourcePath.getFileSystem(spark.sessionState.newHadoopConf())
        spark.sharedState.cacheManager.recacheByPath(
          spark,
          resourcePath,
          fs,
          includeTimeTravel = false)
        assertCacheLoading(df, expected = true)

        spark.sharedState.cacheManager.recacheByPath(spark, resourcePath, fs)
        assertCacheLoading(df, expected = false)
      } finally {
        df.unpersist(blocking = true)
      }
    }
  }

  test("write-driven recacheByPath includes a live V2 relation") {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val options = CaseInsensitiveStringMap.empty()
      val table = ParquetTable(
        name = "live-table",
        sparkSession = spark,
        options = options,
        paths = Seq(rootPath.toString),
        userSpecifiedSchema = Some(tableSchema),
        fallbackFileFormat = classOf[ParquetFileFormat])
      assert(!table.fileIndex.isTimeTravel)

      val relation = DataSourceV2Relation.create(
        table,
        catalog = None,
        identifier = None,
        options = options,
        timeTravelSpec = None)
      val df = ClassicDataset.ofRows(spark, relation).persist()
      try {
        df.count()
        assertCacheLoading(df, expected = true)

        val resourcePath = table.fileIndex.rootPaths.head
        val fs = resourcePath.getFileSystem(spark.sessionState.newHadoopConf())
        spark.sharedState.cacheManager.recacheByPath(
          spark,
          resourcePath,
          fs,
          includeTimeTravel = false)
        assertCacheLoading(df, expected = false)
      } finally {
        df.unpersist(blocking = true)
      }
    }
  }

  private def assertRecacheBehavior(
      relation: BaseRelation,
      includeTimeTravel: Boolean,
      expectedToRecache: Boolean): Unit = {
    val df = ClassicDataset.ofRows(spark, LogicalRelation(relation, tableMetadata)).persist()
    try {
      df.count()
      assertCacheLoading(df, expected = true)

      spark.sharedState.cacheManager.recacheTableOrView(
        spark,
        cachedTableNameParts,
        includeTimeTravel = includeTimeTravel)

      assertCacheLoading(df, expected = !expectedToRecache)
    } finally {
      df.unpersist(blocking = true)
    }
  }

  private def assertCacheLoading(df: ClassicDataset[_], expected: Boolean): Unit = {
    val cachedData = spark.sharedState.cacheManager
      .lookupCachedData(df)
      .getOrElse(fail("DataFrame is not cached"))
    assert(cachedData.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded === expected)
  }

  private def assertRecacheByPathBehavior(
      isTimeTravel: Boolean,
      writeDriven: Boolean,
      expectedToRecache: Boolean): Unit = {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val fileIndex = new TestFileIndex(isTimeTravel, Seq(rootPath))
      val df = ClassicDataset.ofRows(
        spark,
        LogicalRelation(newFileRelation(fileIndex), tableMetadata)).persist()
      try {
        df.count()
        assertCacheLoading(df, expected = true)

        val fs = rootPath.getFileSystem(spark.sessionState.newHadoopConf())
        if (writeDriven) {
          spark.sharedState.cacheManager.recacheByPath(
            spark,
            rootPath,
            fs,
            includeTimeTravel = false)
        } else {
          spark.sharedState.cacheManager.recacheByPath(spark, rootPath, fs)
        }

        assertCacheLoading(df, expected = !expectedToRecache)
        assert(fileIndex.refreshCount === (if (expectedToRecache) 1 else 0))
      } finally {
        df.unpersist(blocking = true)
      }
    }
  }

  /**
   * Caches a query that reads the table at a pinned version and reads it live only inside an IN
   * subquery, then passes the materialized cache, the root path, and both file indexes to `f`.
   */
  private def withPinnedCacheWithLiveSubqueryRead(
      f: (ClassicDataset[_], Path, TestFileIndex, TestFileIndex) => Unit): Unit = {
    withTempDir { dir =>
      val rootPath = new Path(dir.toURI)
      val pinnedIndex = new TestFileIndex(isTimeTravel = true, rootPaths = Seq(rootPath))
      val liveIndex = new TestFileIndex(isTimeTravel = false, rootPaths = Seq(rootPath))
      withTempView("pinned", "live") {
        ClassicDataset.ofRows(spark, LogicalRelation(newFileRelation(pinnedIndex), tableMetadata))
          .createOrReplaceTempView("pinned")
        ClassicDataset.ofRows(spark, LogicalRelation(newFileRelation(liveIndex), tableMetadata))
          .createOrReplaceTempView("live")
        val cached = spark.sql("SELECT * FROM pinned WHERE id IN (SELECT id FROM live)").persist()
        try {
          cached.count()
          assertCacheLoading(cached, expected = true)
          f(cached, rootPath, pinnedIndex, liveIndex)
        } finally {
          cached.unpersist(blocking = true)
        }
      }
    }
  }

  /**
   * Creates a temp view over each file index, caches and materializes each of `queries`, and
   * checks that one write-driven `recacheByPath` on `rootPath` clears all of their caches.
   */
  private def assertWriteDrivenRecacheByPathClearsCache(
      rootPath: Path,
      views: Seq[(String, TestFileIndex)],
      queries: String*): Unit = {
    withTempView(views.map(_._1): _*) {
      views.foreach { case (name, fileIndex) =>
        ClassicDataset.ofRows(spark, LogicalRelation(newFileRelation(fileIndex), tableMetadata))
          .createOrReplaceTempView(name)
      }
      val cached = queries.map(spark.sql(_).persist())
      try {
        cached.foreach { dataset =>
          dataset.count()
          assertCacheLoading(dataset, expected = true)
        }

        val fs = rootPath.getFileSystem(spark.sessionState.newHadoopConf())
        spark.sharedState.cacheManager.recacheByPath(
          spark,
          rootPath,
          fs,
          includeTimeTravel = false)

        cached.foreach(assertCacheLoading(_, expected = false))
      } finally {
        cached.foreach(_.unpersist(blocking = true))
      }
    }
  }

  private def newFileRelation(isTimeTravel: Boolean): HadoopFsRelation = {
    newFileRelation(new TestFileIndex(isTimeTravel))
  }

  private def newFileRelation(fileIndex: FileIndex): HadoopFsRelation = {
    HadoopFsRelation(
      location = fileIndex,
      partitionSchema = StructType(Nil),
      dataSchema = tableSchema,
      bucketSpec = None,
      fileFormat = new ParquetFileFormat,
      options = Map.empty)(spark)
  }

  private class TestFileIndex(
      override val isTimeTravel: Boolean,
      override val rootPaths: Seq[Path] = Nil) extends FileIndex {
    var refreshCount: Int = 0

    override def listFiles(
        partitionFilters: Seq[Expression],
        dataFilters: Seq[Expression]): Seq[PartitionDirectory] = Nil
    override def inputFiles: Array[String] = Array.empty
    override def refresh(): Unit = refreshCount += 1
    override def sizeInBytes: Long = 0L
    override def partitionSchema: StructType = StructType(Nil)
  }
}
