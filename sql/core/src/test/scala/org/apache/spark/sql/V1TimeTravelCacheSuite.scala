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
