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
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.{Dataset => ClassicDataset}
import org.apache.spark.sql.execution.datasources.{
  FileIndex,
  HadoopFsRelation,
  LogicalRelation,
  PartitionDirectory}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

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
