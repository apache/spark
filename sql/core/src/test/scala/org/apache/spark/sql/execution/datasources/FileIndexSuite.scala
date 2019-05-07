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

package org.apache.spark.sql.execution.datasources

import java.io.File
import java.net.URI

import scala.collection.mutable

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path, RawLocalFileSystem}

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.KnownSizeEstimation

class FileIndexSuite extends SharedSQLContext {

  private class TestInMemoryFileIndex(
      spark: SparkSession,
      path: Path,
      fileStatusCache: FileStatusCache = NoopCache)
      extends InMemoryFileIndex(spark, Seq(path), Map.empty, None, fileStatusCache) {
    def leafFilePaths: Seq[Path] = leafFiles.keys.toSeq
    def leafDirPaths: Seq[Path] = leafDirToChildrenFiles.keys.toSeq
    def leafFileStatuses: Iterable[FileStatus] = leafFiles.values
  }

  test("InMemoryFileIndex: leaf files are qualified paths") {
    withTempDir { dir =>
      val file = new File(dir, "text.txt")
      stringToFile(file, "text")

      val path = new Path(file.getCanonicalPath)
      val catalog = new TestInMemoryFileIndex(spark, path)
      assert(catalog.leafFilePaths.forall(p => p.toString.startsWith("file:/")))
      assert(catalog.leafDirPaths.forall(p => p.toString.startsWith("file:/")))
    }
  }

  test("SPARK-26188: don't infer data types of partition columns if user specifies schema") {
    withTempDir { dir =>
      val partitionDirectory = new File(dir, "a=4d")
      partitionDirectory.mkdir()
      val file = new File(partitionDirectory, "text.txt")
      stringToFile(file, "text")
      val path = new Path(dir.getCanonicalPath)
      val schema = StructType(Seq(StructField("a", StringType, false)))
      val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, Some(schema))
      val partitionValues = fileIndex.partitionSpec().partitions.map(_.values)
      assert(partitionValues.length == 1 && partitionValues(0).numFields == 1 &&
        partitionValues(0).getString(0) == "4d")
    }
  }

  test("SPARK-26990: use user specified field names if possible") {
    withTempDir { dir =>
      val partitionDirectory = new File(dir, "a=foo")
      partitionDirectory.mkdir()
      val file = new File(partitionDirectory, "text.txt")
      stringToFile(file, "text")
      val path = new Path(dir.getCanonicalPath)
      val schema = StructType(Seq(StructField("A", StringType, false)))
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, Some(schema))
        assert(fileIndex.partitionSchema.length == 1 && fileIndex.partitionSchema.head.name == "A")
      }
    }
  }

  test("SPARK-26230: if case sensitive, validate partitions with original column names") {
    withTempDir { dir =>
      val partitionDirectory = new File(dir, "a=1")
      partitionDirectory.mkdir()
      val file = new File(partitionDirectory, "text.txt")
      stringToFile(file, "text")
      val partitionDirectory2 = new File(dir, "A=2")
      partitionDirectory2.mkdir()
      val file2 = new File(partitionDirectory2, "text.txt")
      stringToFile(file2, "text")
      val path = new Path(dir.getCanonicalPath)

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, None)
        val partitionValues = fileIndex.partitionSpec().partitions.map(_.values)
        assert(partitionValues.length == 2)
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val msg = intercept[AssertionError] {
          val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, None)
          fileIndex.partitionSpec()
        }.getMessage
        assert(msg.contains("Conflicting partition column names detected"))
        assert("Partition column name list #[0-1]: A".r.findFirstIn(msg).isDefined)
        assert("Partition column name list #[0-1]: a".r.findFirstIn(msg).isDefined)
      }
    }
  }

  test("SPARK-26263: Throw exception when partition value can't be casted to user-specified type") {
    withTempDir { dir =>
      val partitionDirectory = new File(dir, "a=foo")
      partitionDirectory.mkdir()
      val file = new File(partitionDirectory, "text.txt")
      stringToFile(file, "text")
      val path = new Path(dir.getCanonicalPath)
      val schema = StructType(Seq(StructField("a", IntegerType, false)))
      withSQLConf(SQLConf.VALIDATE_PARTITION_COLUMNS.key -> "true") {
        val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, Some(schema))
        val msg = intercept[RuntimeException] {
          fileIndex.partitionSpec()
        }.getMessage
        assert(msg == "Failed to cast value `foo` to `IntegerType` for partition column `a`")
      }

      withSQLConf(SQLConf.VALIDATE_PARTITION_COLUMNS.key -> "false") {
        val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, Some(schema))
        val partitionValues = fileIndex.partitionSpec().partitions.map(_.values)
        assert(partitionValues.length == 1 && partitionValues(0).numFields == 1 &&
          partitionValues(0).isNullAt(0))
      }
    }
  }

  test("InMemoryFileIndex: input paths are converted to qualified paths") {
    withTempDir { dir =>
      val file = new File(dir, "text.txt")
      stringToFile(file, "text")

      val unqualifiedDirPath = new Path(dir.getCanonicalPath)
      val unqualifiedFilePath = new Path(file.getCanonicalPath)
      require(!unqualifiedDirPath.toString.contains("file:"))
      require(!unqualifiedFilePath.toString.contains("file:"))

      val fs = unqualifiedDirPath.getFileSystem(spark.sessionState.newHadoopConf())
      val qualifiedFilePath = fs.makeQualified(new Path(file.getCanonicalPath))
      require(qualifiedFilePath.toString.startsWith("file:"))

      val catalog1 = new InMemoryFileIndex(
        spark, Seq(unqualifiedDirPath), Map.empty, None)
      assert(catalog1.allFiles.map(_.getPath) === Seq(qualifiedFilePath))

      val catalog2 = new InMemoryFileIndex(
        spark, Seq(unqualifiedFilePath), Map.empty, None)
      assert(catalog2.allFiles.map(_.getPath) === Seq(qualifiedFilePath))

    }
  }

  test("InMemoryFileIndex: folders that don't exist don't throw exceptions") {
    withTempDir { dir =>
      val deletedFolder = new File(dir, "deleted")
      assert(!deletedFolder.exists())
      val catalog1 = new InMemoryFileIndex(
        spark, Seq(new Path(deletedFolder.getCanonicalPath)), Map.empty, None)
      // doesn't throw an exception
      assert(catalog1.listLeafFiles(catalog1.rootPaths).isEmpty)
    }
  }

  test("PartitioningAwareFileIndex listing parallelized with many top level dirs") {
    for ((scale, expectedNumPar) <- Seq((10, 0), (50, 1))) {
      withTempDir { dir =>
        val topLevelDirs = (1 to scale).map { i =>
          val tmp = new File(dir, s"foo=$i.txt")
          tmp.mkdir()
          new Path(tmp.getCanonicalPath)
        }
        HiveCatalogMetrics.reset()
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
        new InMemoryFileIndex(spark, topLevelDirs, Map.empty, None)
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == expectedNumPar)
      }
    }
  }

  test("PartitioningAwareFileIndex listing parallelized with large child dirs") {
    for ((scale, expectedNumPar) <- Seq((10, 0), (50, 1))) {
      withTempDir { dir =>
        for (i <- 1 to scale) {
          new File(dir, s"foo=$i.txt").mkdir()
        }
        HiveCatalogMetrics.reset()
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
        new InMemoryFileIndex(spark, Seq(new Path(dir.getCanonicalPath)), Map.empty, None)
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == expectedNumPar)
      }
    }
  }

  test("PartitioningAwareFileIndex listing parallelized with large, deeply nested child dirs") {
    for ((scale, expectedNumPar) <- Seq((10, 0), (50, 4))) {
      withTempDir { dir =>
        for (i <- 1 to 2) {
          val subdirA = new File(dir, s"a=$i")
          subdirA.mkdir()
          for (j <- 1 to 2) {
            val subdirB = new File(subdirA, s"b=$j")
            subdirB.mkdir()
            for (k <- 1 to scale) {
              new File(subdirB, s"foo=$k.txt").mkdir()
            }
          }
        }
        HiveCatalogMetrics.reset()
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
        new InMemoryFileIndex(spark, Seq(new Path(dir.getCanonicalPath)), Map.empty, None)
        assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == expectedNumPar)
      }
    }
  }

  test("InMemoryFileIndex - file filtering") {
    assert(!InMemoryFileIndex.shouldFilterOut("abcd"))
    assert(InMemoryFileIndex.shouldFilterOut(".ab"))
    assert(InMemoryFileIndex.shouldFilterOut("_cd"))
    assert(!InMemoryFileIndex.shouldFilterOut("_metadata"))
    assert(!InMemoryFileIndex.shouldFilterOut("_common_metadata"))
    assert(InMemoryFileIndex.shouldFilterOut("_ab_metadata"))
    assert(InMemoryFileIndex.shouldFilterOut("_cd_common_metadata"))
    assert(InMemoryFileIndex.shouldFilterOut("a._COPYING_"))
  }

  test("SPARK-17613 - PartitioningAwareFileIndex: base path w/o '/' at end") {
    class MockCatalog(
      override val rootPaths: Seq[Path])
      extends PartitioningAwareFileIndex(spark, Map.empty, None) {

      override def refresh(): Unit = {}

      override def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = mutable.LinkedHashMap(
        new Path("mockFs://some-bucket/file1.json") -> new FileStatus()
      )

      override def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = Map(
        new Path("mockFs://some-bucket/") -> Array(new FileStatus())
      )

      override def partitionSpec(): PartitionSpec = {
        PartitionSpec.emptySpec
      }
    }

    withSQLConf(
        "fs.mockFs.impl" -> classOf[FakeParentPathFileSystem].getName,
        "fs.mockFs.impl.disable.cache" -> "true") {
      val pathWithSlash = new Path("mockFs://some-bucket/")
      assert(pathWithSlash.getParent === null)
      val pathWithoutSlash = new Path("mockFs://some-bucket")
      assert(pathWithoutSlash.getParent === null)
      val catalog1 = new MockCatalog(Seq(pathWithSlash))
      val catalog2 = new MockCatalog(Seq(pathWithoutSlash))
      assert(catalog1.allFiles().nonEmpty)
      assert(catalog2.allFiles().nonEmpty)
    }
  }

  test("InMemoryFileIndex with empty rootPaths when PARALLEL_PARTITION_DISCOVERY_THRESHOLD" +
    "is a nonpositive number") {
    withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "0") {
      new InMemoryFileIndex(spark, Seq.empty, Map.empty, None)
    }

    val e = intercept[IllegalArgumentException] {
      withSQLConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> "-1") {
        new InMemoryFileIndex(spark, Seq.empty, Map.empty, None)
      }
    }.getMessage
    assert(e.contains("The maximum number of paths allowed for listing files at " +
      "driver side must not be negative"))
  }

  test("refresh for InMemoryFileIndex with FileStatusCache") {
    withTempDir { dir =>
      val fileStatusCache = FileStatusCache.getOrCreate(spark)
      val dirPath = new Path(dir.getAbsolutePath)
      val fs = dirPath.getFileSystem(spark.sessionState.newHadoopConf())
      val catalog = new TestInMemoryFileIndex(spark, dirPath, fileStatusCache)

      val file = new File(dir, "text.txt")
      stringToFile(file, "text")
      assert(catalog.leafDirPaths.isEmpty)
      assert(catalog.leafFilePaths.isEmpty)

      catalog.refresh()

      assert(catalog.leafFilePaths.size == 1)
      assert(catalog.leafFilePaths.head == fs.makeQualified(new Path(file.getAbsolutePath)))

      assert(catalog.leafDirPaths.size == 1)
      assert(catalog.leafDirPaths.head == fs.makeQualified(dirPath))
    }
  }

  test("SPARK-20280 - FileStatusCache with a partition with very many files") {
    /* fake the size, otherwise we need to allocate 2GB of data to trigger this bug */
    class MyFileStatus extends FileStatus with KnownSizeEstimation {
      override def estimatedSize: Long = 1000 * 1000 * 1000
    }
    /* files * MyFileStatus.estimatedSize should overflow to negative integer
     * so, make it between 2bn and 4bn
     */
    val files = (1 to 3).map { i =>
      new MyFileStatus()
    }
    val fileStatusCache = FileStatusCache.getOrCreate(spark)
    fileStatusCache.putLeafFiles(new Path("/tmp", "abc"), files.toArray)
  }

  test("SPARK-20367 - properly unescape column names in inferPartitioning") {
    withTempPath { path =>
      val colToUnescape = "Column/#%'?"
      spark
        .range(1)
        .select(col("id").as(colToUnescape), col("id"))
        .write.partitionBy(colToUnescape).parquet(path.getAbsolutePath)
      assert(spark.read.parquet(path.getAbsolutePath).schema.exists(_.name == colToUnescape))
    }
  }

  test("SPARK-25062 - InMemoryFileIndex stores BlockLocation objects no matter what subclass " +
    "the FS returns") {
    withSQLConf("fs.file.impl" -> classOf[SpecialBlockLocationFileSystem].getName) {
      withTempDir { dir =>
        val file = new File(dir, "text.txt")
        stringToFile(file, "text")

        val inMemoryFileIndex = new TestInMemoryFileIndex(spark, new Path(file.getCanonicalPath))
        val blockLocations = inMemoryFileIndex.leafFileStatuses.flatMap(
          _.asInstanceOf[LocatedFileStatus].getBlockLocations)

        assert(blockLocations.forall(_.getClass == classOf[BlockLocation]))
      }
    }
  }

}

class FakeParentPathFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "mockFs"

  override def getUri: URI = {
    URI.create("mockFs://some-bucket")
  }
}

class SpecialBlockLocationFileSystem extends RawLocalFileSystem {

  class SpecialBlockLocation(
      names: Array[String],
      hosts: Array[String],
      offset: Long,
      length: Long)
    extends BlockLocation(names, hosts, offset, length)

  override def getFileBlockLocations(
      file: FileStatus,
      start: Long,
      len: Long): Array[BlockLocation] = {
    Array(new SpecialBlockLocation(Array("dummy"), Array("dummy"), 0L, file.getLen))
  }
}
