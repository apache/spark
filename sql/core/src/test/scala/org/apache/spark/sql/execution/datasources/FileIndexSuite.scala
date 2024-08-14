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

import java.io.{File, FileNotFoundException}
import java.net.URI

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path, RawLocalFileSystem, RemoteIterator}
import org.apache.hadoop.fs.viewfs.ViewFileSystem
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.KnownSizeEstimation

class FileIndexSuite extends SharedSparkSession {

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
        val msg = intercept[SparkRuntimeException] {
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
        checkError(
          exception = intercept[SparkRuntimeException] {
            fileIndex.partitionSpec()
          },
          errorClass = "_LEGACY_ERROR_TEMP_2058",
          parameters = Map("value" -> "foo", "dataType" -> "IntegerType", "columnName" -> "a")
        )
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
      assert(catalog1.allFiles().map(_.getPath) === Seq(qualifiedFilePath))

      val catalog2 = new InMemoryFileIndex(
        spark, Seq(unqualifiedFilePath), Map.empty, None)
      assert(catalog2.allFiles().map(_.getPath) === Seq(qualifiedFilePath))

    }
  }

  test("InMemoryFileIndex: root folders that don't exist don't throw exceptions") {
    withTempDir { dir =>
      val deletedFolder = new File(dir, "deleted")
      assert(!deletedFolder.exists())
      val catalog1 = new InMemoryFileIndex(
        spark, Seq(new Path(deletedFolder.getCanonicalPath)), Map.empty, None)
      // doesn't throw an exception
      assert(catalog1.listLeafFiles(catalog1.rootPaths).isEmpty)
    }
  }

  test("SPARK-27676: InMemoryFileIndex respects ignoreMissingFiles config for non-root paths") {
    import DeletionRaceFileSystem._
    for (
      raceCondition <- Seq(
        classOf[SubdirectoryDeletionRaceFileSystem],
        classOf[FileDeletionRaceFileSystem]
      );
      (ignoreMissingFiles, sqlConf, options) <- Seq(
        (true, "true", Map.empty[String, String]),
        // Explicitly set sqlConf to false, but data source options should take precedence
        (true, "false", Map("ignoreMissingFiles" -> "true")),
        (false, "false", Map.empty[String, String]),
        // Explicitly set sqlConf to true, but data source options should take precedence
        (false, "true", Map("ignoreMissingFiles" -> "false"))
      );
      parDiscoveryThreshold <- Seq(0, 100)
    ) {
      withClue(s"raceCondition=$raceCondition, ignoreMissingFiles=$ignoreMissingFiles, " +
        s"parDiscoveryThreshold=$parDiscoveryThreshold, sqlConf=$sqlConf, options=$options"
      ) {
        withSQLConf(
          SQLConf.IGNORE_MISSING_FILES.key -> sqlConf,
          SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key -> parDiscoveryThreshold.toString,
          "fs.mockFs.impl" -> raceCondition.getName,
          "fs.mockFs.impl.disable.cache" -> "true"
        ) {
          def makeCatalog(): InMemoryFileIndex = new InMemoryFileIndex(
            spark, Seq(rootDirPath), options, None)
          if (ignoreMissingFiles) {
            // We're ignoring missing files, so catalog construction should succeed
            val catalog = makeCatalog()
            val leafFiles = catalog.listLeafFiles(catalog.rootPaths)
            if (raceCondition == classOf[SubdirectoryDeletionRaceFileSystem]) {
              // The only subdirectory was missing, so there should be no leaf files:
              assert(leafFiles.isEmpty)
            } else {
              assert(raceCondition == classOf[FileDeletionRaceFileSystem])
              // One of the two leaf files was missing, but we should still list the other:
              assert(leafFiles.size == 1)
              assert(leafFiles.head.getPath == nonDeletedLeafFilePath)
            }
          } else {
            // We're NOT ignoring missing files, so catalog construction should fail
            val e = intercept[Exception] {
              makeCatalog()
            }
            // The exact exception depends on whether we're using parallel listing
            if (parDiscoveryThreshold == 0) {
              // The FileNotFoundException occurs in a Spark executor (as part of a job)
              assert(e.isInstanceOf[SparkException])
              assert(e.getMessage.contains("FileNotFoundException"))
            } else {
              // The FileNotFoundException occurs directly on the driver
              assert(e.isInstanceOf[FileNotFoundException])
              // Test that the FileNotFoundException is triggered for the expected reason:
              if (raceCondition == classOf[SubdirectoryDeletionRaceFileSystem]) {
                assert(e.getMessage.contains(subDirPath.toString))
              } else {
                assert(raceCondition == classOf[FileDeletionRaceFileSystem])
                assert(e.getMessage.contains(leafFilePath.toString))
              }
            }
          }
        }
      }
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

  test("SPARK-45452: PartitioningAwareFileIndex uses listFiles API for large child dirs") {
    withSQLConf(SQLConf.USE_LISTFILES_FILESYSTEM_LIST.key -> "file") {
      for (scale <- Seq(10, 50)) {
        withTempDir { dir =>
          for (i <- 1 to scale) {
            new File(dir, s"foo=$i.txt").mkdir()
          }
          HiveCatalogMetrics.reset()
          assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
          new InMemoryFileIndex(spark, Seq(new Path(dir.getCanonicalPath)), Map.empty, None)
          assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
        }
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

  test("SPARK-45452: PartitioningAwareFileIndex listing parallelized with large, deeply nested " +
      "child dirs") {
    withSQLConf(SQLConf.USE_LISTFILES_FILESYSTEM_LIST.key -> "file") {
      for (scale <- Seq(10, 50)) {
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
          assert(HiveCatalogMetrics.METRIC_PARALLEL_LISTING_JOB_COUNT.getCount() == 0)
        }
      }
    }
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

  test ("SPARK-29537: throw exception when user defined a wrong base path") {
    withTempDir { dir =>
      val partitionDirectory = new File(dir, "a=foo")
      partitionDirectory.mkdir()
      val file = new File(partitionDirectory, "text.txt")
      stringToFile(file, "text")
      val path = new Path(dir.getCanonicalPath)
      val wrongBasePath = new File(dir, "unknown")
      // basePath must be a directory
      wrongBasePath.mkdir()
      withClue("SPARK-32368: 'basePath' can be case insensitive") {
        val parameters = Map("bAsepAtH" -> wrongBasePath.getCanonicalPath)
        val fileIndex = new InMemoryFileIndex(spark, Seq(path), parameters, None)
        val msg = intercept[IllegalArgumentException] {
          // trigger inferPartitioning()
          fileIndex.partitionSpec()
        }.getMessage
        assert(msg === s"Wrong basePath ${wrongBasePath.getCanonicalPath} for the root path: $path")
      }
    }
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

  test("SPARK-34075: InMemoryFileIndex filters out hidden file on partition inference") {
    withTempPath { path =>
      spark
        .range(2)
        .select(col("id").as("p"), col("id"))
        .write
        .partitionBy("p")
        .parquet(path.getAbsolutePath)
      val targetPath = new File(path, "p=1")
      val hiddenPath = new File(path, "_hidden_path")
      targetPath.renameTo(hiddenPath)
      assert(spark.read.parquet(path.getAbsolutePath).count() == 1L)
    }
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

  test("Add an option to ignore block locations when listing file") {
    withTempDir { dir =>
      val partitionDirectory = new File(dir, "a=foo")
      partitionDirectory.mkdir()
      for (i <- 1 to 8) {
        val file = new File(partitionDirectory, s"$i.txt")
        stringToFile(file, "text")
      }
      val path = new Path(dir.getCanonicalPath)
      val fileIndex = new InMemoryFileIndex(spark, Seq(path), Map.empty, None)
      withSQLConf(SQLConf.IGNORE_DATA_LOCALITY.key -> "false",
         "fs.file.impl" -> classOf[SpecialBlockLocationFileSystem].getName) {
        val withBlockLocations = fileIndex.
          listLeafFiles(Seq(new Path(partitionDirectory.getPath)))

        withSQLConf(SQLConf.IGNORE_DATA_LOCALITY.key -> "true") {
          val withoutBlockLocations = fileIndex.
            listLeafFiles(Seq(new Path(partitionDirectory.getPath)))

          assert(withBlockLocations.size == withoutBlockLocations.size)
          assert(withBlockLocations.forall(b => b.isInstanceOf[LocatedFileStatus] &&
            b.asInstanceOf[LocatedFileStatus].getBlockLocations.nonEmpty))
          assert(withoutBlockLocations.forall(b => b.isInstanceOf[FileStatus] &&
            !b.isInstanceOf[LocatedFileStatus]))
          assert(withoutBlockLocations.forall(withBlockLocations.contains))
        }
      }
    }
  }

  test("SPARK-31047 - Improve file listing for ViewFileSystem") {
    val path = mock(classOf[Path])
    val dfs = mock(classOf[ViewFileSystem])
    when(path.getFileSystem(any[Configuration])).thenReturn(dfs)
    val statuses =
      Seq(
        new LocatedFileStatus(
          new FileStatus(0, false, 0, 100, 0,
            new Path("file")), Array(new BlockLocation()))
      )
    when(dfs.listLocatedStatus(path)).thenReturn(new RemoteIterator[LocatedFileStatus] {
      val iter = statuses.iterator
      override def hasNext: Boolean = iter.hasNext
      override def next(): LocatedFileStatus = iter.next()
    })
    val fileIndex = new TestInMemoryFileIndex(spark, path)
    assert(fileIndex.leafFileStatuses.toSeq == statuses)
  }

  test("SPARK-48649: Ignore invalid partitions") {
    // Table:
    // id   part_col
    //  1          1
    //  2          2
    val df = spark.range(1, 3, 1, 2).toDF("id")
      .withColumn("part_col", col("id"))

    withTempPath { directoryPath =>
      df.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("part_col")
        .save(directoryPath.getCanonicalPath)

      // Rename one of the folders.
      new File(directoryPath, "part_col=1").renameTo(new File(directoryPath, "undefined"))

      // By default, we expect the invalid path assertion to trigger.
      val ex = intercept[AssertionError] {
        spark.read
          .format("parquet")
          .load(directoryPath.getCanonicalPath)
          .collect()
      }
      assert(ex.getMessage.contains("Conflicting directory structures detected"))

      // With the config enabled, we should only read the valid partition.
      withSQLConf(SQLConf.IGNORE_INVALID_PARTITION_PATHS.key -> "true") {
        assert(
          spark.read
            .format("parquet")
            .load(directoryPath.getCanonicalPath)
            .collect() === Seq(Row(2, 2)))
      }

      // Data source option override takes precedence.
      withSQLConf(SQLConf.IGNORE_INVALID_PARTITION_PATHS.key -> "true") {
        val ex = intercept[AssertionError] {
          spark.read
            .format("parquet")
            .option(FileIndexOptions.IGNORE_INVALID_PARTITION_PATHS, "false")
            .load(directoryPath.getCanonicalPath)
            .collect()
        }
        assert(ex.getMessage.contains("Conflicting directory structures detected"))
      }

      // Data source option override takes precedence.
      withSQLConf(SQLConf.IGNORE_INVALID_PARTITION_PATHS.key -> "false") {
        assert(
          spark.read
            .format("parquet")
            .option(FileIndexOptions.IGNORE_INVALID_PARTITION_PATHS, "true")
            .load(directoryPath.getCanonicalPath)
            .collect() === Seq(Row(2, 2)))
      }
    }
  }

  test("expire FileStatusCache if TTL is configured") {
    val previousValue = SQLConf.get.getConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS)
    try {
      // using 'SQLConf.get.setConf' instead of 'withSQLConf' to set a static config at runtime
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, 1L)

      val path = new Path("/dummy_tmp", "abc")
      val files = (1 to 3).map(_ => new FileStatus())

      FileStatusCache.resetForTesting()
      val fileStatusCache = FileStatusCache.getOrCreate(spark)
      fileStatusCache.putLeafFiles(path, files.toArray)

      // Exactly 3 files are cached.
      assert(fileStatusCache.getLeafFiles(path).get.length === 3)
      // Wait until the cache expiration.
      eventually(timeout(3.seconds)) {
        // And the cache is gone.
        assert(fileStatusCache.getLeafFiles(path).isEmpty === true)
      }
    } finally {
      SQLConf.get.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, previousValue)
    }
  }

  test("SPARK-38182: Fix NoSuchElementException if pushed filter does not contain any " +
    "references") {
    withTable("t") {
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.BooleanSimplification") {

        sql("CREATE TABLE t (c1 int) USING PARQUET")
        assert(sql("SELECT * FROM t WHERE c1 = 1 AND 2 > 1").count() == 0)
      }
    }
  }

  test("SPARK-40667: validate FileIndex Options") {
    assert(FileIndexOptions.getAllOptions.size == 8)
    // Please add validation on any new FileIndex options here
    assert(FileIndexOptions.isValidOption("ignoreMissingFiles"))
    assert(FileIndexOptions.isValidOption("ignoreInvalidPartitionPaths"))
    assert(FileIndexOptions.isValidOption("timeZone"))
    assert(FileIndexOptions.isValidOption("recursiveFileLookup"))
    assert(FileIndexOptions.isValidOption("basePath"))
    assert(FileIndexOptions.isValidOption("modifiedbefore"))
    assert(FileIndexOptions.isValidOption("modifiedafter"))
    assert(FileIndexOptions.isValidOption("pathglobfilter"))
  }
}

object DeletionRaceFileSystem {
  val rootDirPath: Path = new Path("mockFs:///rootDir/")
  val subDirPath: Path = new Path(rootDirPath, "subDir")
  val leafFilePath: Path = new Path(subDirPath, "leafFile")
  val nonDeletedLeafFilePath: Path = new Path(subDirPath, "nonDeletedLeafFile")
  val rootListing: Array[FileStatus] =
    Array(new FileStatus(0, true, 0, 0, 0, subDirPath))
  val subFolderListing: Array[FileStatus] =
    Array(
      new FileStatus(0, false, 0, 100, 0, leafFilePath),
      new FileStatus(0, false, 0, 100, 0, nonDeletedLeafFilePath))
}

// Used in SPARK-27676 test to simulate a race where a subdirectory is deleted
// between back-to-back listing calls.
class SubdirectoryDeletionRaceFileSystem extends RawLocalFileSystem {
  import DeletionRaceFileSystem._

  override def getScheme: String = "mockFs"

  override def listStatus(path: Path): Array[FileStatus] = {
    if (path == rootDirPath) {
      rootListing
    } else if (path == subDirPath) {
      throw new FileNotFoundException(subDirPath.toString)
    } else {
      throw new IllegalArgumentException()
    }
  }
}

// Used in SPARK-27676 test to simulate a race where a file is deleted between
// being listed and having its size / file status checked.
class FileDeletionRaceFileSystem extends RawLocalFileSystem {
  import DeletionRaceFileSystem._

  override def getScheme: String = "mockFs"

  override def listStatus(path: Path): Array[FileStatus] = {
    if (path == rootDirPath) {
      rootListing
    } else if (path == subDirPath) {
      subFolderListing
    } else {
      throw new IllegalArgumentException()
    }
  }

  override def getFileBlockLocations(
      file: FileStatus,
      start: Long,
      len: Long): Array[BlockLocation] = {
    if (file.getPath == leafFilePath) {
      throw new FileNotFoundException(leafFilePath.toString)
    } else {
      Array.empty
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
