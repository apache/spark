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
import scala.language.reflectiveCalls

import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.SharedSQLContext

class FileIndexSuite extends SharedSQLContext {

  test("InMemoryFileIndex: leaf files are qualified paths") {
    withTempDir { dir =>
      val file = new File(dir, "text.txt")
      stringToFile(file, "text")

      val path = new Path(file.getCanonicalPath)
      val catalog = new InMemoryFileIndex(spark, Seq(path), Map.empty, None) {
        def leafFilePaths: Seq[Path] = leafFiles.keys.toSeq
        def leafDirPaths: Seq[Path] = leafDirToChildrenFiles.keys.toSeq
      }
      assert(catalog.leafFilePaths.forall(p => p.toString.startsWith("file:/")))
      assert(catalog.leafDirPaths.forall(p => p.toString.startsWith("file:/")))
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

      val fs = unqualifiedDirPath.getFileSystem(sparkContext.hadoopConfiguration)
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

  test("PartitioningAwareFileIndex - file filtering") {
    assert(!PartitioningAwareFileIndex.shouldFilterOut("abcd"))
    assert(PartitioningAwareFileIndex.shouldFilterOut(".ab"))
    assert(PartitioningAwareFileIndex.shouldFilterOut("_cd"))
    assert(!PartitioningAwareFileIndex.shouldFilterOut("_metadata"))
    assert(!PartitioningAwareFileIndex.shouldFilterOut("_common_metadata"))
    assert(PartitioningAwareFileIndex.shouldFilterOut("_ab_metadata"))
    assert(PartitioningAwareFileIndex.shouldFilterOut("_cd_common_metadata"))
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
}

class FakeParentPathFileSystem extends RawLocalFileSystem {
  override def getScheme: String = "mockFs"

  override def getUri: URI = {
    URI.create("mockFs://some-bucket")
  }
}
