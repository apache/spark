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

package org.apache.spark.util

import java.io.File
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}

import org.apache.spark.{SharedSparkContext, SparkFunSuite}

class HadoopFSUtilsDeepNarrowSuite extends SparkFunSuite with SharedSparkContext {

  private val hadoopConf = new Configuration()
  private val acceptAllFilter: PathFilter = (_: Path) => true

  private def createDeepHierarchy(
      baseDir: File,
      levels: Seq[Seq[String]],
      numFiles: Int): Unit = {
    def createLevel(parent: File, remaining: Seq[Seq[String]]): Unit = {
      if (remaining.isEmpty) {
        // Create leaf files
        for (i <- 0 until numFiles) {
          Files.createFile(new File(parent, s"part-$i.parquet").toPath)
        }
      } else {
        for (dirName <- remaining.head) {
          val dir = new File(parent, dirName)
          dir.mkdirs()
          createLevel(dir, remaining.tail)
        }
      }
    }
    createLevel(baseDir, levels)
  }

  test("traversalDepth=1 (default) lists from root level") {
    withTempDir { baseDir =>
      // Create: root/year=2023/month=01/files, root/year=2023/month=02/files
      createDeepHierarchy(
        baseDir,
        Seq(Seq("year=2023"), Seq("month=01", "month=02")),
        numFiles = 3)

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 1)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 6) // 2 months * 3 files
      assert(allFiles.forall(_.getPath.getName.endsWith(".parquet")))
    }
  }

  test("traversalDepth=2 expands single-child root by one level") {
    withTempDir { baseDir =>
      // Deep-narrow: root/subpath/year=2023/files, root/subpath/year=2024/files
      createDeepHierarchy(
        baseDir,
        Seq(Seq("subpath"), Seq("year=2023", "year=2024")),
        numFiles = 4)

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 2)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 8) // 2 years * 4 files
      assert(allFiles.forall(_.getPath.getName.endsWith(".parquet")))
    }
  }

  test("traversalDepth=3 expands three levels deep") {
    withTempDir { baseDir =>
      // root/region/year=2023/month=01/files
      createDeepHierarchy(
        baseDir,
        Seq(Seq("region-us"), Seq("year=2023"), Seq("month=01", "month=02")),
        numFiles = 2)

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 3)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 4) // 2 months * 2 files
      assert(allFiles.forall(_.getPath.getName.endsWith(".parquet")))
    }
  }

  test("traversalDepth handles leaf directories before reaching target depth") {
    withTempDir { baseDir =>
      // Create a shallow hierarchy but request deep traversal
      // root/files (no subdirectories)
      for (i <- 0 until 5) {
        Files.createFile(new File(baseDir, s"part-$i.parquet").toPath)
      }

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 3)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 5)
    }
  }

  test("traversalDepth with wide hierarchy at intermediate level") {
    withTempDir { baseDir =>
      // root/subpath/{year=2020..year=2023}/{month=01..month=12}/files
      val years = (2020 to 2023).map(y => s"year=$y")
      val months = (1 to 12).map(m => f"month=$m%02d")
      createDeepHierarchy(
        baseDir,
        Seq(Seq("subpath"), years, months),
        numFiles = 1)

      val rootPath = new Path(baseDir.getAbsolutePath)
      // traversalDepth=2 should expand root -> subpath -> year=20xx (4 paths)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 2)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 48) // 4 years * 12 months * 1 file
    }
  }

  test("traversalDepth with missing intermediate directory") {
    withTempDir { baseDir =>
      createDeepHierarchy(
        baseDir,
        Seq(Seq("subpath"), Seq("year=2023")),
        numFiles = 2)

      // Add a non-existent path
      val missingPath = new Path(new File(baseDir, "nonexistent").getAbsolutePath)
      val validPath = new Path(baseDir.getAbsolutePath)

      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(validPath, missingPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = true,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 2)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 2)
    }
  }

  test("traversalDepth stops expanding when directory contains mixed files and subdirs") {
    withTempDir { baseDir =>
      // root/subpath/ contains both a file and a subdirectory
      val subpath = new File(baseDir, "subpath")
      subpath.mkdirs()
      Files.createFile(new File(subpath, "loose-file.parquet").toPath)
      val nested = new File(subpath, "year=2023")
      nested.mkdirs()
      Files.createFile(new File(nested, "part-0.parquet").toPath)

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 3)

      val allFiles = result.flatMap(_._2)
      // Both the loose file and the nested file should be discovered
      assert(allFiles.length === 2)
      assert(allFiles.map(_.getPath.getName).toSet === Set("loose-file.parquet", "part-0.parquet"))
    }
  }

  test("traversalDepth filters hidden/underscore directories during sequential traversal") {
    withTempDir { baseDir =>
      // root/subpath/ has a normal dir and hidden dirs
      val subpath = new File(baseDir, "subpath")
      subpath.mkdirs()
      val visible = new File(subpath, "year=2023")
      visible.mkdirs()
      Files.createFile(new File(visible, "part-0.parquet").toPath)
      // Hidden directories that should be filtered
      val hidden = new File(subpath, ".hidden")
      hidden.mkdirs()
      Files.createFile(new File(hidden, "part-0.parquet").toPath)
      val underscore = new File(subpath, "_temporary")
      underscore.mkdirs()
      Files.createFile(new File(underscore, "part-0.parquet").toPath)

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 2)

      val allFiles = result.flatMap(_._2)
      // Only the file under year=2023 should be found
      assert(allFiles.length === 1)
      assert(allFiles.head.getPath.getName === "part-0.parquet")
      assert(allFiles.head.getPath.getParent.getName === "year=2023")
    }
  }

  test("traversalDepth preserves _metadata and _common_metadata directories") {
    withTempDir { baseDir =>
      // root/subpath/ has _metadata, _common_metadata (should be preserved) and _temporary
      val subpath = new File(baseDir, "subpath")
      subpath.mkdirs()
      val metadata = new File(subpath, "_metadata")
      metadata.mkdirs()
      Files.createFile(new File(metadata, "part-0.parquet").toPath)
      val commonMetadata = new File(subpath, "_common_metadata")
      commonMetadata.mkdirs()
      Files.createFile(new File(commonMetadata, "part-0.parquet").toPath)
      val temporary = new File(subpath, "_temporary")
      temporary.mkdirs()
      Files.createFile(new File(temporary, "part-0.parquet").toPath)
      val normal = new File(subpath, "year=2023")
      normal.mkdirs()
      Files.createFile(new File(normal, "part-0.parquet").toPath)

      val rootPath = new Path(baseDir.getAbsolutePath)
      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(rootPath),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 2)

      val allFiles = result.flatMap(_._2)
      val parentNames = allFiles.map(_.getPath.getParent.getName).toSet
      // _metadata and _common_metadata should be preserved, _temporary should be filtered
      assert(parentNames.contains("_metadata"))
      assert(parentNames.contains("_common_metadata"))
      assert(parentNames.contains("year=2023"))
      assert(!parentNames.contains("_temporary"))
    }
  }

  test("traversalDepth=1 and traversalDepth>1 produce identical file listings") {
    withTempDir { baseDir =>
      // root/region/year=2023/month={01,02}/files
      createDeepHierarchy(
        baseDir,
        Seq(Seq("region"), Seq("year=2023"), Seq("month=01", "month=02")),
        numFiles = 3)

      val rootPath = new Path(baseDir.getAbsolutePath)

      def listWithDepth(depth: Int): Set[String] = {
        HadoopFSUtils.parallelListLeafFiles(
          sc = sc,
          paths = Seq(rootPath),
          hadoopConf = hadoopConf,
          filter = acceptAllFilter,
          ignoreMissingFiles = false,
          ignoreLocality = true,
          parallelismThreshold = 32,
          parallelismMax = 10,
          traversalDepth = depth
        ).flatMap(_._2).map(_.getPath.toString).toSet
      }

      val depth1 = listWithDepth(1)
      val depth2 = listWithDepth(2)
      val depth3 = listWithDepth(3)

      assert(depth1.size === 6)
      assert(depth1 === depth2)
      assert(depth1 === depth3)
    }
  }

  test("traversalDepth with multiple input root paths") {
    withTempDir { baseDir =>
      // Two separate root paths, each with a narrow hierarchy
      val root1 = new File(baseDir, "table1")
      val root2 = new File(baseDir, "table2")
      createDeepHierarchy(root1, Seq(Seq("region"), Seq("year=2023")), numFiles = 2)
      createDeepHierarchy(root2, Seq(Seq("region"), Seq("year=2024")), numFiles = 3)

      val result = HadoopFSUtils.parallelListLeafFiles(
        sc = sc,
        paths = Seq(new Path(root1.getAbsolutePath), new Path(root2.getAbsolutePath)),
        hadoopConf = hadoopConf,
        filter = acceptAllFilter,
        ignoreMissingFiles = false,
        ignoreLocality = true,
        parallelismThreshold = 32,
        parallelismMax = 10,
        traversalDepth = 2)

      val allFiles = result.flatMap(_._2)
      assert(allFiles.length === 5) // 2 + 3
    }
  }
}
