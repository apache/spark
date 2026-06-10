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
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.LocalSparkContext.withSpark

class HadoopFSUtilsSuite extends SparkFunSuite {

  // Accept everything; hidden-file filtering is exercised via the listHiddenFiles flag.
  private val acceptAllFilter: PathFilter = AcceptAllPathFilter

  /**
   * Builds a tree with one regular file, hidden entries ('_'-, '.'-, '._COPYING_'-named) and a
   * hidden subdir with its own file.
   *
   * @return a tuple of (root path to pass to the listing APIs, name of the only non-hidden file
   *         in the tree).
   */
  private def createHiddenFileTree(root: File): (Path, String) = {
    def writeFile(parent: File, name: String): Unit = {
      val file = new File(parent, name)
      Files.write(file.toPath, "content".getBytes)
    }
    writeFile(root, "data.parquet")
    writeFile(root, "_hidden")
    writeFile(root, ".dot")
    writeFile(root, "x._COPYING_")
    val hiddenDir = new File(root, "_tmp")
    assert(hiddenDir.mkdir())
    writeFile(hiddenDir, "nested.parquet")
    // Use getCanonicalPath, not toURI: toURI's trailing slash breaks HadoopFSUtils' prefix
    // stripping and defeats shouldFilterOutPath's leading-'/' match.
    (new Path(root.getCanonicalPath), "data.parquet")
  }

  // The set of leaf-file names surfaced for the root path.
  private def leafFileNames(listing: Seq[(Path, Seq[FileStatus])]): Set[String] =
    listing.flatMap(_._2).map(_.getPath.getName).toSet

  test("HadoopFSUtils - file filtering") {
    assert(!HadoopFSUtils.shouldFilterOutPathName("abcd"))
    assert(HadoopFSUtils.shouldFilterOutPathName(".ab"))
    assert(HadoopFSUtils.shouldFilterOutPathName("_cd"))
    assert(!HadoopFSUtils.shouldFilterOutPathName("_metadata"))
    assert(!HadoopFSUtils.shouldFilterOutPathName("_common_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPathName("_ab_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPathName("_cd_common_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPathName("a._COPYING_"))
    // listHiddenFiles short-circuits the predicates: nothing is considered hidden.
    assert(!HadoopFSUtils.shouldFilterOutPathName(".ab", listHiddenFiles = true))
    assert(!HadoopFSUtils.shouldFilterOutPath("/.ab", listHiddenFiles = true))
  }

  test("SPARK-45452: HadoopFSUtils - path filtering") {
    // Case 1: Regular and metadata paths
    assert(!HadoopFSUtils.shouldFilterOutPath("/abcd"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/abcd/efg"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/year=2023/month=10/day=8/hour=13"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/part=__HIVE_DEFAULT_PARTITION__"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_cd=123"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_cd=123/1"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_metadata"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_metadata/1"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_common_metadata"))
    assert(!HadoopFSUtils.shouldFilterOutPath("/_common_metadata/1"))
    // Case 2: Hidden paths and the paths ending `._COPYING_`
    assert(HadoopFSUtils.shouldFilterOutPath("/.ab"))
    assert(HadoopFSUtils.shouldFilterOutPath("/.ab/cde"))
    assert(HadoopFSUtils.shouldFilterOutPath("/.ab/_metadata/1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/fg"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/_common_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/.cde/year=2023/month=10/day=8/hour=13"))
    assert(HadoopFSUtils.shouldFilterOutPath("/x/.hidden/part=__HIVE_DEFAULT_PARTITION__"))
    assert(HadoopFSUtils.shouldFilterOutPath("/a._COPYING_"))
    // Case 3: Underscored paths (except metadata paths of Case 1)
    assert(HadoopFSUtils.shouldFilterOutPath("/_cd"))
    assert(HadoopFSUtils.shouldFilterOutPath("/_cd/1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/_cd/1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/ab/_cd/part=1"))
    assert(HadoopFSUtils.shouldFilterOutPath("/_ab_metadata"))
    assert(HadoopFSUtils.shouldFilterOutPath("/_cd_common_metadata"))
  }

  test("listFiles - listHiddenFiles=false filters hidden files and dirs") {
    withTempDir { root =>
      val (path, regularFile) = createHiddenFileTree(root)
      val hadoopConf = new Configuration()
      val names = leafFileNames(
        HadoopFSUtils.listFiles(path, hadoopConf, acceptAllFilter, listHiddenFiles = false))
      // Only the regular file survives; every hidden entry is filtered out.
      assert(names === Set(regularFile))
    }
  }

  test("listFiles - listHiddenFiles=true surfaces hidden files and dirs") {
    withTempDir { root =>
      val (path, regularFile) = createHiddenFileTree(root)
      val hadoopConf = new Configuration()
      val names = leafFileNames(
        HadoopFSUtils.listFiles(path, hadoopConf, acceptAllFilter, listHiddenFiles = true))
      assert(names === Set(regularFile, "_hidden", ".dot", "x._COPYING_", "nested.parquet"))
    }
  }

  test("parallelListLeafFiles - listHiddenFiles toggles hidden file visibility") {
    withTempDir { root =>
      val (path, regularFile) = createHiddenFileTree(root)
      val hadoopConf = new Configuration()
      withSpark(new SparkContext("local", "HadoopFSUtilsSuite")) { sc =>
        def listNames(listHiddenFiles: Boolean): Set[String] =
          leafFileNames(HadoopFSUtils.parallelListLeafFiles(
            sc,
            Seq(path),
            hadoopConf,
            acceptAllFilter,
            ignoreMissingFiles = false,
            listHiddenFiles = listHiddenFiles,
            ignoreLocality = true,
            // Use 0 so the parallel (Spark job) code path is exercised rather than the
            // serial short-circuit.
            parallelismThreshold = 0,
            parallelismMax = 1))

        assert(listNames(listHiddenFiles = false) === Set(regularFile))
        assert(listNames(listHiddenFiles = true) ===
          Set(regularFile, "_hidden", ".dot", "x._COPYING_", "nested.parquet"))
      }
    }
  }
}

private object AcceptAllPathFilter extends PathFilter with Serializable {
  override def accept(path: Path): Boolean = true
}
