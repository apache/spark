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

package org.apache.spark.deploy

import java.io.{File, FileOutputStream}

import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Tests for `SparkHadoopUtil` methods.
 */
class SparkHadoopUtilSuite extends SparkFunSuite with Matchers with Logging
with BeforeAndAfter {

  val sparkConf = new SparkConf()
  val hadoopUtils = new SparkHadoopUtil()
  val tempDir = Utils.createTempDir("SparkHadoopUtilSuite")

  before {
    tempDir.mkdirs()
  }

  after {
    Utils.deleteRecursively(tempDir)
  }

  test("GlobNoFileSimple") {
    val file = tempFile("does-not-exist")
    assertEmptyGlob(toPath(file))
  }

  test("GlobNoFileWildcard") {
    val file = tempFile("does-not-exist")
    val path = toPath(file)
    assertEmptyGlob(new Path(path, "*"))
  }

  /**
   * Glob a simple file and expect to get the path back.
   */
  test("GlobSimpleFile") {
    val name = "simplefile"
    val file = tempFile(name)
    val path = toPath(file)
    touch(file)
    globToSize(path, 1)(0).getPath should be(path)
  }

  /**
   * Glob a simple file + wildcard and expect nothing back.
   */
  test("GlobSimpleFileWildcard") {
    val file = tempFile("simplefile")
    touch(file)
    assertEmptyGlob(toWildcardPath(file))
  }

  /**
   * Glob an empty dir and expect to get the directory back.
   */
  test("GlobEmptyDir") {
    val file = tempFile("emptydir")
    val path = toPath(file)
    file.mkdirs()
    globToSize(path, 1)(0).getPath should be(path)
  }

  /**
   * Glob an empty dir + wildcard and expect nothing back.
   */
  test("GlobEmptyDirWildcard") {
    val file = tempFile("emptydir")
    file.mkdirs()
    assertEmptyGlob(toWildcardPath(file))
  }

  /**
   * Glob a directory with children and expect to only get the directory back.
   */
  test("GlobNonEmptyDir") {
    val file = tempFile("dir")
    val path = toPath(file)
    file.mkdirs()
    val child = new File(file, "child")
    touch(child)
    globToSize(path, 1)(0).getPath should be(path)
  }

  /**
   * Glob a non empty dir + wildcard and expect to get the child back.
   */
  test("GlobNonEmptyDirWildcard") {
    val file = tempFile("dir")
    file.mkdirs()
    val path = toPath(file)
    file.mkdirs()
    val child = new File(file, "child")
    touch(child)
    globToSize(toWildcardPath(file), 1)(0).getPath should be(toPath(child))
  }

  /**
   * Assert that the glob returned an empty list.
   * @param pattern pattern to glob
   */
  def assertEmptyGlob(pattern: Path): Unit = {
    assert(Array.empty[FileStatus] === hadoopUtils.globToFileStatusIfNecessary(pattern),
      s"globToFileStatus($pattern)")
  }

  /**
   * glob to an expected size of returned array.
   * @param pattern pattern to glob
   * @param size size to expect
   * @return a list of results
   */
  def globToSize(pattern: Path, size: Int): Array[FileStatus] = {
    val result = hadoopUtils.globToFileStatusIfNecessary(pattern)
    assert(size === result.length,
      s"globToFileStatus($pattern) = $result")
    result
  }

  /**
   * Create a 0-byte file at the given path.
   * @param file file to create
   */
  def touch(file: File): Unit = {
    file.getParentFile.mkdirs()
    new FileOutputStream(file, false).close()
  }

  /**
   * Convert a file to a path.
   * @param file file
   * @return the path equivalent.
   */
  def toPath(file: File): Path = {
    new Path(file.toURI)
  }

  /**
   * Create a wildcard matching all children of the given path.
   * @param file file
   * @return a path
   */
  def toWildcardPath(file: File): Path = {
    new Path(toPath(file), "*")
  }

  /**
   * Get a File instance for a filename in the temp directory.
   * No file is created.
   * @param name filename
   * @return an absolute file under the temporary directory
   */
  def tempFile(name: String): File = {
    new File(tempDir, name).getAbsoluteFile
  }

}
