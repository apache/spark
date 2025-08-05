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
import java.net.{URI, URISyntaxException}
import java.nio.file.{Files, Path, StandardCopyOption}
import java.nio.file.attribute.FileTime

import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.network.util.JavaUtils

private[spark] trait SparkFileUtils extends Logging {
  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getCanonicalFile().toURI()
  }

  /**
   * Size of files recursively.
   */
  def sizeOf(f: File): Long = {
    JavaUtils.sizeOf(f)
  }

  /**
   * Lists files recursively.
   */
  def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val result = f.listFiles.toBuffer
    val dirList = result.filter(_.isDirectory)
    while (dirList.nonEmpty) {
      val curDir = dirList.remove(0)
      val files = curDir.listFiles()
      result ++= files
      dirList ++= files.filter(_.isDirectory)
    }
    result.toArray
  }

  /**
   * Lists regular files recursively.
   */
  def listFiles(f: File): java.util.Set[File] = {
    JavaUtils.listFiles(f)
  }

  /**
   * Lists regular paths recursively.
   */
  def listPaths(f: File): java.util.Set[Path] = {
    JavaUtils.listPaths(f)
  }

  /**
   * Create a directory given the abstract pathname
   * @return true, if the directory is successfully created; otherwise, return false.
   */
  def createDirectory(dir: File): Boolean = {
    try {
      // SPARK-35907: The check was required by File.mkdirs() because it could sporadically
      // fail silently. After switching to Files.createDirectories(), ideally, there should
      // no longer be silent fails. But the check is kept for the safety concern. We can
      // remove the check when we're sure that Files.createDirectories() would never fail silently.
      Files.createDirectories(dir.toPath)
      if ( !dir.exists() || !dir.isDirectory) {
        logError(log"Failed to create directory ${MDC(LogKeys.PATH, dir)}")
      }
      dir.isDirectory
    } catch {
      case e: Exception =>
        logError(log"Failed to create directory ${MDC(LogKeys.PATH, dir)}", e)
        false
    }
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    JavaUtils.createDirectory(root, namePrefix)
  }

  /**
   * Create a temporary directory inside the `java.io.tmpdir` prefixed with `spark`.
   * The directory will be automatically deleted when the VM shuts down.
   */
  def createTempDir(): File =
    createTempDir(System.getProperty("java.io.tmpdir"), "spark")

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    createDirectory(root, namePrefix)
  }

  /** Delete recursively while keeping the given directory itself. */
  def cleanDirectory(dir: File): Unit = {
    JavaUtils.cleanDirectory(dir)
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File): Unit = {
    JavaUtils.deleteRecursively(file)
  }

  /** Delete a file or directory and its contents recursively without throwing exceptions. */
  def deleteQuietly(file: File): Unit = {
    JavaUtils.deleteQuietly(file)
  }

  def getFile(names: String*): File = {
    require(names != null && names.forall(_ != null))
    names.tail.foldLeft(Path.of(names.head)) { (path, part) =>
      path.resolve(part)
    }.toFile
  }

  def getFile(parent: File, names: String*): File = {
    require(parent != null && names != null && names.forall(_ != null))
    names.foldLeft(parent.toPath) { (path, part) =>
      path.resolve(part)
    }.toFile
  }

  /** Move src to dst simply. File attribute times are not copied. */
  def moveFile(src: File, dst: File): Unit = {
    JavaUtils.moveFile(src, dst)
  }

  /** Move src to dst simply. File attribute times are not copied. */
  def moveDirectory(src: File, dst: File): Unit = {
    JavaUtils.moveDirectory(src, dst)
  }

  /** Copy src to the target directory simply. File attribute times are not copied. */
  def copyDirectory(src: File, dir: File): Unit = {
    JavaUtils.copyDirectory(src, dir)
  }

  /** Copy file to the target directory simply. File attribute times are not copied. */
  def copyFileToDirectory(file: File, dir: File): Unit = {
    if (file == null || dir == null || !file.exists() || (dir.exists() && !dir.isDirectory())) {
      throw new IllegalArgumentException(s"Invalid input file $file or directory $dir")
    }
    Files.createDirectories(dir.toPath())
    val newFile = new File(dir, file.getName())
    Files.copy(file.toPath(), newFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
  }

  def copyFile(src: File, dst: File): Unit = {
    if (src == null || dst == null || !src.exists() || (dst.exists() && dst.isDirectory())) {
      throw new IllegalArgumentException(s"Invalid input file $src or directory $dst")
    }
    Files.copy(src.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING)
  }

  /** Return true if the content of the files are equal or they both don't exist */
  def contentEquals(file1: File, file2: File): Boolean = {
    if (file1 == null && file2 != null || file1 != null && file2 == null) {
      false
    } else if (file1 == null && file2 == null || !file1.exists() && !file2.exists()) {
      true
    } else if (!file1.exists() || !file2.exists()) {
      false
    } else if (file1.isDirectory() || file2.isDirectory()) {
      throw new IllegalArgumentException(s"Input is not a file: $file1 or $file2")
    } else if (file1.length != file2.length) {
      false
    } else {
      val path1 = file1.toPath
      val path2 = file2.toPath
      Files.isSameFile(path1, path2) || Files.mismatch(path1, path2) == -1L
    }
  }

  def touch(file: File): Unit = {
    if (file == null) {
      throw new IllegalArgumentException("Invalid input file: null")
    }
    val path = file.toPath
    if (Files.exists(path)) {
      Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis()))
    } else {
      val parent = path.getParent()
      if (parent != null && !Files.exists(parent)) {
        Files.createDirectories(parent)
      }
      Files.createFile(path)
    }
  }
}

private[spark] object SparkFileUtils extends SparkFileUtils
