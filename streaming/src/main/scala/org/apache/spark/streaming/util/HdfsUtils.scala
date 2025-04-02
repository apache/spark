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
package org.apache.spark.streaming.util

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.deploy.SparkHadoopUtil

private[streaming] object HdfsUtils {

  def getOutputStream(path: String, conf: Configuration): FSDataOutputStream = {
    val dfsPath = new Path(path)
    val dfs = getFileSystemForPath(dfsPath, conf)
    // If the file exists and we have append support, append instead of creating a new file
    val stream: FSDataOutputStream = {
      if (SparkHadoopUtil.isFile(dfs, dfsPath)) {
        if (conf.getBoolean("dfs.support.append", true) ||
            conf.getBoolean("hdfs.append.support", false) ||
            dfs.isInstanceOf[RawLocalFileSystem]) {
          dfs.append(dfsPath)
        } else {
          throw new IllegalStateException("File exists and there is no append support!")
        }
      } else {
        // we don't want to use hdfs erasure coding, as that lacks support for append and hflush
        SparkHadoopUtil.createFile(dfs, dfsPath, false)
      }
    }
    stream
  }

  def getInputStream(path: String, conf: Configuration): FSDataInputStream = {
    val dfsPath = new Path(path)
    val dfs = getFileSystemForPath(dfsPath, conf)
    try {
      dfs.open(dfsPath)
    } catch {
      case _: FileNotFoundException =>
        null
      case e: IOException =>
        // If we are really unlucky, the file may be deleted as we're opening the stream.
        // This can happen as clean up is performed by daemon threads that may be left over from
        // previous runs.
        if (!dfs.getFileStatus(dfsPath).isFile) null else throw e
    }
  }

  def checkState(state: Boolean, errorMsg: => String): Unit = {
    if (!state) {
      throw new IllegalStateException(errorMsg)
    }
  }

  /** Get the locations of the HDFS blocks containing the given file segment. */
  def getFileSegmentLocations(
      path: String, offset: Long, length: Long, conf: Configuration): Array[String] = {
    val dfsPath = new Path(path)
    val dfs = getFileSystemForPath(dfsPath, conf)
    val fileStatus = dfs.getFileStatus(dfsPath)
    val blockLocs = Option(dfs.getFileBlockLocations(fileStatus, offset, length))
    blockLocs.map(_.flatMap(_.getHosts)).getOrElse(Array.empty)
  }

  def getFileSystemForPath(path: Path, conf: Configuration): FileSystem = {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    val fs = path.getFileSystem(conf)
    fs match {
      case localFs: LocalFileSystem => localFs.getRawFileSystem
      case _ => fs
    }
  }

  /** Check if the file exists at the given path. */
  def checkFileExists(path: String, conf: Configuration): Boolean = {
    val hdpPath = new Path(path)
    val fs = getFileSystemForPath(hdpPath, conf)
    try {
      fs.getFileStatus(hdpPath).isFile
    } catch {
      case _: FileNotFoundException => false
    }
  }
}
