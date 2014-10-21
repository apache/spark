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
package org.apache.spark.streaming.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}

private[streaming] object HdfsUtils {

  def getOutputStream(path: String, conf: Configuration): FSDataOutputStream = {
    // HDFS is not thread-safe when getFileSystem is called, so synchronize on that

    val dfsPath = new Path(path)
    val dfs =
      this.synchronized {
        dfsPath.getFileSystem(conf)
      }
    // If the file exists and we have append support, append instead of creating a new file
    val stream: FSDataOutputStream = {
      if (dfs.isFile(dfsPath)) {
        if (conf.getBoolean("hdfs.append.support", false)) {
          dfs.append(dfsPath)
        } else {
          throw new IllegalStateException("File exists and there is no append support!")
        }
      } else {
        dfs.create(dfsPath)
      }
    }
    stream
  }

  def getInputStream(path: String, conf: Configuration): FSDataInputStream = {
    val dfsPath = new Path(path)
    val dfs = this.synchronized {
      dfsPath.getFileSystem(conf)
    }
    val instream = dfs.open(dfsPath)
    instream
  }

  def checkState(state: Boolean, errorMsg: => String) {
    if(!state) {
      throw new IllegalStateException(errorMsg)
    }
  }

  def getBlockLocations(path: String, conf: Configuration): Option[Array[String]] = {
    val dfsPath = new Path(path)
    val dfs =
      this.synchronized {
        dfsPath.getFileSystem(conf)
      }
    val fileStatus = dfs.getFileStatus(dfsPath)
    val blockLocs = Option(dfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen))
    blockLocs.map(_.flatMap(_.getHosts))
  }
}
