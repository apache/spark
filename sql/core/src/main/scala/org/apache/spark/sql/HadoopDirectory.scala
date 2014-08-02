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

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsAction
import org.apache.spark.sql.catalyst.plans.physical.TableLocation

/**
 * Represents the location of a table that exists within a single Hadoop directory.
 */
private[sql] class HadoopDirectory(path: String) extends TableLocation with HadoopDirectoryLike {
  def this(path: Path) = this(path.toString)

  def asString = path
  def asPath = new Path(path)
  def asHadoopDirectory = this

  override def equals(other: Any) = other match {
    case o: HadoopDirectory =>
      o.asString == asString
    case o: HadoopDirectoryLike =>
      o.asHadoopDirectory.asString == asString
    case _ => false
  }

  override def toString = asString
}

private[sql] object HadoopDirectory {

  /**
   * Creates a HadoopDirectory object that contains a fully-qualified path. This method also ensures
   * that the given directory is writable by Spark, and, optionally, whether is already contains
   * files.
   */
  def createChecked(
      pathString: String,
      conf: Configuration,
      allowExisting: Boolean = false): HadoopDirectory = {
    val path = new Path(pathString)
    require(path != null, "Unable to create ParquetRelation: path is null")

    val fs = path.getFileSystem(conf)
    require(fs != null, s"Unable to create ParquetRelation: incorrectly formatted path $path")

    if (!allowExisting && fs.exists(path) && fs.listStatus(path).nonEmpty) {
      sys.error(s"File $path already exists and is not empty.")
    }

    if (fs.exists(path) &&
      !fs.getFileStatus(path)
        .getPermission
        .getUserAction
        .implies(FsAction.READ_WRITE)) {
      throw new IOException(
        s"Unable to create ParquetRelation: path $path not read-writable")
    }
    new HadoopDirectory(fs.makeQualified(path))
  }
}

private[sql] trait HadoopDirectoryLike {
  def asHadoopDirectory: HadoopDirectory
}
