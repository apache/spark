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

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.CharStreams
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogTable

object SymlinkTextInputFormatUtil {

  /**
   * Determine if InputFormat is SymlinkTable
   *
   * @param inputFormat Table InputFormat
   * @return
   */
  def isSymlinkTextFormat(inputFormat: String): Boolean = {
    inputFormat.equals("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
  }

  /**
   * Determine CatalogTable is SymlinkTable
   *
   * @param catalogTable CatalogTable
   * @return
   */
  def isSymlinkTextFormat(catalogTable: CatalogTable): Boolean = {
    catalogTable.storage.inputFormat.exists(isSymlinkTextFormat)
  }

  /**
   * Get symlink files from target path
   * Mostly copied from BackgroundHiveSplitLoader#getTargetPathsFromSymlink of trino(prestosql)
   * compatible with hive SymlinkTextInputFormat#getTargetPathsFromSymlinksDirs
   *
   * @param fileSystem filesystem
   * @param symlinkDir symlink table location
   * @return
   */
  def getTargetPathsFromSymlink(
      fileSystem: FileSystem,
      symlinkDir: Path): Seq[Path] = {

    val symlinks = fileSystem.listStatus(symlinkDir, new PathFilter() {
      override def accept(p: Path): Boolean = DataSourceUtils.isDataPath(p)
    })

    symlinks.flatMap {
      case fileStatus if fileStatus.isFile =>
        val reader = new BufferedReader(
          new InputStreamReader(fileSystem.open(fileStatus.getPath), UTF_8))
        try {
          CharStreams.readLines(reader).asScala
              .map(symlinkStr => new Path(symlinkStr))
        } finally {
          reader.close()
          Seq.empty
        }
      case _ =>
        Seq.empty
    }
  }

  /**
   * Get symlink uris from target path
   *
   * @param fileSystem filesystem
   * @param location symlink table location
   * @return
   */
  def getSymlinkTableLocationPaths(fileSystem: FileSystem, location: URI): Seq[Option[URI]] = {
    SymlinkTextInputFormatUtil
      .getTargetPathsFromSymlink(fileSystem, new Path(location))
      .map(path => Option(path.toUri))
  }
}
