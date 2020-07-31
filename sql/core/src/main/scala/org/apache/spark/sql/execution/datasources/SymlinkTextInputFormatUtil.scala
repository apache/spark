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

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import com.google.common.io.CharStreams
import org.apache.hadoop.fs.{FileSystem, Path}

object SymlinkTextInputFormatUtil {

  def isSymlinkTextFormat(inputFormat: String): Boolean = {
    inputFormat.equals("org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
  }

  // Mostly copied from SymlinkTextInputFormat#getTargetPathsFromSymlinksDirs of Hive 3.1
  def getTargetPathsFromSymlink(
      fileSystem: FileSystem,
      symlinkDir: Path): Seq[Path] = {

    val symlinkIterator = fileSystem.listFiles(symlinkDir, true)
    var targetPaths = Seq[Path]()

    while (symlinkIterator.hasNext) {
      val fileStatus = symlinkIterator.next()
      if (fileStatus.isFile) {
        val reader = new BufferedReader(
          new InputStreamReader(fileSystem.open(fileStatus.getPath), UTF_8))
        try {
          val targets: Seq[Path] = CharStreams.readLines(reader).asScala.
            map(symlinkStr => new Path(symlinkStr))
          targetPaths = targetPaths ++ targets
        } finally {
          reader.close
        }
      }
    }
    targetPaths
  }
}
