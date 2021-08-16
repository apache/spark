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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.apache.orc.impl.{OrcTail, ReaderImpl}

import org.apache.spark.sql.execution.datasources.{FileMeta, FileMetaCacheManager, FileMetaKey}
import org.apache.spark.util.Utils

case class OrcFileMetaKey(path: Path, configuration: Configuration)
  extends FileMetaKey {
  override def getFileMeta: OrcFileMeta = OrcFileMeta(path, configuration)
}

case class OrcFileMeta(tail: OrcTail) extends FileMeta

object OrcFileMeta {
  def apply(path: Path, conf: Configuration): OrcFileMeta = {
    val fs = path.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    Utils.tryWithResource(new ReaderImpl(path, readerOptions)) { fileReader =>
      new OrcFileMeta(new OrcTail(fileReader.getFileTail, fileReader.getSerializedFileFooter))
    }
  }

  def readTailFromCache(path: Path, conf: Configuration): OrcTail =
    FileMetaCacheManager.get(OrcFileMetaKey(path, conf)).asInstanceOf[OrcFileMeta].tail
}
