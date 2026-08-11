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

import org.apache.spark.sql.internal.SQLConf

/**
 * Binds [[ArchiveReadSuiteBase]]'s hooks to ORC (entries unpacked to a local file for footer
 * random access). ORC is self-describing, so the base's schema-inference tests run too.
 */
trait OrcArchiveReadBase extends ArchiveReadSuiteBase {

  override protected def format: String = "orc"

  override protected def fileExtension: String = "orc"

  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // ORC has authoritative per-file schemas and only unions under `mergeSchema`, so it opts out of
  // the by-name default-inference union (covered by the shared localize-path tests instead).
  override protected def supportsSchemaMerge: Boolean = false

  // ORC samples one part-file for non-merge inference.
  override protected def inferenceSamplesOneFile: Boolean = true

  // ORC unpacks each entry to a local temp file for footer random access.
  override protected def localizesEntries: Boolean = true

  override protected def archiveTempDirPrefix: String = "orc-archive"

  override protected def vectorizedReaderConfKey: Option[String] =
    Some(SQLConf.ORC_VECTORIZED_READER_ENABLED.key)
}

class OrcTarArchiveReadSuite
  extends ArchiveReadSuiteBase
  with OrcArchiveReadBase
  with TarArchiveReadBase

class OrcZipArchiveReadSuite
  extends ArchiveReadSuiteBase
  with OrcArchiveReadBase
  with ZipArchiveReadBase

class OrcSevenZArchiveReadSuite
  extends ArchiveReadSuiteBase
  with OrcArchiveReadBase
  with SevenZArchiveReadBase
