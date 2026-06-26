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

import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.util.Utils

/**
 * Binds [[ArchiveReadSuiteBase]]'s file-format hooks to Parquet. Unlike the streaming formats,
 * Parquet entries are unpacked to a local file before reading (Parquet needs random access to its
 * footer), but the read/inference parity contract the base verifies is identical. Parquet is
 * self-describing, so [[inferenceOptions]] stays empty and the base's schema-inference tests run as
 * well. There is no header concept, so (unlike CSV) there is a single base rather than header and
 * headerless variants.
 */
trait ParquetArchiveReadBase extends ArchiveReadSuiteBase {

  override protected def format: String = "parquet"

  override protected def fileExtension: String = "parquet"

  override protected def readOptions: Map[String, String] = Map.empty

  override protected def readSchema: String = "id INT, name STRING"

  // Parquet has an authoritative per-file schema and only unions differing fields across files when
  // `mergeSchema` is set, so it does not union by name during default inference the way the
  // schema-on-read formats do. The differing-fields *read* (missing column -> null) is still
  // supported and is covered by a Parquet-specific test in ParquetTarArchiveReadSuite.
  override protected def supportsSchemaMerge: Boolean = false

  override protected def encodeFile(
      df: DataFrame,
      writeOptions: Map[String, String]): Array[Byte] = {
    val dir = Utils.createTempDir(namePrefix = "archive-test-encode")
    try {
      df.coalesce(1).write.format("parquet")
        .options(writeOptions).mode("overwrite").save(dir.getCanonicalPath)
      val parts = dir.listFiles().filter { f =>
        f.isFile && !f.getName.startsWith("_") && !f.getName.startsWith(".") &&
          !f.getName.endsWith(".crc")
      }
      assert(parts.length == 1,
        s"expected exactly one data file, got: ${parts.map(_.getName).toList}")
      Files.readAllBytes(parts.head.toPath)
    } finally Utils.deleteRecursively(dir)
  }
}
