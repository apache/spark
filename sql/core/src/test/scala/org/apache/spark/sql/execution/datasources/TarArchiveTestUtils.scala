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

import java.io.{File, FileOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale
import java.util.zip.GZIPOutputStream

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}

/**
 * Tar container-writing helpers (plain `.tar`, gzipped `.tar.gz`, and `.tgz`), independent of any
 * read-test harness. [[TarArchiveReadBase]] mixes this into [[ArchiveReadSuiteBase]] for the
 * format-agnostic suites, and standalone suites that cannot extend `ArchiveReadSuiteBase` (e.g.
 * `TextTarArchiveReadSuite`, whose single `value` column doesn't fit the two-column shared tests)
 * mix it in directly, so the container logic lives in one place.
 */
trait TarArchiveTestUtils {

  /** Tar extensions to exercise; the head is the default. */
  protected def archiveExtensions: Seq[String] = Seq("tar", "tar.gz", "tgz")

  /** Writes `entries` (name -> bytes) into the archive at `dest`; compression follows the ext. */
  protected def writeArchive(dest: File, entries: Seq[(String, Array[Byte])]): Unit = {
    val name = dest.getName.toLowerCase(Locale.ROOT)
    val rawOut: OutputStream = if (name.endsWith(".gz") || name.endsWith(".tgz")) {
      new GZIPOutputStream(new FileOutputStream(dest))
    } else {
      new FileOutputStream(dest)
    }
    val out = new TarArchiveOutputStream(rawOut)
    try {
      entries.foreach { case (entryName, bytes) =>
        val entry = new TarArchiveEntry(entryName)
        entry.setSize(bytes.length.toLong)
        out.putArchiveEntry(entry)
        out.write(bytes)
        out.closeArchiveEntry()
      }
      out.finish()
    } finally out.close()
  }

  /** Writes bytes that are not a valid gzip-compressed tar archive to `dest`. */
  protected def writeCorruptArchive(dest: File): Unit =
    Files.write(dest.toPath, "this is not a valid gzip-compressed tar archive"
      .getBytes(StandardCharsets.UTF_8))
}
