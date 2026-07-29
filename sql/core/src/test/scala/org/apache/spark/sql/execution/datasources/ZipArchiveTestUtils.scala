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

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}

/**
 * Zip container-writing helpers (`.zip`), independent of any read-test harness -- the zip peer of
 * [[TarArchiveTestUtils]]. [[ZipArchiveReadBase]] mixes this into [[ArchiveReadSuiteBase]] for the
 * format-agnostic suites, and standalone suites that cannot extend `ArchiveReadSuiteBase` (e.g.
 * `TextZipArchiveReadSuite`) mix it in directly, so the container logic lives in one place.
 */
trait ZipArchiveTestUtils {

  /** Zip extensions to exercise; the head is the default. */
  protected def archiveExtensions: Seq[String] = Seq("zip")

  /** Writes `entries` (name -> bytes) into the zip archive at `dest`. */
  protected def writeArchive(dest: File, entries: Seq[(String, Array[Byte])]): Unit = {
    val out = new ZipArchiveOutputStream(new FileOutputStream(dest))
    try {
      entries.foreach { case (entryName, bytes) =>
        val entry = new ZipArchiveEntry(entryName)
        entry.setSize(bytes.length.toLong)
        out.putArchiveEntry(entry)
        out.write(bytes)
        out.closeArchiveEntry()
      }
      out.finish()
    } finally out.close()
  }

  /**
   * Writes bytes that are not a readable zip archive to `dest`. The leading bytes are not a local
   * file header signature, so `ZipArchiveInputStream` fails when the first entry is read (rather
   * than silently reporting an empty archive).
   */
  protected def writeCorruptArchive(dest: File): Unit =
    Files.write(dest.toPath, "this is not a valid zip archive, just some random bytes"
      .getBytes(StandardCharsets.UTF_8))
}
