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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.commons.compress.archivers.sevenz.{SevenZArchiveEntry, SevenZOutputFile}

/**
 * Binds [[ArchiveReadSuiteBase]]'s archive-format hooks to 7z containers (`.7z`). The
 * container-writing helpers live in [[SevenZArchiveTestUtils]] (shared with standalone suites that
 * cannot extend `ArchiveReadSuiteBase`). Reusable across file formats -- a
 * `<format>SevenZArchiveReadSuite` mixes this in alongside the file-format trait.
 */
trait SevenZArchiveReadBase extends ArchiveReadSuiteBase with SevenZArchiveTestUtils {

  override protected def corruptArchiveExtension: String = "7z"
}

/**
 * 7z container-writing helpers (`.7z`), independent of any read-test harness -- the 7z peer of
 * [[TarArchiveTestUtils]] and [[ZipArchiveTestUtils]]. [[SevenZArchiveReadBase]] mixes this into
 * [[ArchiveReadSuiteBase]] for the format-agnostic suites, and standalone suites that cannot extend
 * `ArchiveReadSuiteBase` (e.g. `TextSevenZArchiveReadSuite`) mix it in directly, so the container
 * logic lives in one place.
 */
trait SevenZArchiveTestUtils {

  /** 7z extensions to exercise; the head is the default. */
  protected def archiveExtensions: Seq[String] = Seq("7z")

  /** Writes `entries` (name -> bytes) into the 7z archive at `dest` (default LZMA2 compression). */
  protected def writeArchive(dest: File, entries: Seq[(String, Array[Byte])]): Unit = {
    val out = new SevenZOutputFile(dest)
    try {
      entries.foreach { case (entryName, bytes) =>
        val entry = new SevenZArchiveEntry()
        entry.setName(entryName)
        out.putArchiveEntry(entry)
        out.write(bytes)
        out.closeArchiveEntry()
      }
      out.finish()
    } finally out.close()
  }

  /**
   * Writes bytes that are not a readable 7z archive to `dest`. The leading bytes are not the 7z
   * signature, so `SevenZFile` fails when the archive is opened (rather than reporting it empty).
   */
  protected def writeCorruptArchive(dest: File): Unit =
    Files.write(dest.toPath, "this is not a valid 7z archive, just some random bytes"
      .getBytes(StandardCharsets.UTF_8))
}
