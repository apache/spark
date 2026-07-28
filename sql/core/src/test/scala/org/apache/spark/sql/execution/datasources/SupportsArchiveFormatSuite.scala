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

import java.io.{ByteArrayOutputStream, Closeable, File, FileOutputStream, InputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Properties
import java.util.regex.Pattern
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.compress.archivers.sevenz.{SevenZArchiveEntry, SevenZOutputFile}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkFunSuite, SparkRuntimeException, TaskContext, TaskContextImpl}

/**
 * Unit tests for the streaming [[SupportsArchiveFormat]] engine: `isArchivePath` dispatch and
 * `readArchiveEntries` (entry ordering, gzip handling, dir/dotfile skipping, lazy advance, the
 * non-closing entry stream, and cleanup). Nothing here touches local disk -- entries are streams.
 */
class SupportsArchiveFormatSuite extends SparkFunSuite {

  private case class Entry(name: String, data: Array[Byte], isDir: Boolean = false)

  private def writeTar(file: File, entries: Seq[Entry]): Unit =
    writeTarTo(new FileOutputStream(file), entries)

  /** Write a gzipped tar, used to verify the `.tar.gz` / `.tgz` archive paths. */
  private def writeTarGz(file: File, entries: Seq[Entry]): Unit =
    writeTarTo(new GZIPOutputStream(new FileOutputStream(file)), entries)

  private def writeTarTo(rawOut: OutputStream, entries: Seq[Entry]): Unit = {
    val out = new TarArchiveOutputStream(rawOut)
    try {
      entries.foreach { e =>
        // TarArchiveEntry treats a trailing slash in the name as a directory marker.
        val rawName = if (e.isDir && !e.name.endsWith("/")) e.name + "/" else e.name
        val tarEntry = new TarArchiveEntry(rawName)
        if (!e.isDir) tarEntry.setSize(e.data.length.toLong)
        out.putArchiveEntry(tarEntry)
        if (!e.isDir) out.write(e.data)
        out.closeArchiveEntry()
      }
      out.finish()
    } finally out.close()
  }

  /** Write a zip archive, used to verify the `.zip` archive path. */
  private def writeZip(file: File, entries: Seq[Entry]): Unit = {
    val out = new ZipArchiveOutputStream(new FileOutputStream(file))
    try {
      entries.foreach { e =>
        // A trailing slash marks a directory entry.
        val rawName = if (e.isDir && !e.name.endsWith("/")) e.name + "/" else e.name
        val zipEntry = new ZipArchiveEntry(rawName)
        if (!e.isDir) zipEntry.setSize(e.data.length.toLong)
        out.putArchiveEntry(zipEntry)
        if (!e.isDir) out.write(e.data)
        out.closeArchiveEntry()
      }
      out.finish()
    } finally out.close()
  }

  /** Appends `v` as an unsigned 2-byte little-endian integer -- one of ZIP's header fields. */
  private def u16(out: ByteArrayOutputStream, v: Int): Unit = {
    out.write(v & 0xFF); out.write((v >>> 8) & 0xFF)
  }

  /** Appends `v` as an unsigned 4-byte little-endian integer -- one of ZIP's header fields. */
  private def u32(out: ByteArrayOutputStream, v: Long): Unit = {
    out.write((v & 0xFF).toInt); out.write(((v >>> 8) & 0xFF).toInt)
    out.write(((v >>> 16) & 0xFF).toInt); out.write(((v >>> 24) & 0xFF).toInt)
  }

  /**
   * Writes a zip with one STORED (uncompressed) entry that uses a data descriptor: general-purpose
   * bit 3 is set and the local header's crc/size fields are zeroed, so the real values live only in
   * the trailing data descriptor.
   */
  private def writeStoredEntryWithDataDescriptor(file: File, name: String, body: String): Unit = {
    val nameBytes = name.getBytes(StandardCharsets.UTF_8)
    val data = body.getBytes(StandardCharsets.UTF_8)
    val crc = { val c = new java.util.zip.CRC32(); c.update(data); c.getValue }
    val out = new ByteArrayOutputStream()
    def u16(v: Int): Unit = this.u16(out, v)
    def u32(v: Long): Unit = this.u32(out, v)
    // Local file header: GP bit 3 set (data descriptor), STORED method, sizes zeroed here.
    val localHeaderOffset = out.size()
    u32(0x04034b50L); u16(10); u16(0x0008); u16(0); u16(0); u16(0)
    u32(0); u32(0); u32(0)
    u16(nameBytes.length); u16(0)
    out.write(nameBytes); out.write(data)
    // Data descriptor (with optional signature): the real crc and sizes.
    u32(0x08074b50L); u32(crc); u32(data.length.toLong); u32(data.length.toLong)
    // Central directory.
    val cdOffset = out.size()
    u32(0x02014b50L); u16(20); u16(10); u16(0x0008); u16(0); u16(0); u16(0)
    u32(crc); u32(data.length.toLong); u32(data.length.toLong)
    u16(nameBytes.length); u16(0); u16(0); u16(0); u16(0); u32(0); u32(localHeaderOffset.toLong)
    out.write(nameBytes)
    val cdSize = out.size() - cdOffset
    // End of central directory.
    u32(0x06054b50L); u16(0); u16(0); u16(1); u16(1)
    u32(cdSize.toLong); u32(cdOffset.toLong); u16(0)
    val fos = new FileOutputStream(file)
    try {
      fos.write(out.toByteArray)
    } finally {
      fos.close()
    }
  }

  /**
   * Writes a zip whose single STORED entry sets the encryption flag (general-purpose bit 0), so
   * `ZipFile#canReadEntryData` returns false. Assembled by hand: no writer we depend on emits an
   * encryption-flagged entry (commons-compress's `ZipArchiveOutputStream` cannot). The bytes are
   * not actually encrypted -- only the flag is set, which alone drives the read path under test.
   * ZIP is a sequence of little-endian records, each led by a 4-byte signature.
   */
  private def writeEncryptedEntry(file: File, name: String, body: String): Unit = {
    val nameBytes = name.getBytes(StandardCharsets.UTF_8)
    val data = body.getBytes(StandardCharsets.UTF_8)
    val crc = { val c = new java.util.zip.CRC32(); c.update(data); c.getValue }
    val out = new ByteArrayOutputStream()
    def u16(v: Int): Unit = this.u16(out, v)
    def u32(v: Long): Unit = this.u32(out, v)
    val size = data.length.toLong
    // Local file header (sig 0x04034b50): version, GP flags = 0x0001 (bit 0 = encrypted), STORED
    // method (0), mod time/date, crc, compressed & uncompressed sizes, name & extra lengths.
    val localHeaderOffset = out.size()
    u32(0x04034b50L); u16(10); u16(0x0001); u16(0); u16(0); u16(0)
    u32(crc); u32(size); u32(size)
    u16(nameBytes.length); u16(0)
    out.write(nameBytes); out.write(data)
    // Central directory record (sig 0x02014b50): mirrors the header, same 0x0001 encrypted flag,
    // and points back at the local header offset.
    val cdOffset = out.size()
    u32(0x02014b50L); u16(20); u16(10); u16(0x0001); u16(0); u16(0); u16(0)
    u32(crc); u32(size); u32(size)
    u16(nameBytes.length); u16(0); u16(0); u16(0); u16(0); u32(0); u32(localHeaderOffset.toLong)
    out.write(nameBytes)
    val cdSize = out.size() - cdOffset
    // End of central directory (sig 0x06054b50): 1 entry, directory size and offset.
    u32(0x06054b50L); u16(0); u16(0); u16(1); u16(1)
    u32(cdSize.toLong); u32(cdOffset.toLong); u16(0)
    val fos = new FileOutputStream(file)
    try {
      fos.write(out.toByteArray)
    } finally {
      fos.close()
    }
  }

  /** Write a 7z archive, used to verify the `.7z` archive path. */
  private def writeSevenZ(file: File, entries: Seq[Entry]): Unit = {
    val out = new SevenZOutputFile(file)
    try {
      entries.foreach { e =>
        // 7z records an explicit directory flag rather than inferring it from a trailing slash.
        val sevenZEntry = new SevenZArchiveEntry()
        sevenZEntry.setName(e.name)
        sevenZEntry.setDirectory(e.isDir)
        sevenZEntry.setHasStream(!e.isDir)
        out.putArchiveEntry(sevenZEntry)
        if (!e.isDir) out.write(e.data)
        out.closeArchiveEntry()
      }
      out.finish()
    } finally out.close()
  }

  private def textEntry(name: String, body: String): Entry =
    Entry(name, body.getBytes(StandardCharsets.UTF_8))

  private def readAll(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val buf = new Array[Byte](4096)
    var n = in.read(buf)
    while (n >= 0) {
      out.write(buf, 0, n)
      n = in.read(buf)
    }
    out.toByteArray
  }

  /** Drains every entry into `(name, decodedText)` pairs through `SupportsArchiveFormat`. */
  private def collect(file: File): Seq[(String, String)] =
    SupportsArchiveFormat.readArchiveEntries(new Path(file.toURI), new Configuration()) {
      (entry, in) =>
        Iterator.single((entry.getName, new String(readAll(in), StandardCharsets.UTF_8)))
    }.toList

  // ----- isArchivePath ------------------------------------------------------

  test("isArchivePath: positive cases") {
    Seq(
      "foo.tar", "FOO.TAR", "/a/b/c/x.tar", "weird.TaR",
      "foo.tar.gz", "FOO.TAR.GZ", "mixed.Tar.Gz", "/a/b/c/x.tar.gz",
      "foo.tgz", "FOO.TGZ", "/a/b/c/x.tgz",
      "data.zip", "FOO.ZIP", "weird.ZiP", "/a/b/c/x.zip"
    ).foreach { p =>
      assert(SupportsArchiveFormat.isArchivePath(new Path(p)), s"expected archive match for $p")
    }
  }

  test("isArchivePath: negative cases") {
    Seq("foo.csv", "foo.gz", "foo", "dir/", "foo.tarball",
        "foo.tar.bz2", "foo.targz", "foo.zipx", "foo.gzip").foreach { p =>
      assert(!SupportsArchiveFormat.isArchivePath(new Path(p)), s"expected non-match for $p")
    }
  }

  // ----- readArchiveEntries --------------------------------------------------------

  test("readArchiveEntries: empty tar yields empty iterator") {
    withTempDir { dir =>
      val tar = new File(dir, "empty.tar")
      writeTar(tar, Seq.empty)
      assert(collect(tar).isEmpty)
    }
  }

  test("readArchiveEntries: single entry exposes its name and bytes") {
    withTempDir { dir =>
      val tar = new File(dir, "single.tar")
      writeTar(tar, Seq(textEntry("only.csv", "hello\n")))
      assert(collect(tar) == Seq("only.csv" -> "hello\n"))
    }
  }

  test("readArchiveEntries: multiple entries chained in tar order") {
    withTempDir { dir =>
      val tar = new File(dir, "multi.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))
      assert(collect(tar) == Seq("a.csv" -> "a", "b.csv" -> "b", "c.csv" -> "c"))
    }
  }

  test("readArchiveEntries: gzipped tar (.tar.gz) via Hadoop codec factory") {
    withTempDir { dir =>
      val tarGz = new File(dir, "data.tar.gz")
      writeTarGz(tarGz, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))
      assert(collect(tarGz) == Seq("a.csv" -> "a", "b.csv" -> "b"))
    }
  }

  test("readArchiveEntries: gzipped tar (.tgz) via explicit GZIPInputStream wrap") {
    withTempDir { dir =>
      val tgz = new File(dir, "data.tgz")
      writeTarGz(tgz, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))
      assert(collect(tgz) == Seq("a.csv" -> "a", "b.csv" -> "b"))
    }
  }

  test("readArchiveEntries: directory entries are skipped") {
    withTempDir { dir =>
      val tar = new File(dir, "dirs.tar")
      writeTar(tar, Seq(
        Entry("subdir", Array.emptyByteArray, isDir = true),
        textEntry("subdir/data.csv", "x")))
      assert(collect(tar) == Seq("subdir/data.csv" -> "x"))
    }
  }

  test("readArchiveEntries: dotfile, underscore-marker, and prefixed-dir entries are skipped") {
    withTempDir { dir =>
      val tar = new File(dir, "skipped.tar")
      writeTar(tar, Seq(
        textEntry("._real.csv", "junk"),           // macOS AppleDouble sidecar
        textEntry(".hidden", "ignored"),           // bare dotfile
        textEntry("_SUCCESS", "marker"),           // _-prefixed marker (InMemoryFileIndex skips it)
        textEntry("_temporary/part-0.csv", "tmp"), // entry under a _-prefixed dir (skipped whole)
        textEntry("real.csv", "kept"),
        textEntry("nested/._sidecar", "junk2")))   // dotfile in a subdir
      assert(collect(tar) == Seq("real.csv" -> "kept"))
    }
  }

  test("readArchiveEntries: a custom ignoredPathSegmentRegex pattern surfaces hidden entries") {
    withTempDir { dir =>
      val tar = new File(dir, "custom-filter.tar")
      writeTar(tar, Seq(textEntry("_SUCCESS", "marker"), textEntry("real.csv", "kept")))
      // "(?!)" matches nothing, so hidden-name filtering is disabled and only the carve-outs in
      // HadoopFSUtils.shouldFilterOutPathName still apply -- mirroring a loose-file listing with
      // the ignoredPathSegmentRegex option set to the same regex.
      val entries = SupportsArchiveFormat.readArchiveEntries(
        new Path(tar.toURI), new Configuration(), Pattern.compile("(?!)")) { (entry, in) =>
        Iterator.single((entry.getName, new String(readAll(in), StandardCharsets.UTF_8)))
      }.toList
      assert(entries == Seq("_SUCCESS" -> "marker", "real.csv" -> "kept"))
    }
  }

  test("readArchiveEntries: advances lazily, one entry at a time") {
    withTempDir { dir =>
      val tar = new File(dir, "lazy.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))

      val opened = ArrayBuffer[String]()
      // parseEntry yields a single element without reading the stream, so each invocation maps to
      // exactly one consumed output element -- letting us observe when the next entry is opened.
      val it = SupportsArchiveFormat.readArchiveEntries(new Path(tar.toURI), new Configuration()) {
        (entry, _) =>
          opened += entry.getName
          Iterator.single(entry.getName)
      }

      // Construction opens only the first entry; later entries open on demand as iteration
      // crosses each entry boundary (never all upfront).
      assert(opened.toList == List("a.csv"))
      assert(it.hasNext)
      assert(it.next() == "a.csv")
      // Entry 0 is still in flight until its element is consumed, so entry 1 stays unopened.
      assert(opened.toList == List("a.csv"))
      assert(it.next() == "b.csv")
      assert(opened.toList == List("a.csv", "b.csv"))
      assert(it.next() == "c.csv")
      assert(opened.toList == List("a.csv", "b.csv", "c.csv"))
      assert(!it.hasNext)
      assert(opened.size == 3)
    }
  }

  test("readArchiveEntries: a parseEntry that closes its stream still advances") {
    withTempDir { dir =>
      val tar = new File(dir, "close.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))

      val seen = ArrayBuffer[String]()
      val it = SupportsArchiveFormat.readArchiveEntries(new Path(tar.toURI), new Configuration()) {
        (entry, in) =>
          val body = new String(readAll(in), StandardCharsets.UTF_8)
          in.close() // must NOT close the underlying archive
          seen += body
          Iterator.single(entry.getName)
      }
      assert(it.toList == List("a.csv", "b.csv"))
      assert(seen.toList == List("a", "b"))
    }
  }

  test("readArchiveEntries: close() is safe, idempotent, and stops iteration") {
    withTempDir { dir =>
      val tar = new File(dir, "closeable.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))

      val it = SupportsArchiveFormat.readArchiveEntries(new Path(tar.toURI), new Configuration()) {
        (entry, _) => Iterator.single(entry.getName)
      }
      assert(it.hasNext)
      it.asInstanceOf[Closeable].close()
      it.asInstanceOf[Closeable].close() // idempotent
      assert(!it.hasNext)
    }
  }

  test("readArchiveEntries: TaskContext completion cleans up without error") {
    withTempDir { dir =>
      val tar = new File(dir, "ctx.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))

      val ctx = new TaskContextImpl(
        stageId = 0,
        stageAttemptNumber = 0,
        partitionId = 0,
        taskAttemptId = 1L,
        attemptNumber = 0,
        numPartitions = 0,
        taskMemoryManager = null,
        localProperties = new Properties,
        metricsSystem = null,
        cpuAmount = 1)
      TaskContext.setTaskContext(ctx)
      try {
        val it = SupportsArchiveFormat.readArchiveEntries(
          new Path(tar.toURI), new Configuration()) {
          (entry, _) => Iterator.single(entry.getName)
        }
        assert(it.hasNext)
        it.next() // open the archive and register the completion listener
        // Simulate task completion without exhausting/closing the iterator.
        ctx.markTaskCompleted(None)
      } finally {
        TaskContext.unset()
      }
    }
  }

  // ----- zip ----------------------------------------------------------------

  test("readArchiveEntries: empty zip yields empty iterator") {
    withTempDir { dir =>
      val zip = new File(dir, "empty.zip")
      writeZip(zip, Seq.empty)
      assert(collect(zip).isEmpty)
    }
  }

  test("readArchiveEntries: zip single entry exposes its name and bytes") {
    withTempDir { dir =>
      val zip = new File(dir, "single.zip")
      writeZip(zip, Seq(textEntry("only.csv", "hello\n")))
      assert(collect(zip) == Seq("only.csv" -> "hello\n"))
    }
  }

  test("readArchiveEntries: zip multiple entries chained in archive order") {
    withTempDir { dir =>
      val zip = new File(dir, "multi.zip")
      writeZip(zip, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))
      assert(collect(zip) == Seq("a.csv" -> "a", "b.csv" -> "b", "c.csv" -> "c"))
    }
  }

  test("readArchiveEntries: zip directory entries are skipped") {
    withTempDir { dir =>
      val zip = new File(dir, "dirs.zip")
      writeZip(zip, Seq(
        Entry("subdir", Array.emptyByteArray, isDir = true),
        textEntry("subdir/data.csv", "x")))
      assert(collect(zip) == Seq("subdir/data.csv" -> "x"))
    }
  }

  test("readArchiveEntries: zip dotfile, underscore-marker, and prefixed-dir entries are skipped") {
    withTempDir { dir =>
      val zip = new File(dir, "skipped.zip")
      writeZip(zip, Seq(
        textEntry("._real.csv", "junk"),           // macOS AppleDouble sidecar
        textEntry(".hidden", "ignored"),           // bare dotfile
        textEntry("_SUCCESS", "marker"),           // _-prefixed marker (InMemoryFileIndex skips it)
        textEntry("_temporary/part-0.csv", "tmp"), // entry under a _-prefixed dir (skipped whole)
        textEntry("real.csv", "kept"),
        textEntry("nested/._sidecar", "junk2")))   // dotfile in a subdir
      assert(collect(zip) == Seq("real.csv" -> "kept"))
    }
  }

  test("readArchiveEntries: zip advances lazily, one entry at a time") {
    withTempDir { dir =>
      val zip = new File(dir, "lazy.zip")
      writeZip(zip, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))

      val opened = ArrayBuffer[String]()
      val it = SupportsArchiveFormat.readArchiveEntries(new Path(zip.toURI), new Configuration()) {
        (entry, _) =>
          opened += entry.getName
          Iterator.single(entry.getName)
      }
      // Construction opens only the first entry; advancing past each boundary opens the next.
      assert(opened.toList == List("a.csv"))
      assert(it.hasNext)
      assert(it.next() == "a.csv")
      assert(opened.toList == List("a.csv"))
      assert(it.next() == "b.csv")
      assert(opened.toList == List("a.csv", "b.csv"))
      assert(it.next() == "c.csv")
      assert(opened.toList == List("a.csv", "b.csv", "c.csv"))
      assert(!it.hasNext)
      assert(opened.size == 3)
    }
  }

  test("readArchiveEntries: a zip parseEntry that closes its stream still advances") {
    withTempDir { dir =>
      val zip = new File(dir, "close.zip")
      writeZip(zip, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))

      val seen = ArrayBuffer[String]()
      val it = SupportsArchiveFormat.readArchiveEntries(new Path(zip.toURI), new Configuration()) {
        (entry, in) =>
          val body = new String(readAll(in), StandardCharsets.UTF_8)
          in.close() // must NOT close the underlying archive
          seen += body
          Iterator.single(entry.getName)
      }
      assert(it.toList == List("a.csv", "b.csv"))
      assert(seen.toList == List("a", "b"))
    }
  }

  test("readArchiveEntries: a STORED zip entry sized only by a data descriptor reads correctly") {
    withTempDir { dir =>
      val zip = new File(dir, "stored-dd.zip")
      writeStoredEntryWithDataDescriptor(zip, "a.csv", "hello")
      assert(collect(zip) == Seq("a.csv" -> "hello"))
    }
  }

  test("readArchiveEntries: corrupt zip bytes fail loudly") {
    withTempDir { dir =>
      val zip = new File(dir, "corrupt.zip")
      Files.write(zip.toPath,
        "this is not a valid zip archive, just some random bytes"
          .getBytes(StandardCharsets.UTF_8))
      // Not a ZIP (no end-of-central-directory signature), so ZipFile construction fails fast
      // rather than reporting an empty archive. The ZipException surfaces wrapped in IOException.
      val ex = intercept[java.io.IOException](collect(zip))
      assert(ex.getCause.isInstanceOf[java.util.zip.ZipException])
      assert(ex.getCause.getMessage.contains("Archive is not a ZIP archive"))
    }
  }

  test("readArchiveEntries: zip with duplicate entry names yields both entries") {
    withTempDir { dir =>
      val zip = new File(dir, "dupes.zip")
      writeZip(zip, Seq(textEntry("a.csv", "first"), textEntry("a.csv", "second")))
      // ZipFile reads every central-directory record, so both duplicate-named entries surface.
      assert(collect(zip) == Seq("a.csv" -> "first", "a.csv" -> "second"))
    }
  }

  test("readArchiveEntries: encrypted zip entry fails with a clear unsupported error") {
    withTempDir { dir =>
      val zip = new File(dir, "encrypted.zip")
      writeEncryptedEntry(zip, "a.csv", "hello")
      val ex = intercept[SparkRuntimeException](collect(zip))
      checkError(
        exception = ex,
        condition = "CANNOT_READ_ZIP_ENTRY",
        parameters = Map("entry" -> "a.csv", "path" -> new Path(zip.toURI).toString))
    }
  }

  test("readArchiveEntries: an encrypted zip entry that is skipped does not throw") {
    withTempDir { dir =>
      val zip = new File(dir, "encrypted-dotfile.zip")
      // The encrypted entry has a dotfile name the engine filters out, so it is never read; its
      // readability must not be checked (else it would throw CANNOT_READ_ZIP_ENTRY on a skip).
      writeEncryptedEntry(zip, "._skipped.csv", "secret")
      assert(collect(zip).isEmpty)
    }
  }

  // ----- 7z -----------------------------------------------------------------
  // 7z keeps its entry index at the end of the file, so the reader seeks rather than streaming
  // forward like tar and zip. These cases confirm the `.7z` dispatch and that the seek-based
  // container surfaces entries through the same engine.

  test("readArchiveEntries: empty 7z yields empty iterator") {
    withTempDir { dir =>
      val sevenZ = new File(dir, "empty.7z")
      writeSevenZ(sevenZ, Seq.empty)
      assert(collect(sevenZ).isEmpty)
    }
  }

  test("readArchiveEntries: 7z single entry exposes its name and bytes") {
    withTempDir { dir =>
      val sevenZ = new File(dir, "single.7z")
      writeSevenZ(sevenZ, Seq(textEntry("only.csv", "hello\n")))
      assert(collect(sevenZ) == Seq("only.csv" -> "hello\n"))
    }
  }

  test("readArchiveEntries: 7z multiple entries chained in archive order") {
    withTempDir { dir =>
      val sevenZ = new File(dir, "multi.7z")
      writeSevenZ(sevenZ,
        Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))
      assert(collect(sevenZ) == Seq("a.csv" -> "a", "b.csv" -> "b", "c.csv" -> "c"))
    }
  }

  test("readArchiveEntries: 7z directory entries are skipped") {
    withTempDir { dir =>
      val sevenZ = new File(dir, "dirs.7z")
      writeSevenZ(sevenZ, Seq(
        Entry("subdir", Array.emptyByteArray, isDir = true),
        textEntry("subdir/data.csv", "x")))
      assert(collect(sevenZ) == Seq("subdir/data.csv" -> "x"))
    }
  }

  test("readArchiveEntries: 7z dotfile, underscore-marker, and prefixed-dir entries are skipped") {
    withTempDir { dir =>
      val sevenZ = new File(dir, "skipped.7z")
      writeSevenZ(sevenZ, Seq(
        textEntry("._real.csv", "junk"),           // macOS AppleDouble sidecar
        textEntry(".hidden", "ignored"),           // bare dotfile
        textEntry("_SUCCESS", "marker"),           // _-prefixed marker (InMemoryFileIndex skips it)
        textEntry("_temporary/part-0.csv", "tmp"), // entry under a _-prefixed dir (skipped whole)
        textEntry("real.csv", "kept"),
        textEntry("nested/._sidecar", "junk2")))   // dotfile in a subdir
      assert(collect(sevenZ) == Seq("real.csv" -> "kept"))
    }
  }
}
