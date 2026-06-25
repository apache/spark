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
import java.util.Properties
import java.util.regex.Pattern
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkFunSuite, TaskContext, TaskContextImpl}

/**
 * Unit tests for the streaming [[ArchiveReader]] core: `isArchivePath` dispatch and `readEntries`
 * (entry ordering, gzip handling, dir/dotfile skipping, lazy advance, the non-closing entry
 * stream, and cleanup). Nothing here touches local disk -- entries are consumed as streams.
 */
class ArchiveReaderSuite extends SparkFunSuite {

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

  /** Drains every entry into `(name, decodedText)` pairs through `ArchiveReader.readEntries`. */
  private def collect(file: File): Seq[(String, String)] =
    ArchiveReader(new Path(file.toURI)).readEntries(new Configuration()) { (name, in) =>
      Iterator.single((name, new String(readAll(in), StandardCharsets.UTF_8)))
    }.toList

  // ----- isArchivePath ------------------------------------------------------

  test("isArchivePath: positive cases") {
    Seq(
      "foo.tar", "FOO.TAR", "/a/b/c/x.tar", "weird.TaR",
      "foo.tar.gz", "FOO.TAR.GZ", "mixed.Tar.Gz", "/a/b/c/x.tar.gz",
      "foo.tgz", "FOO.TGZ", "/a/b/c/x.tgz"
    ).foreach { p =>
      assert(ArchiveReader.isArchivePath(new Path(p)), s"expected archive match for $p")
    }
  }

  test("isArchivePath: negative cases") {
    Seq("foo.csv", "foo.gz", "foo", "dir/", "foo.tarball", "data.zip",
        "foo.tar.bz2", "foo.targz").foreach { p =>
      assert(!ArchiveReader.isArchivePath(new Path(p)), s"expected non-match for $p")
    }
  }

  // ----- readEntries --------------------------------------------------------

  test("readEntries: empty tar yields empty iterator") {
    withTempDir { dir =>
      val tar = new File(dir, "empty.tar")
      writeTar(tar, Seq.empty)
      assert(collect(tar).isEmpty)
    }
  }

  test("readEntries: single entry exposes its name and bytes") {
    withTempDir { dir =>
      val tar = new File(dir, "single.tar")
      writeTar(tar, Seq(textEntry("only.csv", "hello\n")))
      assert(collect(tar) == Seq("only.csv" -> "hello\n"))
    }
  }

  test("readEntries: multiple entries chained in tar order") {
    withTempDir { dir =>
      val tar = new File(dir, "multi.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))
      assert(collect(tar) == Seq("a.csv" -> "a", "b.csv" -> "b", "c.csv" -> "c"))
    }
  }

  test("readEntries: gzipped tar (.tar.gz) via Hadoop codec factory") {
    withTempDir { dir =>
      val tarGz = new File(dir, "data.tar.gz")
      writeTarGz(tarGz, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))
      assert(collect(tarGz) == Seq("a.csv" -> "a", "b.csv" -> "b"))
    }
  }

  test("readEntries: gzipped tar (.tgz) via explicit GZIPInputStream wrap") {
    withTempDir { dir =>
      val tgz = new File(dir, "data.tgz")
      writeTarGz(tgz, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))
      assert(collect(tgz) == Seq("a.csv" -> "a", "b.csv" -> "b"))
    }
  }

  test("readEntries: directory entries are skipped") {
    withTempDir { dir =>
      val tar = new File(dir, "dirs.tar")
      writeTar(tar, Seq(
        Entry("subdir", Array.emptyByteArray, isDir = true),
        textEntry("subdir/data.csv", "x")))
      assert(collect(tar) == Seq("subdir/data.csv" -> "x"))
    }
  }

  test("readEntries: dotfile, underscore-marker, and prefixed-dir entries are skipped") {
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

  test("readEntries: a custom ignoredPathSegmentRegex pattern surfaces hidden entries") {
    withTempDir { dir =>
      val tar = new File(dir, "custom-filter.tar")
      writeTar(tar, Seq(textEntry("_SUCCESS", "marker"), textEntry("real.csv", "kept")))
      // "(?!)" matches nothing, so hidden-name filtering is disabled and only the carve-outs in
      // HadoopFSUtils.shouldFilterOutPathName still apply -- mirroring a loose-file listing with
      // the ignoredPathSegmentRegex option set to the same regex.
      val entries = ArchiveReader(new Path(tar.toURI))
        .readEntries(new Configuration(), Pattern.compile("(?!)")) { (name, in) =>
          Iterator.single((name, new String(readAll(in), StandardCharsets.UTF_8)))
        }.toList
      assert(entries == Seq("_SUCCESS" -> "marker", "real.csv" -> "kept"))
    }
  }

  test("readEntries: advances lazily, one entry at a time") {
    withTempDir { dir =>
      val tar = new File(dir, "lazy.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b"), textEntry("c.csv", "c")))

      val opened = ArrayBuffer[String]()
      // parseEntry yields a single element without reading the stream, so each invocation maps to
      // exactly one consumed output element -- letting us observe when the next entry is opened.
      val it = ArchiveReader(new Path(tar.toURI)).readEntries(new Configuration()) { (name, _) =>
        opened += name
        Iterator.single(name)
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

  test("readEntries: a parseEntry that closes its stream still advances to the next entry") {
    withTempDir { dir =>
      val tar = new File(dir, "close.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))

      val seen = ArrayBuffer[String]()
      val it = ArchiveReader(new Path(tar.toURI)).readEntries(new Configuration()) { (name, in) =>
        val body = new String(readAll(in), StandardCharsets.UTF_8)
        in.close() // must NOT close the underlying archive
        seen += body
        Iterator.single(name)
      }
      assert(it.toList == List("a.csv", "b.csv"))
      assert(seen.toList == List("a", "b"))
    }
  }

  test("readEntries: close() is safe, idempotent, and stops iteration") {
    withTempDir { dir =>
      val tar = new File(dir, "closeable.tar")
      writeTar(tar, Seq(textEntry("a.csv", "a"), textEntry("b.csv", "b")))

      val it = ArchiveReader(new Path(tar.toURI)).readEntries(new Configuration()) { (name, _) =>
        Iterator.single(name)
      }
      assert(it.hasNext)
      it.asInstanceOf[Closeable].close()
      it.asInstanceOf[Closeable].close() // idempotent
      assert(!it.hasNext)
    }
  }

  test("readEntries: TaskContext completion cleans up without error") {
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
        cpus = 1)
      TaskContext.setTaskContext(ctx)
      try {
        val it = ArchiveReader(new Path(tar.toURI)).readEntries(new Configuration()) { (name, _) =>
          Iterator.single(name)
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
}
