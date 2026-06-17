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

import java.io.{Closeable, InputStream}
import java.util.Locale
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

import scala.util.control.NonFatal

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.io.ByteOrderMark
import org.apache.commons.io.input.{BOMInputStream, CloseShieldInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import org.apache.spark.TaskContext
import org.apache.spark.util.HadoopFSUtils

/**
 * Streaming reader for a single archive file. The archive is opened once and decompressed/unpacked
 * as a stream -- entries are never materialized to local disk. [[readEntries]] hands each entry's
 * bytes to a caller-supplied parse function as a bounded [[InputStream]] and concatenates the
 * per-entry results into a single iterator, advancing to the next entry only once the current one
 * is fully consumed. At most one entry is in flight at a time, so memory stays bounded regardless
 * of archive size.
 *
 * This is format-agnostic: a data source whose per-file reader can consume an `InputStream` wires
 * up archive support by calling [[readEntries]] from its read/inference paths and supplying a
 * `parseEntry` that turns one entry stream into rows (or tokens). Formats that need random access
 * within a file (e.g. Parquet/ORC footers) cannot use this streaming path.
 *
 * A concrete subclass implements [[readEntries]] for a specific archive format. Obtain the reader
 * for a path via `ArchiveReader(path)`, which selects the implementation by file extension; new
 * archive formats are added by writing another subclass rather than modifying existing ones.
 */
abstract class ArchiveReader(path: Path) {

  /**
   * Streams the archive entry by entry, applying `parseEntry` to each non-skipped entry's
   * `(name, stream)` and concatenating the results into a single iterator. The next entry is opened
   * only once the current entry's iterator is exhausted, so nothing is buffered to disk and at most
   * one entry's bytes are read at a time. The archive stream is closed when the returned iterator
   * is exhausted, when [[Closeable.close]] is called on it, and (defensively) on task completion.
   * Entry skipping mirrors Spark's file listing: `ignoredPathSegmentRegex` is the effective filter
   * (the `ignoredPathSegmentRegex` data source option), so callers reading with a custom filter must
   * pass its compiled form here for archive entries to be filtered like loose files.
   */
  def readEntries[T](
      conf: Configuration,
      ignoredPathSegmentRegex: Pattern = HadoopFSUtils.defaultIgnoredPathSegmentRegexPattern)(
      parseEntry: (String, InputStream) => Iterator[T]): Iterator[T]
}

object ArchiveReader {

  /**
   * Whether `path` names an archive this reader can stream. Dispatched purely on the file
   * extension -- `.tar`, `.tar.gz`, or `.tgz` -- since the bytes are not inspected here.
   */
  def isArchivePath(path: Path): Boolean = {
    val name = path.getName.toLowerCase(Locale.ROOT)
    name.endsWith(".tar") || name.endsWith(".tar.gz") || name.endsWith(".tgz")
  }

  /**
   * Returns the [[ArchiveReader]] implementation for `path`, selected by its file extension. Only
   * paths for which [[isArchivePath]] is true are supported; new archive formats add a case here.
   */
  def apply(path: Path): ArchiveReader = new TarArchiveReader(path)

  /**
   * Splits one already-decompressed archive entry's bytes into lines. The reusable, format-agnostic
   * line source for archive entries; the entry stream is not closed here (the reader owns the
   * underlying stream).
   *
   * @param in bytes of one archive entry.
   * @param lineSeparatorInRead the explicit read line separator, or `None` to detect line breaks.
   * @return an iterator over the entry's lines as [[Text]], without the trailing separator.
   */
  def lineIterator(in: InputStream, lineSeparatorInRead: Option[Array[Byte]]): Iterator[Text] = {
    // A leading byte-order mark is stripped (LineReader does not strip it on its own) so the lines
    // match the non-archive read path.
    val bomInputStream = BOMInputStream.builder()
      .setInputStream(in)
      .setByteOrderMarks(
        ByteOrderMark.UTF_8,
        ByteOrderMark.UTF_16LE,
        ByteOrderMark.UTF_16BE,
        ByteOrderMark.UTF_32LE,
        ByteOrderMark.UTF_32BE)
      .setInclude(false)
      .get()
    val reader = lineSeparatorInRead match {
      case Some(sep) => new LineReader(bomInputStream, sep)
      case _ => new LineReader(bomInputStream)
    }
    new Iterator[Text] {
      private val text = new Text()
      private var finished = false
      private var hasValue = false

      override def hasNext: Boolean = {
        if (!finished && !hasValue) {
          finished = reader.readLine(text) == 0
          hasValue = !finished
        }
        !finished
      }

      override def next(): Text = {
        if (!hasNext) throw new NoSuchElementException
        hasValue = false
        text
      }
    }
  }
}

/**
 * [[ArchiveReader]] for tar archives: plain `.tar`, gzipped `.tar.gz`, and `.tgz`.
 *
 * Gzip handling: Hadoop's `CompressionCodecFactory` matches the trailing `.gz` extension and
 * auto-decompresses `.tar.gz` via `CodecStreams`, so we just wrap that stream in
 * `TarArchiveInputStream`. `.tgz` is not a registered Hadoop codec extension, so the gzip layer is
 * unwrapped explicitly here.
 */
class TarArchiveReader(path: Path) extends ArchiveReader(path) {

  // Paths Hadoop's codec factory won't auto-decompress: we apply the gzip layer here.
  private def needsExplicitGunzip: Boolean =
    path.getName.toLowerCase(Locale.ROOT).endsWith(".tgz")

  /**
   * Whether an entry is not a real data file and must be skipped: a directory, or a name Spark's
   * own file listing would filter out. Applying [[HadoopFSUtils.shouldFilterOutPathName]] (the
   * `InMemoryFileIndex` filter) with the same effective `ignoredPathSegmentRegex` to every path
   * component keeps archive reads in parity with reading the same entries as loose files,
   * including when the user supplies a custom `ignoredPathSegmentRegex` option: under the default
   * filter, `.`-prefixed sidecars (macOS `._x`, `.DS_Store`), `_`-prefixed markers (`_SUCCESS`,
   * `_committed_*`), and anything under a `.`/`_`-prefixed directory (e.g. a leftover
   * `_temporary/` from a failed write) are skipped, while data files are kept. The per-component
   * check matters because `InMemoryFileIndex` never recurses into such directories, so a
   * basename-only filter would read `_temporary/part-0.csv` that a loose-file scan drops.
   */
  private def shouldSkipEntry(entry: TarArchiveEntry, ignoredPathSegmentRegex: Pattern): Boolean = {
    if (entry.isDirectory) return true
    entry.getName.split("/").exists(c =>
      c.nonEmpty && HadoopFSUtils.shouldFilterOutPathName(c, ignoredPathSegmentRegex))
  }

  /** Opens the archive as a tar stream, transparently decompressing `.tar.gz` / `.tgz`. */
  private def openTarStream(conf: Configuration): TarArchiveInputStream = {
    val base = CodecStreams.createInputStreamWithCloseResource(conf, path)
    val tarBytes = if (needsExplicitGunzip) new GZIPInputStream(base) else base
    new TarArchiveInputStream(tarBytes)
  }

  /**
   * Wraps the shared tar stream as a view over exactly the current entry's bytes
   * (`TarArchiveInputStream.read` returns -1 at the entry boundary). [[CloseShieldInputStream]]
   * ignores `close()`, so a parser closing its input does not close the underlying archive; any
   * unread remainder of an entry is skipped by `getNextEntry()` when advancing.
   */
  private def entryStream(tar: TarArchiveInputStream): InputStream =
    CloseShieldInputStream.wrap(tar)

  override def readEntries[T](
      conf: Configuration,
      ignoredPathSegmentRegex: Pattern)(
      parseEntry: (String, InputStream) => Iterator[T]): Iterator[T] = {
    val tar = openTarStream(conf)
    var closed = false

    def cleanup(): Unit = {
      if (!closed) {
        closed = true
        try tar.close() catch { case NonFatal(_) => }
      }
    }

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => cleanup()))

    new Iterator[T] with Closeable {
      private var currentIter: Iterator[T] = Iterator.empty
      private var done = false

      // Move to the next entry whose iterator has elements (releasing each exhausted entry's
      // reader and skipping any unread bytes), or mark the stream done once entries run out.
      // Advancing here -- driven by `hasNext` -- rather than eagerly after producing a row in
      // `next` is essential for parsers that reuse a single mutable row and look ahead on
      // `hasNext`: probing the current entry right after returning a row would overwrite that row's
      // contents before the caller has copied it.
      private def advance(): Unit = {
        while (!done && !currentIter.hasNext) {
          currentIter match {
            case c: Closeable => try c.close() catch { case NonFatal(_) => }
            case _ =>
          }
          var entry = tar.getNextEntry
          while (entry != null && shouldSkipEntry(entry, ignoredPathSegmentRegex)) {
            entry = tar.getNextEntry
          }
          if (entry == null) {
            done = true
            cleanup()
          } else {
            currentIter = parseEntry(entry.getName, entryStream(tar))
          }
        }
      }

      // Open the first entry eagerly so construction reflects the archive's first entry.
      advance()

      override def hasNext: Boolean = {
        advance()
        !done && currentIter.hasNext
      }

      override def next(): T = {
        if (!hasNext) throw new NoSuchElementException
        currentIter.next()
      }

      override def close(): Unit = {
        done = true
        currentIter = Iterator.empty
        cleanup()
      }
    }
  }
}
