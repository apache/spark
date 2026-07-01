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

import java.io.{Closeable, File, FileNotFoundException, FileOutputStream, InputStream, IOException}
import java.util.Locale
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

import scala.util.control.NonFatal

import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.io.ByteOrderMark
import org.apache.commons.io.input.{BOMInputStream, CloseShieldInputStream}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{HadoopFSUtils, Utils}

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
 * The entry-streaming engine ([[readEntries]]) is shared across archive formats -- tar and zip
 * differ only in how the container stream is opened, and both use commons-compress
 * `ArchiveInputStream`. A concrete subclass implements only [[openArchiveStream]]. Obtain the
 * reader for a path via `ArchiveReader(path)`, which selects the implementation by file
 * extension; new archive formats are added by writing another subclass rather than modifying the
 * shared engine.
 */
abstract class ArchiveReader(path: Path) {

  /**
   * Opens the archive at `path` as a commons-compress stream, transparently handling its
   * compression. The shared [[readEntries]] engine steps through entries via `getNextEntry`; a
   * subclass only chooses the container type (e.g. tar vs zip).
   */
  protected def openArchiveStream(conf: Configuration): ArchiveInputStream[_ <: ArchiveEntry]

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
  private def shouldSkipEntry(entry: ArchiveEntry, ignoredPathSegmentRegex: Pattern): Boolean = {
    if (entry.isDirectory) return true
    entry.getName.split("/").exists(c =>
      c.nonEmpty && HadoopFSUtils.shouldFilterOutPathName(c, ignoredPathSegmentRegex))
  }

  /**
   * Wraps the shared archive stream as a view over exactly the current entry's bytes
   * (`ArchiveInputStream.read` returns -1 at the entry boundary). [[CloseShieldInputStream]]
   * ignores `close()`, so a parser closing its input does not close the underlying archive; any
   * unread remainder of an entry is skipped by `getNextEntry()` when advancing.
   */
  private def entryStream(archive: ArchiveInputStream[_ <: ArchiveEntry]): InputStream =
    CloseShieldInputStream.wrap(archive)

  /**
   * Streams the archive entry by entry, applying `parseEntry` to each non-skipped entry's
   * `(name, stream)` and concatenating the results into a single iterator. The next entry is opened
   * only once the current entry's iterator is exhausted, so nothing is buffered to disk and at most
   * one entry's bytes are read at a time. The archive stream is closed when the returned iterator
   * is exhausted, when [[Closeable.close]] is called on it, and (defensively) on task completion.
   * Entry skipping mirrors Spark's file listing: `ignoredPathSegmentRegex` is the effective filter
   * (the `ignoredPathSegmentRegex` data source option), so callers reading with a custom filter
   * must pass its compiled form here for archive entries to be filtered like loose files.
   */
  def readEntries[T](
      conf: Configuration,
      ignoredPathSegmentRegex: Pattern = HadoopFSUtils.defaultIgnoredPathSegmentRegexPattern)(
      parseEntry: (String, InputStream) => Iterator[T]): Iterator[T] = {
    val archive = openArchiveStream(conf)
    var closed = false

    def cleanup(): Unit = {
      if (!closed) {
        closed = true
        try archive.close() catch { case NonFatal(_) => }
      }
    }

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => cleanup()))

    val entries = new Iterator[T] with Closeable {
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
          var entry = archive.getNextEntry
          while (entry != null && shouldSkipEntry(entry, ignoredPathSegmentRegex)) {
            entry = archive.getNextEntry
          }
          if (entry == null) {
            done = true
            cleanup()
          } else {
            currentIter = parseEntry(entry.getName, entryStream(archive))
          }
        }
      }

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

    // Open the first entry eagerly so the construction cost (and any failure) surfaces here rather
    // than at the first `hasNext`. A corrupt archive throws before the caller ever holds the
    // iterator, leaving it nothing to close: executors release the stream through the task-
    // completion listener, but driver-side callers (e.g. Avro's header-only schema inference) have
    // no task, so close it here before propagating.
    try {
      entries.hasNext
    } catch {
      case NonFatal(e) =>
        cleanup()
        throw e
    }
    entries
  }

  /**
   * Materializes each kept entry to a file under `localDir` as `(entryName, localFile)`, lazily one
   * at a time -- the [[readEntries]] counterpart for random-access formats (Parquet/ORC footers).
   */
  final def localizeEntries(
      conf: Configuration,
      localDir: File,
      entryFilter: String => Boolean = _ => true): Iterator[(String, File)] =
    readEntries(conf) { (name, in) =>
      if (entryFilter(name)) {
        Iterator.single((name, ArchiveReader.copyEntryToLocalFile(in, localDir, name)))
      } else {
        Iterator.empty
      }
    }
}

object ArchiveReader extends Logging {

  /**
   * Whether `path` names an archive this reader can stream.
   */
  def isArchivePath(path: Path): Boolean = {
    val name = path.getName.toLowerCase(Locale.ROOT)
    name.endsWith(".tar") || name.endsWith(".tar.gz") || name.endsWith(".tgz") ||
      name.endsWith(".zip")
  }

  /**
   * Returns the [[ArchiveReader]] implementation for `path`, selected by its file extension. Only
   * paths for which [[isArchivePath]] is true are supported; new archive formats add a case here.
   */
  def apply(path: Path): ArchiveReader = {
    val name = path.getName.toLowerCase(Locale.ROOT)
    name match {
      case n if n.endsWith(".tar") || n.endsWith(".tar.gz") || n.endsWith(".tgz") =>
        new TarArchiveReader(path)
      case n if n.endsWith(".zip") =>
        new ZipArchiveReader(path)
      case _ =>
        throw new IllegalArgumentException(
          s"$path is not a supported archive (expected .tar, .tar.gz, .tgz, or .zip)")
    }
  }

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

  /**
   * Copies one entry's bytes to a unique file under `localDir`.
   * The entry stream is left open (the reader owns it); the output file is closed.
   */
  private def copyEntryToLocalFile(in: InputStream, localDir: File, entryName: String): File = {
    val rawBasename = entryName.substring(entryName.lastIndexOf('/') + 1)
    val basename = rawBasename.replaceAll("[^A-Za-z0-9._-]", "_")
    val local = File.createTempFile("archive-entry-", "-" + basename, localDir)
    val out = new FileOutputStream(local)
    try Utils.copyStream(in, out) finally out.close()
    local
  }

  /**
   * Reads an archive of random-access files -- the [[readEntries]] counterpart for formats needing
   * a complete file (Parquet/ORC, Excel). The per-entry [[PartitionedFile]] keeps the
   * archive's path, so `input_file_name()`/`_metadata` report it.
   */
  def readLocalizedEntries(
      file: PartitionedFile,
      conf: Configuration,
      entryFilter: String => Boolean,
      tempPrefix: String)(
      readOne: PartitionedFile => Iterator[InternalRow]): Iterator[InternalRow] = {
    val tempDir = Utils.createTempDir(Utils.getLocalDir(SparkEnv.get.conf), tempPrefix)
    val entries = ArchiveReader(file.toPath).localizeEntries(conf, tempDir, entryFilter)

    // Element type is `Object`, not `InternalRow`: a batch scan yields `ColumnarBatch`, so
    // per-element casts would fail. The whole iterator is cast back to `Iterator[InternalRow]`
    // at the end, matching how the plain reader's columnar output is typed.
    val rows = new Iterator[Object] with Closeable {
      private var current: Iterator[Object] = Iterator.empty
      private var currentFile: File = _
      private var done = false

      private def releaseCurrent(): Unit = {
        current match {
          case c: Closeable => try c.close() catch { case NonFatal(_) => }
          case _ =>
        }
        current = Iterator.empty
        if (currentFile != null) {
          currentFile.delete()
          currentFile = null
        }
      }

      // Advance on `hasNext`, not in `next`, so a reader reusing a mutable batch is not probed for
      // the next entry before the current batch is consumed.
      private def advance(): Unit = {
        while (!done && !current.hasNext) {
          releaseCurrent()
          if (entries.hasNext) {
            val (_, entryFile) = entries.next()
            currentFile = entryFile
            current = readOne(file.copy(
              filePath = SparkPath.fromUri(entryFile.toURI),
              start = 0L,
              length = entryFile.length(),
              fileSize = entryFile.length(),
              modificationTime = file.modificationTime)).asInstanceOf[Iterator[Object]]
          } else {
            done = true
          }
        }
      }

      override def hasNext: Boolean = {
        advance()
        !done && current.hasNext
      }

      override def next(): Object = {
        if (!hasNext) throw new NoSuchElementException
        current.next()
      }

      override def close(): Unit = {
        done = true
        releaseCurrent()
        entries match {
          case c: Closeable => try c.close() catch { case NonFatal(_) => }
          case _ =>
        }
      }
    }

    // Delete only the temp dir here; FileScanRDD closes `rows`. Closing the per-entry reader from
    // this listener (it runs before FileScanRDD's) would free the vectorized reader's off-heap
    // vectors while downstream operators still read them.
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] { _ =>
      Utils.deleteRecursively(tempDir)
    })
    rows.asInstanceOf[Iterator[InternalRow]]
  }

  /**
   * Localizes one archive's kept entries and applies `use`. The entries iterator is closed before
   * returning (the driver has no TaskContext), so `use` must consume what it needs first.
   */
  private[datasources] def withLocalizedArchive[T](
      archive: FileStatus,
      conf: Configuration,
      tempDir: File,
      entryFilter: String => Boolean,
      ignoreMissingFiles: Boolean,
      ignoreCorruptFiles: Boolean,
      onSkip: => T)(
      use: Iterator[(String, File)] => T): T = {
    var entries: Iterator[(String, File)] = null
    try {
      entries = ArchiveReader(archive.getPath).localizeEntries(conf, tempDir, entryFilter)
      use(entries)
    } catch {
      case e: Exception if ignoreMissingFiles &&
          ExceptionUtils.getThrowables(e).exists(_.isInstanceOf[FileNotFoundException]) =>
        logWarning(log"Skipping missing archive during inference: " +
          log"${MDC(LogKeys.PATH, archive.getPath.toString)}", e)
        onSkip
      case NonFatal(e) =>
        Utils.getRootCause(e) match {
          // A missing archive is a FileNotFoundException; govern it by
          // ignoreMissingFiles (handled above), not by ignoreCorruptFiles -- matching FileScanRDD,
          // which rethrows missing-file errors regardless of ignoreCorruptFiles.
          case _: FileNotFoundException => throw e
          case _: RuntimeException | _: IOException if ignoreCorruptFiles =>
            logWarning(log"Skipping corrupt archive during inference: " +
              log"${MDC(LogKeys.PATH, archive.getPath.toString)}", e)
            onSkip
          case _ => throw e
        }
    } finally {
      entries match {
        case c: Closeable => c.close()
        case _ =>
      }
    }
  }

  /**
   * Driver-side schema inference for random-access archive formats: `looseInfer` seeds from loose
   * files; each archive's entries fold in one at a time -- `inferOne` reads one unpacked entry,
   * `mergeSchemas` combines it -- deleting each before the next. `mergeSchema` off samples one.
   */
  def inferArchiveSchema(
      files: Seq[FileStatus],
      conf: Configuration,
      tempPrefix: String,
      entryFilter: String => Boolean,
      ignoreMissingFiles: Boolean,
      ignoreCorruptFiles: Boolean,
      mergeSchema: Boolean,
      looseInfer: Seq[FileStatus] => Option[StructType],
      inferOne: File => Option[StructType],
      mergeSchemas: (StructType, StructType) => StructType): Option[StructType] = {
    val (archives, nonArchives) = files.partition(f => isArchivePath(f.getPath))
    val tempDir = Utils.createTempDir(namePrefix = tempPrefix)
    def foldArchive(archive: FileStatus, seed: Option[StructType], limit: Int): Option[StructType] =
      withLocalizedArchive(
        archive, conf, tempDir, entryFilter, ignoreMissingFiles, ignoreCorruptFiles,
        onSkip = seed) { entries =>
        var merged = seed
        entries.take(limit).foreach { case (_, file) =>
          try inferOne(file).foreach(s => merged = Some(merged.fold(s)(mergeSchemas(_, s))))
          finally file.delete()
        }
        merged
      }
    try {
      val looseSchema = if (nonArchives.nonEmpty) looseInfer(nonArchives) else None
      if (mergeSchema) {
        archives.foldLeft(looseSchema)((acc, a) => foldArchive(a, acc, Int.MaxValue))
      } else if (looseSchema.isDefined) {
        looseSchema
      } else {
        archives.iterator.map(foldArchive(_, None, 1)).find(_.isDefined).flatten
      }
    } finally {
      Utils.deleteRecursively(tempDir)
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

  override protected def openArchiveStream(
      conf: Configuration): ArchiveInputStream[_ <: ArchiveEntry] = {
    val base = CodecStreams.createInputStreamWithCloseResource(conf, path)
    try {
      // GZIPInputStream reads the gzip header in its constructor, so a corrupt archive can throw
      // here -- after `base` is already open -- and `base` must not leak.
      val tarBytes = if (needsExplicitGunzip) new GZIPInputStream(base) else base
      new TarArchiveInputStream(tarBytes)
    } catch {
      case NonFatal(e) =>
        try base.close() catch { case NonFatal(_) => }
        throw e
    }
  }
}

/**
 * [[ArchiveReader]] for zip archives (`.zip`). Each entry is individually compressed inside the
 * container (the container itself is not gzip-wrapped), so `ZipArchiveInputStream` decompresses
 * entries as they are streamed and no Hadoop codec layer is applied. The stream reads local file
 * headers sequentially rather than the central directory, matching the tar reader's pure-streaming
 * model: a few unusual zips (e.g. a stored entry whose size is recorded only in a trailing data
 * descriptor) are not streamable this way.
 */
class ZipArchiveReader(path: Path) extends ArchiveReader(path) {

  override protected def openArchiveStream(
      conf: Configuration): ArchiveInputStream[_ <: ArchiveEntry] =
    new ZipArchiveInputStream(CodecStreams.createInputStreamWithCloseResource(conf, path))
}
