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

import java.io.{Closeable, File, FileOutputStream, InputStream}
import java.util.Locale
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

import scala.util.control.NonFatal

import org.apache.commons.compress.archivers.{ArchiveEntry, ArchiveInputStream}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.io.ByteOrderMark
import org.apache.commons.io.input.{BOMInputStream, CloseShieldInputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.util.{HadoopFSUtils, Utils}

/**
 * Mixed into a file-based data source to let it read tar/zip archives of its files, treating an
 * archive like a directory of the entries it contains. Schema inference is left to each format.
 */
trait SupportsArchiveFormat extends Logging {

  /**
   * Splits one already-decompressed archive entry's bytes into lines (`None` separator = detect
   * line breaks), yielding each line as [[Text]] without the trailing separator. Does not close the
   * entry stream (the reader owns it).
   */
  protected def lineIterator(
      in: InputStream,
      lineSeparatorInRead: Option[Array[Byte]]): Iterator[Text] = {
    // Strip a leading BOM (LineReader does not) so lines match the non-archive read path.
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

  /** Which archive entries are data files of this format; others are skipped. */
  protected def archiveEntryFilter(name: String): Boolean =
    throw new UnsupportedOperationException(
      s"${getClass.getName} does not support random-access archive reads")

  /**
   * Materializes each kept entry (those passing [[archiveEntryFilter]]) to a temp file under
   * `localDir`, lazily one at a time, so only one entry occupies disk at once.
   *
   * @param path     the archive path
   * @param conf     Hadoop configuration used to open the archive
   * @param localDir directory the per-entry temp files are created under
   * @return an iterator of `(entryName, localFile)` for the kept entries, in archive order
   */
  private def localizeEntries(
      path: Path,
      conf: Configuration,
      localDir: File): Iterator[(String, File)] =
    SupportsArchiveFormat.readArchiveEntries(path, conf) { (name, in) =>
      if (archiveEntryFilter(name)) {
        Iterator.single((name, SupportsArchiveFormat.copyEntryToLocalFile(in, localDir, name)))
      } else {
        Iterator.empty
      }
    }

  /**
   * Reads an archive by unpacking each entry to a temp file and applying `readEntry`, for a
   * format that needs a complete file on disk (random access).
   *
   * @param file       the archive as a [[PartitionedFile]]
   * @param conf       Hadoop configuration used to open the archive
   * @param tempPrefix prefix for the per-task temp dir the entries are unpacked into
   * @param readEntry  reads one unpacked entry file into rows
   * @return iterator of rows across all entries
   */
  protected def readLocalizedEntries(
      file: PartitionedFile,
      conf: Configuration,
      tempPrefix: String)(
      readEntry: PartitionedFile => Iterator[InternalRow]): Iterator[InternalRow] = {
    val tempDir = Utils.createTempDir(Utils.getLocalDir(SparkEnv.get.conf), tempPrefix)
    val entries = localizeEntries(file.toPath, conf, tempDir)

    // Element type is `Object`, not `InternalRow`: a batch scan yields `ColumnarBatch`.
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

      // Advance on `hasNext`, not `next`, so a reader reusing a mutable batch is not probed early.
      private def advance(): Unit = {
        while (!done && !current.hasNext) {
          releaseCurrent()
          if (entries.hasNext) {
            val (_, entryFile) = entries.next()
            currentFile = entryFile
            current = readEntry(file.copy(
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
    // this listener would free the vectorized reader's off-heap vectors while still in use.
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit] { _ =>
      Utils.deleteRecursively(tempDir)
    })
    rows.asInstanceOf[Iterator[InternalRow]]
  }

}

object SupportsArchiveFormat {

  // Basename chars unsafe in a temp file name; precompiled since it runs per entry.
  private val unsafeEntryNameChars = Pattern.compile("[^A-Za-z0-9._-]")

  /**
   * Whether the file path is a supported archive format.
   */
  def isArchivePath(path: Path): Boolean = {
    val name = path.getName.toLowerCase(Locale.ROOT)
    name.endsWith(".tar") || name.endsWith(".tar.gz") || name.endsWith(".tgz") ||
      name.endsWith(".zip")
  }

  /**
   * Opens the archive at `path` as a commons-compress stream, selecting the container by extension.
   */
  private def openArchiveStream(
      path: Path,
      conf: Configuration): ArchiveInputStream[_ <: ArchiveEntry] = {
    val name = path.getName.toLowerCase(Locale.ROOT)
    name match {
      case n if n.endsWith(".tar") || n.endsWith(".tar.gz") || n.endsWith(".tgz") =>
        val base = CodecStreams.createInputStreamWithCloseResource(conf, path)
        try {
          // GZIPInputStream reads the gzip header in its constructor, so a corrupt archive can
          // throw here -- after `base` is already open -- and `base` must not leak.
          val tarBytes = if (n.endsWith(".tgz")) new GZIPInputStream(base) else base
          new TarArchiveInputStream(tarBytes)
        } catch {
          case NonFatal(e) =>
            try base.close() catch { case NonFatal(_) => }
            throw e
        }
      case n if n.endsWith(".zip") =>
        new ZipArchiveInputStream(CodecStreams.createInputStreamWithCloseResource(conf, path))
      case _ =>
        throw new IllegalArgumentException(
          s"$path is not a supported archive (expected .tar, .tar.gz, .tgz, or .zip)")
    }
  }

  /**
   * Whether an entry is a directory or a name Spark's file listing would filter out, so archive
   * reads match a loose-file scan.
   *
   * @param entry                   the archive entry to test
   * @param ignoredPathSegmentRegex per-segment filter matched against each `/`-separated component
   * @return true if the entry is a directory or any path component is filtered out
   */
  private def shouldSkipEntry(entry: ArchiveEntry, ignoredPathSegmentRegex: Pattern): Boolean = {
    if (entry.isDirectory) return true
    entry.getName.split("/").exists(c =>
      c.nonEmpty && HadoopFSUtils.shouldFilterOutPathName(c, ignoredPathSegmentRegex))
  }

  /**
   * Streams the archive at `path` entry by entry, applying `parseEntry` to each non-skipped
   * `(name, stream)` and concatenating the results.
   *
   * @param path                    the archive path
   * @param conf                    Hadoop configuration used to open the archive
   * @param ignoredPathSegmentRegex per-segment filter for entries to skip (defaults to the
   *                                `InMemoryFileIndex` filter); pass a custom one to match a
   *                                loose-file scan
   * @param parseEntry              turns one entry's `(name, stream)` into an iterator of results
   * @return the concatenated results across kept entries, lazily one entry at a time
   */
  def readArchiveEntries[T](
      path: Path,
      conf: Configuration,
      ignoredPathSegmentRegex: Pattern = HadoopFSUtils.defaultIgnoredPathSegmentRegexPattern)(
      parseEntry: (String, InputStream) => Iterator[T]): Iterator[T] = {
    val archive = openArchiveStream(path, conf)
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

      // Move to the next entry with elements (releasing the exhausted one), or mark done. Advancing
      // on `hasNext` rather than eagerly in `next` matters for parsers that reuse a mutable row: an
      // early probe would overwrite the returned row before the caller copies it.
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
            // CloseShieldInputStream ignores close(), so a parser closing its input does not close
            // the archive; any unread remainder is skipped by getNextEntry() when advancing.
            currentIter = parseEntry(entry.getName, CloseShieldInputStream.wrap(archive))
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

    // Open the first entry eagerly so a corrupt-archive failure surfaces here, not at first
    // `hasNext`. The caller then holds no iterator to close, and a driver-side caller (e.g. Avro
    // header inference) has no task-completion listener, so close the stream here before rethrow.
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
   * Copies one archive entry's bytes to a fresh temp file under localDir.
   *
   * @param in        the entry's byte stream, positioned at the entry start
   * @param localDir  directory the temp file is created under
   * @param entryName the entry's archive path; its sanitized basename names the temp file
   * @return the temp [[File]] holding the entry's bytes
   */
  def copyEntryToLocalFile(in: InputStream, localDir: File, entryName: String): File = {
    val rawBasename = entryName.substring(entryName.lastIndexOf('/') + 1)
    val basename = unsafeEntryNameChars.matcher(rawBasename).replaceAll("_")
    val local = File.createTempFile("archive-entry-", "-" + basename, localDir)
    val out = new FileOutputStream(local)
    try Utils.copyStream(in, out) finally out.close()
    local
  }

}
