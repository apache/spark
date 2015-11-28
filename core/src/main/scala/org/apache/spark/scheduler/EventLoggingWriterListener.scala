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

package org.apache.spark.scheduler

import java.io._
import java.lang.reflect.Method
import java.net.URI

import com.google.common.base.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[spark] trait EventLoggingWriterListener {
  def start()

  def stop ()

  def getFilePath(): String

  def getLoggedEvent: ArrayBuffer[JValue]
}

private[spark] object EventLoggingWriterListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"
  val SPARK_VERSION_KEY = "SPARK_VERSION"
  val COMPRESSION_CODEC_KEY = "COMPRESSION_CODEC"

  val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new mutable.HashMap[String, CompressionCodec]


  def getHadoopFlushMethod: Method = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }

  /**
   * Write metadata about an event log to the given stream.
   * The metadata is encoded in the first line of the event log as JSON.
   *
   * @param logStream Raw output stream to the event log file.
   */
  def initEventLog(logStream: OutputStream): Unit = {
    val metadata = SparkListenerLogStart(SPARK_VERSION)
    val metadataJson = compact(JsonProtocol.logStartToJson(metadata)) + "\n"
    logStream.write(metadataJson.getBytes(Charsets.UTF_8))
  }

  def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata needed to open the file in
   * the first place.
   *
   * The log file name will identify the compression codec used for the contents, if any.
   * For example, app_123 for an uncompressed log, app_123.lzf for an LZF-compressed log.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param appAttemptId A unique attempt id of appId. May be the empty string.
   * @param compressionCodecName Name to identify the codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: URI,
      appId: String,
      appAttemptId: Option[String],
      compressionCodecName: Option[String] = None): String = {
    val base = logBaseDir.toString.stripSuffix("/") + "/" + sanitize(appId)
    val codec = compressionCodecName.map("." + _).getOrElse("")
    if (appAttemptId.isDefined) {
      base + "_" + sanitize(appAttemptId.get) + codec
    } else {
      base + codec
    }
  }

  /**
   * Create the log file and return file writer and DataStream.
   * @param logPath
   * @param fileSystem
   * @param shouldOverwrite
   * @param outputBufferSize
   * @param hadoopConf
   * @param compressionCodec
   * @param isMetaFile
   * @return
   */
  def getFileWriter(
      logPath: String,
      fileSystem: FileSystem,
      shouldOverwrite: Boolean,
      outputBufferSize: Int,
      hadoopConf: Configuration,
      compressionCodec: Option[CompressionCodec],
      isMetaFile: Boolean): (PrintWriter, Option[FSDataOutputStream]) = {
    var hadoopDataStream: Option[FSDataOutputStream] = None
    val workingPath = logPath + IN_PROGRESS
    val uri = new URI(workingPath)
    val path = new Path(workingPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    if (shouldOverwrite && fileSystem.exists(path)) {
      logWarning(s"Event log $path already exists. Overwriting...")
      if (!fileSystem.delete(path, true)) {
        logWarning(s"Error deleting $path")
      }
    }

    /* The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
     * Therefore, for local files, use FileOutputStream instead. */
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(fileSystem.create(path))
        hadoopDataStream.get
      }

    try {
      val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
      val bstream = new BufferedOutputStream(cstream, outputBufferSize)

      if (isMetaFile) {
        initEventLog(bstream)
      }
      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      val writer = new PrintWriter(bstream)
      logInfo("Logging events to %s".format(workingPath))

      (writer, hadoopDataStream)
    } catch {
      case e: Exception =>
        dstream.close()
        throw e
    }
  }

  /**
   * Opens an event log file and returns an input stream that contains the event data.
   *
   * @return input stream that holds one JSON record per line.
   */
  def openEventLog(log: Path, fs: FileSystem): InputStream = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    if (!fs.exists(log)) {
      throw new FileNotFoundException(s"File $log does not exist.")
    }

    val in = new BufferedInputStream(fs.open(log))

    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    val codec = codecName.map { c =>
      codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
    }

    try {
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Exception =>
        // When using snappy codec and the part file is empty, it will occur exception
        val caseMess = e.getMessage
        if (caseMess.contains("empty stream")) {
          return null
        }
        in.close()
        throw e
    }
  }

  def renameFile(fileSystem: FileSystem, shouldOverwrite: Boolean, logPath: String): Unit = {
    val target = new Path(logPath)
    if (fileSystem.exists(target)) {
      if (shouldOverwrite) {
        logWarning(s"Event log $target already exists. Overwriting...")
        if (!fileSystem.delete(target, true)) {
          logWarning(s"Error deleting $target")
        }
      } else {
        throw new IOException("Target log file already exists (%s)".format(logPath))
      }
    }
    fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
  }

  def getMetaStream(log: Path, fs: FileSystem): InputStream = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    if (!fs.exists(log)) {
      throw new FileNotFoundException(s"File $log does not exist.")
    }

    val metaDir = log.toString.replaceAll("part\\d+", "meta")
    val metaPath =
      if (fs.exists(new Path(metaDir))) {
        new Path(metaDir)
      } else if (fs.exists(new Path(metaDir + IN_PROGRESS))) {
        new Path(metaDir + IN_PROGRESS)
      } else {
        throw new FileNotFoundException(s"The meta file ${log.getName} does not exist.")
      }

    val in = new BufferedInputStream(fs.open(metaPath))

    // Compression codec is encoded as an extension, e.g. app_123.lzf
    // Since we sanitize the app ID to not include periods, it is safe to split on it
    val logName = log.getName.stripSuffix(IN_PROGRESS)
    val codecName: Option[String] = logName.split("\\.").tail.lastOption
    val codec = codecName.map { c =>
      codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
    }

    try {
      codec.map(_.compressedInputStream(in)).getOrElse(in)
    } catch {
      case e: Exception =>
        in.close()
        throw e
    }
  }
}
