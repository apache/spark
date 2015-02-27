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
import java.net.URI

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SPARK_VERSION}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.{JsonProtocol, Utils}

/**
 * A SparkListener that logs events to persistent storage.
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 */
private[spark] class EventLoggingListener(
    appId: String,
    logBaseDir: String,
    sparkConf: SparkConf,
    hadoopConf: Configuration)
  extends SparkListener with Logging {

  import EventLoggingListener._

  def this(appId: String, logBaseDir: String, sparkConf: SparkConf) =
    this(appId, logBaseDir, sparkConf, SparkHadoopUtil.get.newConfiguration(sparkConf))

  private val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = sparkConf.getBoolean("spark.eventLog.overwrite", false)
  private val testing = sparkConf.getBoolean("spark.eventLog.testing", false)
  private val outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val fileSystem = Utils.getHadoopFileSystem(new URI(logBaseDir), hadoopConf)
  private val compressionCodec =
    if (shouldCompress) {
      Some(CompressionCodec.createCodec(sparkConf))
    } else {
      None
    }
  private val compressionCodecName = compressionCodec.map(_.getClass.getCanonicalName)

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  // The Hadoop APIs have changed over time, so we use reflection to figure out
  // the correct method to use to flush a hadoop data stream. See SPARK-1518
  // for details.
  private val hadoopFlushMethod = {
    val cls = classOf[FSDataOutputStream]
    scala.util.Try(cls.getMethod("hflush")).getOrElse(cls.getMethod("sync"))
  }

  private var writer: Option[PrintWriter] = None

  // For testing. Keep track of all JSON serialized events that have been logged.
  private[scheduler] val loggedEvents = new ArrayBuffer[JValue]

  // Visible for tests only.
  private[scheduler] val logPath = getLogPath(logBaseDir, appId, compressionCodecName)

  /**
   * Creates the log file in the configured log directory.
   */
  def start() {
    if (!fileSystem.isDirectory(new Path(logBaseDir))) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir does not exist.")
    }

    val workingPath = logPath + IN_PROGRESS
    val uri = new URI(workingPath)
    val path = new Path(workingPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    if (shouldOverwrite && fileSystem.exists(path)) {
      logWarning(s"Event log $path already exists. Overwriting...")
      fileSystem.delete(path, true)
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
    val cstream = compressionCodec.map(_.compressedOutputStream(dstream)).getOrElse(dstream)
    val bstream = new BufferedOutputStream(cstream, outputBufferSize)

    fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)

    val logStream = initEventLog(bstream, compressionCodec)
    writer = Some(new PrintWriter(logStream))

    logInfo("Logging events to %s".format(logPath))
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent, flushLogger: Boolean = false) {
    val eventJson = JsonProtocol.sparkEventToJson(event)
    writer.foreach(_.println(compact(render(eventJson))))
    if (flushLogger) {
      writer.foreach(_.flush())
      hadoopDataStream.foreach(hadoopFlushMethod.invoke(_))
    }
    if (testing) {
      loggedEvents += eventJson
    }
  }

  // Events that do not trigger a flush
  override def onStageSubmitted(event: SparkListenerStageSubmitted) =
    logEvent(event)
  override def onTaskStart(event: SparkListenerTaskStart) =
    logEvent(event)
  override def onTaskGettingResult(event: SparkListenerTaskGettingResult) =
    logEvent(event)
  override def onTaskEnd(event: SparkListenerTaskEnd) =
    logEvent(event)
  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate) =
    logEvent(event)

  // Events that trigger a flush
  override def onStageCompleted(event: SparkListenerStageCompleted) =
    logEvent(event, flushLogger = true)
  override def onJobStart(event: SparkListenerJobStart) =
    logEvent(event, flushLogger = true)
  override def onJobEnd(event: SparkListenerJobEnd) =
    logEvent(event, flushLogger = true)
  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded) =
    logEvent(event, flushLogger = true)
  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved) =
    logEvent(event, flushLogger = true)
  override def onUnpersistRDD(event: SparkListenerUnpersistRDD) =
    logEvent(event, flushLogger = true)
  override def onApplicationStart(event: SparkListenerApplicationStart) =
    logEvent(event, flushLogger = true)
  override def onApplicationEnd(event: SparkListenerApplicationEnd) =
    logEvent(event, flushLogger = true)
  override def onExecutorAdded(event: SparkListenerExecutorAdded) =
    logEvent(event, flushLogger = true)
  override def onExecutorRemoved(event: SparkListenerExecutorRemoved) =
    logEvent(event, flushLogger = true)

  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate) { }

  /**
   * Stop logging events. The event log file will be renamed so that it loses the
   * ".inprogress" suffix.
   */
  def stop() = {
    writer.foreach(_.close())

    val target = new Path(logPath)
    if (fileSystem.exists(target)) {
      if (shouldOverwrite) {
        logWarning(s"Event log $target already exists. Overwriting...")
        fileSystem.delete(target, true)
      } else {
        throw new IOException("Target log file already exists (%s)".format(logPath))
      }
    }
    fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
  }

}

private[spark] object EventLoggingListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"

  val EVENT_LOG_KEY = "EVENT_LOG"
  val SPARK_VERSION_KEY = "SPARK_VERSION"
  val COMPRESSION_CODEC_KEY = "COMPRESSION_CODEC"

  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  // To avoid corrupted files causing the heap to fill up. Value is arbitrary.
  private val MAX_HEADER_LINE_LENGTH = 4096

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  /**
   * Write metadata about the event log to the given stream.
   *
   * The header is a single line of JSON in the beginning of the file. Note that this
   * assumes all metadata necessary to parse the log is also included in the file name.
   * The format needs to be kept in sync with the `openEventLog()` method below. Also, it
   * cannot change in new Spark versions without some other way of detecting the change.
   *
   * @param logStream Raw output stream to the event log file.
   * @param compressionCodec Optional compression codec to use.
   * @return A stream to which event log data is written. This may be a wrapper around the original
   *         stream (for example, when compression is enabled).
   */
  def initEventLog(
      logStream: OutputStream,
      compressionCodec: Option[CompressionCodec]): OutputStream = {
    val metadata = new mutable.HashMap[String, String]
    // Some of these metadata are already encoded in the file name
    // Here we include them again within the file itself for completeness
    metadata += ("Event" -> Utils.getFormattedClassName(SparkListenerMetadataIdentifier))
    metadata += (SPARK_VERSION_KEY -> SPARK_VERSION)
    compressionCodec.foreach { codec =>
      metadata += (COMPRESSION_CODEC_KEY -> codec.getClass.getCanonicalName)
    }
    val metadataJson = compact(render(JsonProtocol.mapToJson(metadata)))
    val metadataBytes = (metadataJson + "\n").getBytes(Charsets.UTF_8)
    if (metadataBytes.length > MAX_HEADER_LINE_LENGTH) {
      throw new IOException(s"Event log metadata too long: $metadataJson")
    }
    logStream.write(metadataBytes, 0, metadataBytes.length)
    logStream
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * Note that because we currently only create a single log file for each application,
   * we must encode all the information needed to parse this event log in the file name
   * instead of within the file itself. Otherwise, if the file is compressed, for instance,
   * we won't know which codec to use to decompress the metadata.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @param compressionCodecName Name of the compression codec used to compress the contents
   *                             of the log, or None if compression is not enabled.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(
      logBaseDir: String,
      appId: String,
      compressionCodecName: Option[String]): String = {
    val sanitizedAppId = appId.replaceAll("[ :/]", "-").replaceAll("[${}'\"]", "_").toLowerCase
    // e.g. EVENT_LOG_app_123_SPARK_VERSION_1.3.1
    // e.g. EVENT_LOG_ {...} _COMPRESSION_CODEC_org.apache.spark.io.LZFCompressionCodec
    val logName = s"${EVENT_LOG_KEY}_${sanitizedAppId}_${SPARK_VERSION_KEY}_$SPARK_VERSION" +
      compressionCodecName.map { c => s"_${COMPRESSION_CODEC_KEY}_$c" }.getOrElse("")
    Utils.resolveURI(logBaseDir).toString.stripSuffix("/") + "/" + logName.stripSuffix("/")
  }

  /**
   * Opens an event log file and returns an input stream to the event data.
   *
   * @return 2-tuple (event input stream, Spark version of event data)
   */
  def openEventLog(log: Path, fs: FileSystem): (InputStream, String) = {
    // It's not clear whether FileSystem.open() throws FileNotFoundException or just plain
    // IOException when a file does not exist, so try our best to throw a proper exception.
    if (!fs.exists(log)) {
      throw new FileNotFoundException(s"File $log does not exist.")
    }

    val in = new BufferedInputStream(fs.open(log))

    // Parse information from the log name
    val logName = log.getName
    val baseRegex = s"${EVENT_LOG_KEY}_(.*)_${SPARK_VERSION_KEY}_(.*)".r
    val compressionRegex = (baseRegex + s"_${COMPRESSION_CODEC_KEY}_(.*)").r
    val (sparkVersion, codecName) = logName match {
      case compressionRegex(_, version, _codecName) => (version, Some(_codecName))
      case baseRegex(_, version) => (version, None)
      case _ => throw new IllegalArgumentException(s"Malformed event log name: $logName")
    }
    val codec = codecName.map { c =>
      codecMap.getOrElseUpdate(c, CompressionCodec.createCodec(new SparkConf, c))
    }

    try {
      (codec.map(_.compressedInputStream(in)).getOrElse(in), sparkVersion)
    } catch {
      case e: Exception =>
        in.close()
        throw e
    }
  }

}
