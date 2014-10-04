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
  private[scheduler] val logPath = getLogPath(logBaseDir, appId)

  /**
   * Creates the log file in the configured log directory.
   */
  def start() {
    if (!fileSystem.isDirectory(new Path(logBaseDir))) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir does not exist.");
    }

    val workingPath = logPath + IN_PROGRESS
    val uri = new URI(workingPath)
    val path = new Path(workingPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    /* The Hadoop LocalFileSystem (r1.0.4) has known issues with syncing (HADOOP-7844).
     * Therefore, for local files, use FileOutputStream instead. */
    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        hadoopDataStream = Some(fileSystem.create(path))
        hadoopDataStream.get
      }

    val compressionCodec =
      if (shouldCompress) {
        Some(CompressionCodec.createCodec(sparkConf))
      } else {
        None
      }

    fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
    val logStream = initEventLog(new BufferedOutputStream(dstream, outputBufferSize),
      compressionCodec)
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
  // No-op because logging every update would be overkill
  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate) { }

  /**
   * Stop logging events.
   */
  def stop() = {
    writer.foreach(_.close())

    val target = new Path(logPath)
    if (fileSystem.exists(target)) {
      throw new IOException("Target log file already exists (%s)".format(logPath))
    }
    fileSystem.rename(new Path(logPath + IN_PROGRESS), target)
  }

}

private[spark] object EventLoggingListener extends Logging {
  // Suffix applied to the names of files still being written by applications.
  val IN_PROGRESS = ".inprogress"
  val DEFAULT_LOG_DIR = "/tmp/spark-events"

  private val LOG_FILE_PERMISSIONS = FsPermission.createImmutable(Integer.parseInt("770", 8)
    .toShort)

  // Marker for the end of header data in a log file. After this marker (and the following
  // new line), log data, potentially compressed, will be found.
  private val HEADER_END_MARKER = "HEADER_END_MARKER"

  // A cache for compression codecs to avoid creating the same codec many times
  private val codecMap = new mutable.HashMap[String, CompressionCodec]

  /**
   * Write metadata about the event log to the given stream.
   *
   * The header is a serialized version of a map, except it does not use Java serialization to
   * avoid incompatibilities between different JDKs. It writes map entries in a simple format:
   *
   *   [len][bytes]
   *
   * Where `len` is an integer, and `bytes` is the UTF-8 encoded version of "key=value". The
   * very last entry in the header is the HEADER_END_MARKER marker, encoded like the above,
   * so that the parsing code can know when to stop.
   *
   * The format needs to be kept in sync with the openEventLog() method below. Also, it cannot
   * change in new Spark versions without some other way of detecting the change (like some
   * metadata encoded in the file name).
   *
   * @param logStream Raw output stream to the even log file.
   * @param compressionCodec Optional compression codec to use.
   * @return A stream where to write event log data. This may be a wrapper around the original
   *         stream (for example, when compression is enabled).
   */
  def initEventLog(logStream: OutputStream, compressionCodec: Option[CompressionCodec]):
    OutputStream = {
    val meta = mutable.HashMap(("version" -> SPARK_VERSION))
    compressionCodec.foreach { codec =>
      meta += ("compressionCodec" -> codec.getClass().getName())
    }

    val header = new DataOutputStream(logStream)
    def write(entry: String) = {
      val bytes = entry.getBytes(Charsets.UTF_8)
      header.writeInt(bytes.length)
      header.write(bytes, 0, bytes.length)
    }

    meta.foreach { case (k, v) => write(s"$k=$v") }
    write(EventLoggingListener.HEADER_END_MARKER)
    header.flush()

    compressionCodec.map(_.compressedOutputStream(logStream)).getOrElse(logStream)
  }

  /**
   * Return a file-system-safe path to the log file for the given application.
   *
   * @param logBaseDir Directory where the log file will be written.
   * @param appId A unique app ID.
   * @return A path which consists of file-system-safe characters.
   */
  def getLogPath(logBaseDir: String, appId: String): String = {
    val name = appId.replaceAll("[ :/]", "-").replaceAll("[${}'\"]", "_").toLowerCase
    Utils.resolveURI(logBaseDir) + "/" + name.stripSuffix("/")
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

    val in = new DataInputStream(new BufferedInputStream(fs.open(log)))
    def read() = {
      // TODO: a corrupted / malicious file can cause this code to load lots of data into
      // memory. Better to have a limit on the size of the header?
      val len = in.readInt()
      val bytes = new Array[Byte](len)
      in.readFully(bytes)
      new String(bytes, Charsets.UTF_8)
    }

    try {
      val meta = new mutable.HashMap[String, String]()
      var foundEndMarker = false
      while (!foundEndMarker) {
        read() match {
          case HEADER_END_MARKER =>
            foundEndMarker = true
          case entry =>
            val prop = entry.split("=", 2)
            if (prop.length != 2) {
              throw new IllegalArgumentException("Invalid metadata in log file.")
            }
            meta += (prop(0) -> prop(1))
        }
      }

      val sparkVersion = meta.get("version").getOrElse(
        throw new IllegalArgumentException("Missing Spark version in log metadata."))

      val codec = meta.get("compressionCodec").map { codecName =>
          val conf = new SparkConf()
          codecMap.getOrElseUpdate(codecName, CompressionCodec.createCodec(conf, codecName))
        }

      (codec.map(_.compressedInputStream(in)).getOrElse(in), sparkVersion)
    } catch {
      case e: Exception =>
        in.close()
        throw e
    }
  }

}
