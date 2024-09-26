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

package org.apache.spark.util

import java.io._
import java.lang.{Byte => JByte}
import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, PlatformManagedObject, ThreadInfo}
import java.lang.reflect.InvocationTargetException
import java.math.{MathContext, RoundingMode}
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.SecureRandom
import java.util.{Locale, Properties, Random, UUID}
import java.util.concurrent._
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.zip.{GZIPInputStream, ZipInputStream}

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.matching.Regex

import _root_.io.netty.channel.unix.Errors.NativeIoException
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Interners
import com.google.common.io.{ByteStreams, Files => GFiles}
import com.google.common.net.InetAddresses
import jakarta.ws.rs.core.UriBuilder
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.{JavaVersion, SystemUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.ipc.{CallerContext => HadoopCallerContext}
import org.apache.hadoop.ipc.CallerContext.{Builder => HadoopCallerContextBuilder}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.{RunJar, StringUtils}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.LoggerConfig
import org.eclipse.jetty.util.MultiException
import org.slf4j.Logger

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{Logging, MDC, MessageWithContext}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Streaming._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.{Utils => CUtils}
import org.apache.spark.util.io.ChunkedByteBufferOutputStream

/** CallSite represents a place in user code. It can have a short and a long form. */
private[spark] case class CallSite(shortForm: String, longForm: String)

private[spark] object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
  val empty = CallSite("", "")
}

/**
 * Various utility methods used by Spark.
 */
private[spark] object Utils
  extends Logging
  with SparkClassUtils
  with SparkEnvUtils
  with SparkErrorUtils
  with SparkFileUtils
  with SparkSerDeUtils
  with SparkStreamUtils {

  private val sparkUncaughtExceptionHandler = new SparkUncaughtExceptionHandler
  @volatile private var cachedLocalDir: String = ""

  /**
   * Define a default value for driver memory here since this value is referenced across the code
   * base and nearly all files already use Utils.scala
   */
  val DEFAULT_DRIVER_MEM_MB = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  @volatile private var localRootDirs: Array[String] = null

  /** Scheme used for files that are locally available on worker nodes in the cluster. */
  val LOCAL_SCHEME = "local"

  private val weakStringInterner = Interners.newWeakInterner[String]()

  private val PATTERN_FOR_COMMAND_LINE_ARG = "-D(.+?)=(.+)".r

  private val COPY_BUFFER_LEN = 1024

  private val copyBuffer = ThreadLocal.withInitial[Array[Byte]](() => {
    new Array[Byte](COPY_BUFFER_LEN)
  })
  /** Deserialize a Long value (used for [[org.apache.spark.api.python.PythonPartitioner]]) */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /** Serialize via nested stream using specific serializer */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
      f: SerializationStream => Unit): Unit = {
    val osWrapper = ser.serializeStream(new OutputStream {
      override def write(b: Int): Unit = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /** Deserialize via nested stream using specific serializer */
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
      f: DeserializationStream => Unit): Unit = {
    val isWrapper = ser.deserializeStream(new InputStream {
      override def read(): Int = is.read()
      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /** String interning to reduce the memory usage. */
  def weakIntern(s: String): String = {
    weakStringInterner.intern(s)
  }

  /**
   * Run a segment of code using a different context class loader in the current thread
   */
  def withContextClassLoader[T](ctxClassLoader: ClassLoader)(fn: => T): T = {
    val oldClassLoader = Thread.currentThread().getContextClassLoader()
    try {
      Thread.currentThread().setContextClassLoader(ctxClassLoader)
      fn
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader)
    }
  }

  private def writeByteBufferImpl(bb: ByteBuffer, writer: (Array[Byte], Int, Int) => Unit): Unit = {
    if (bb.hasArray) {
      // Avoid extra copy if the bytebuffer is backed by bytes array
      writer(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      // Fallback to copy approach
      val buffer = {
        // reuse the copy buffer from thread local
        copyBuffer.get()
      }
      val originalPosition = bb.position()
      var bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      while (bytesToCopy > 0) {
        bb.get(buffer, 0, bytesToCopy)
        writer(buffer, 0, bytesToCopy)
        bytesToCopy = Math.min(bb.remaining(), COPY_BUFFER_LEN)
      }
      bb.position(originalPosition)
    }
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.DataOutput]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: DataOutput): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.OutputStream]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: OutputStream): Unit = {
    writeByteBufferImpl(bb, out.write)
  }

  /**
   * JDK equivalent of `chmod 700 file`.
   *
   * @param file the file whose permissions will be modified
   * @return true if the permissions were successfully changed, false otherwise.
   */
  def chmod700(file: File): Boolean = {
    file.setReadable(false, false) &&
    file.setReadable(true, true) &&
    file.setWritable(false, false) &&
    file.setWritable(true, true) &&
    file.setExecutable(false, false) &&
    file.setExecutable(true, true)
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  override def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
   * Copy the first `maxSize` bytes of data from the InputStream to an in-memory
   * buffer, primarily to check for corruption.
   *
   * This returns a new InputStream which contains the same data as the original input stream.
   * It may be entirely on in-memory buffer, or it may be a combination of in-memory data, and then
   * continue to read from the original stream. The only real use of this is if the original input
   * stream will potentially detect corruption while the data is being read (e.g. from compression).
   * This allows for an eager check of corruption in the first maxSize bytes of data.
   *
   * @return An InputStream which includes all data from the original stream (combining buffered
   *         data and remaining data in the original stream)
   */
  def copyStreamUpTo(in: InputStream, maxSize: Long): InputStream = {
    var count = 0L
    val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
    val fullyCopied = tryWithSafeFinally {
      val bufSize = Math.min(8192L, maxSize)
      val buf = new Array[Byte](bufSize.toInt)
      var n = 0
      while (n != -1 && count < maxSize) {
        n = in.read(buf, 0, Math.min(maxSize - count, bufSize).toInt)
        if (n != -1) {
          out.write(buf, 0, n)
          count += n
        }
      }
      count < maxSize
    } {
      try {
        if (count < maxSize) {
          in.close()
        }
      } finally {
        out.close()
      }
    }
    if (fullyCopied) {
      out.toChunkedByteBuffer.toInputStream(dispose = true)
    } else {
      new SequenceInputStream( out.toChunkedByteBuffer.toInputStream(dispose = true), in)
    }
  }

  /**
   * A file name may contain some invalid URI characters, such as " ". This method will convert the
   * file name to a raw path accepted by `java.net.URI(String)`.
   *
   * Note: the file name must not contain "/" or "\"
   */
  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    encodeRelativeUnixPathToURIRawPath(fileName)
  }

  /**
   * Same as [[encodeFileNameToURIRawPath]] but returns the relative UNIX path.
   */
  def encodeRelativeUnixPathToURIRawPath(path: String): String = {
    require(!path.startsWith("/") && !path.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    new URI("file", null, "localhost", -1, "/" + path, null, null).getRawPath.substring(1)
  }

  /**
   * Get the file name from uri's raw path and decode it. If the raw path of uri ends with "/",
   * return the name before the last "/".
   */
  def decodeFileNameInURI(uri: URI): String = {
    val rawPath = uri.getRawPath
    val rawFileName = rawPath.split("/").last
    new URI("file:///" + rawFileName).getPath.substring(1)
  }

  /**
   * Download a file or directory to target directory. Supports fetching the file in a variety of
   * ways, including HTTP, Hadoop-compatible filesystems, and files on a standard filesystem, based
   * on the URL parameter. Fetching directories is only supported from Hadoop-compatible
   * filesystems.
   *
   * If `useCache` is true, first attempts to fetch the file to a local cache that's shared
   * across executors running the same application. `useCache` is used mainly for
   * the executors, and not in local mode.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
   *
   * If `shouldUntar` is true, it untars the given url if it is a tar.gz or tgz into `targetDir`.
   * This is a legacy behavior, and users should better use `spark.archives` configuration or
   * `SparkContext.addArchive`
   */
  def fetchFile(
      url: String,
      targetDir: File,
      conf: SparkConf,
      hadoopConf: Configuration,
      timestamp: Long,
      useCache: Boolean,
      shouldUntar: Boolean = true): File = {
    val fileName = decodeFileNameInURI(new URI(url))
    val targetFile = new File(targetDir, fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache", defaultValue = true)
    if (useCache && fetchCacheEnabled) {
      val cachedFileName = s"${url.hashCode}${timestamp}_cache"
      val lockFileName = s"${url.hashCode}${timestamp}_lock"
      // Set the cachedLocalDir for the first time and re-use it later
      if (cachedLocalDir.isEmpty) {
        this.synchronized {
          if (cachedLocalDir.isEmpty) {
            cachedLocalDir = getLocalDir(conf)
          }
        }
      }
      val localDir = new File(cachedLocalDir)
      val lockFile = new File(localDir, lockFileName)
      val lockFileChannel = new RandomAccessFile(lockFile, "rw").getChannel()
      // Only one executor entry.
      // The FileLock is only used to control synchronization for executors download file,
      // it's always safe regardless of lock type (mandatory or advisory).
      val lock = lockFileChannel.lock()
      val cachedFile = new File(localDir, cachedFileName)
      try {
        if (!cachedFile.exists()) {
          doFetchFile(url, localDir, cachedFileName, conf, hadoopConf)
        }
      } finally {
        lock.release()
        lockFileChannel.close()
      }
      copyFile(
        url,
        cachedFile,
        targetFile,
        conf.getBoolean("spark.files.overwrite", false)
      )
    } else {
      doFetchFile(url, targetDir, fileName, conf, hadoopConf)
    }

    if (shouldUntar) {
      // Decompress the file if it's a .tar or .tar.gz
      if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
        logWarning(
          "Untarring behavior will be deprecated at spark.files and " +
            "SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive " +
            "instead.")
        logInfo(log"Untarring ${MDC(FILE_NAME, fileName)}")
        executeAndGetOutput(Seq("tar", "-xzf", fileName), targetDir)
      } else if (fileName.endsWith(".tar")) {
        logWarning(
          "Untarring behavior will be deprecated at spark.files and " +
            "SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive " +
            "instead.")
        logInfo(log"Untarring ${MDC(FILE_NAME, fileName)}")
        executeAndGetOutput(Seq("tar", "-xf", fileName), targetDir)
      }
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")

    // Windows does not grant read permission by default to non-admin users
    // Add read permission to owner explicitly
    if (isWindows) {
      FileUtil.chmod(targetFile.getAbsolutePath, "u+r")
    }

    targetFile
  }

  /**
   * Unpacks an archive file into the specified directory. It expects .jar, .zip, .tar.gz, .tgz
   * and .tar files. This behaves same as Hadoop's archive in distributed cache. This method is
   * basically copied from `org.apache.hadoop.yarn.util.FSDownload.unpack`.
   */
  def unpack(source: File, dest: File): Unit = {
    if (!source.exists()) {
      throw new FileNotFoundException(source.getAbsolutePath)
    }
    val lowerSrc = StringUtils.toLowerCase(source.getName)
    if (lowerSrc.endsWith(".jar")) {
      RunJar.unJar(source, dest, RunJar.MATCH_ANY)
    } else if (lowerSrc.endsWith(".zip")) {
      // After issue HADOOP-18145, unzip could keep file permissions.
      FileUtil.unZip(source, dest)
    } else if (lowerSrc.endsWith(".tar.gz") || lowerSrc.endsWith(".tgz")) {
      FileUtil.unTar(source, dest)
    } else if (lowerSrc.endsWith(".tar")) {
      // TODO(SPARK-38632): should keep file permissions. Java implementation doesn't.
      unTarUsingJava(source, dest)
    } else {
      logWarning(log"Cannot unpack ${MDC(LogKeys.FILE_NAME, source)}, " +
        log"just copying it to ${MDC(FILE_NAME2, dest)}.")
      copyRecursive(source, dest)
    }
  }

  /**
   * The method below was copied from `FileUtil.unTar` but uses Java-based implementation
   * to work around a security issue, see also SPARK-38631.
   */
  private def unTarUsingJava(source: File, dest: File): Unit = {
    if (!dest.mkdirs && !dest.isDirectory) {
      throw new IOException(s"Mkdirs failed to create $dest")
    } else {
      try {
        // Should not fail because all Hadoop 2.1+ (from HADOOP-9264)
        // have 'unTarUsingJava'.
        val mth = classOf[FileUtil].getDeclaredMethod(
          "unTarUsingJava", classOf[File], classOf[File], classOf[Boolean])
        mth.setAccessible(true)
        mth.invoke(null, source, dest, java.lang.Boolean.FALSE)
      } catch {
        // Re-throw the original exception.
        case e: java.lang.reflect.InvocationTargetException if e.getCause != null =>
          throw e.getCause
      }
    }
  }

  /** Records the duration of running `body`. */
  def timeTakenMs[T](body: => T): (T, Long) = {
    val startTime = System.nanoTime()
    val result = body
    val endTime = System.nanoTime()
    (result, math.max(NANOSECONDS.toMillis(endTime - startTime), 0))
  }

  /**
   * Download `in` to `tempFile`, then move it to `destFile`.
   *
   * If `destFile` already exists:
   *   - no-op if its contents equal those of `sourceFile`,
   *   - throw an exception if `fileOverwrite` is false,
   *   - attempt to overwrite it otherwise.
   *
   * @param url URL that `sourceFile` originated from, for logging purposes.
   * @param in InputStream to download.
   * @param destFile File path to move `tempFile` to.
   * @param fileOverwrite Whether to delete/overwrite an existing `destFile` that does not match
   *                      `sourceFile`
   */
  private def downloadFile(
      url: String,
      in: InputStream,
      destFile: File,
      fileOverwrite: Boolean): Unit = {
    val tempFile = File.createTempFile("fetchFileTemp", null,
      new File(destFile.getParentFile.getAbsolutePath))
    logInfo(log"Fetching ${MDC(LogKeys.URL, url)} to ${MDC(FILE_ABSOLUTE_PATH, tempFile)}")

    try {
      val out = new FileOutputStream(tempFile)
      Utils.copyStream(in, out, closeStreams = true)
      copyFile(url, tempFile, destFile, fileOverwrite, removeSourceFile = true)
    } finally {
      // Catch-all for the couple of cases where for some reason we didn't move `tempFile` to
      // `destFile`.
      if (tempFile.exists()) {
        tempFile.delete()
      }
    }
  }

  /**
   * Copy `sourceFile` to `destFile`.
   *
   * If `destFile` already exists:
   *   - no-op if its contents equal those of `sourceFile`,
   *   - throw an exception if `fileOverwrite` is false,
   *   - attempt to overwrite it otherwise.
   *
   * @param url URL that `sourceFile` originated from, for logging purposes.
   * @param sourceFile File path to copy/move from.
   * @param destFile File path to copy/move to.
   * @param fileOverwrite Whether to delete/overwrite an existing `destFile` that does not match
   *                      `sourceFile`
   * @param removeSourceFile Whether to remove `sourceFile` after / as part of moving/copying it to
   *                         `destFile`.
   */
  private def copyFile(
      url: String,
      sourceFile: File,
      destFile: File,
      fileOverwrite: Boolean,
      removeSourceFile: Boolean = false): Unit = {

    if (destFile.exists) {
      if (!filesEqualRecursive(sourceFile, destFile)) {
        if (fileOverwrite) {
          logInfo(
            log"File ${MDC(DESTINATION_PATH, destFile)} exists and does not match contents of" +
              log" ${MDC(LogKeys.URL, url)}, replacing it with ${MDC(LogKeys.URL2, url)}"
          )
          if (!destFile.delete()) {
            throw new SparkException(
              "Failed to delete %s while attempting to overwrite it with %s".format(
                destFile.getAbsolutePath,
                sourceFile.getAbsolutePath
              )
            )
          }
        } else {
          throw new SparkException(
            s"File $destFile exists and does not match contents of $url")
        }
      } else {
        // Do nothing if the file contents are the same, i.e. this file has been copied
        // previously.
        logInfo(
          log"${MDC(SOURCE_PATH, sourceFile.getAbsolutePath)} has been previously" +
            log" copied to ${MDC(DESTINATION_PATH, destFile.getAbsolutePath)}"
        )
        return
      }
    }

    // The file does not exist in the target directory. Copy or move it there.
    if (removeSourceFile) {
      Files.move(sourceFile.toPath, destFile.toPath)
    } else {
      logInfo(log"Copying ${MDC(SOURCE_PATH, sourceFile.getAbsolutePath)}" +
        log" to ${MDC(DESTINATION_PATH, destFile.getAbsolutePath)}")
      copyRecursive(sourceFile, destFile)
    }
  }

  private def filesEqualRecursive(file1: File, file2: File): Boolean = {
    if (file1.isDirectory && file2.isDirectory) {
      val subfiles1 = file1.listFiles()
      val subfiles2 = file2.listFiles()
      if (subfiles1.size != subfiles2.size) {
        return false
      }
      subfiles1.sortBy(_.getName).zip(subfiles2.sortBy(_.getName)).forall {
        case (f1, f2) => filesEqualRecursive(f1, f2)
      }
    } else if (file1.isFile && file2.isFile) {
      GFiles.equal(file1, file2)
    } else {
      false
    }
  }

  private def copyRecursive(source: File, dest: File): Unit = {
    if (source.isDirectory) {
      if (!dest.mkdir()) {
        throw new IOException(s"Failed to create directory ${dest.getPath}")
      }
      val subfiles = source.listFiles()
      subfiles.foreach(f => copyRecursive(f, new File(dest, f.getName)))
    } else {
      Files.copy(source.toPath, dest.toPath)
    }
  }

  /**
   * Download a file or directory to target directory. Supports fetching the file in a variety of
   * ways, including HTTP, Hadoop-compatible filesystems, and files on a standard filesystem, based
   * on the URL parameter. Fetching directories is only supported from Hadoop-compatible
   * filesystems.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
   */
  def doFetchFile(
      url: String,
      targetDir: File,
      filename: String,
      conf: SparkConf,
      hadoopConf: Configuration): File = {
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", defaultValue = false)
    Option(uri.getScheme).getOrElse("file") match {
      case "spark" =>
        if (SparkEnv.get == null) {
          throw new IllegalStateException(
            "Cannot retrieve files with 'spark' scheme without an active SparkEnv.")
        }
        val source = SparkEnv.get.rpcEnv.openChannel(url)
        val is = Channels.newInputStream(source)
        downloadFile(url, is, targetFile, fileOverwrite)
      case "http" | "https" | "ftp" =>
        val uc = new URI(url).toURL.openConnection()
        val timeoutMs =
          conf.getTimeAsSeconds("spark.files.fetchTimeout", "60s").toInt * 1000
        uc.setConnectTimeout(timeoutMs)
        uc.setReadTimeout(timeoutMs)
        uc.connect()
        val in = uc.getInputStream()
        downloadFile(url, in, targetFile, fileOverwrite)
      case "file" =>
        // In the case of a local file, copy the local file to the target directory.
        // Note the difference between uri vs url.
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(uri.getPath)
        copyFile(url, sourceFile, targetFile, fileOverwrite)
      case _ =>
        val fs = getHadoopFileSystem(uri, hadoopConf)
        val path = new Path(uri)
        fetchHcfsFile(path, targetDir, fs, conf, hadoopConf, fileOverwrite,
                      filename = Some(filename))
    }

    targetFile
  }

  /**
   * Fetch a file or directory from a Hadoop-compatible filesystem.
   *
   * Visible for testing
   */
  private[spark] def fetchHcfsFile(
      path: Path,
      targetDir: File,
      fs: FileSystem,
      conf: SparkConf,
      hadoopConf: Configuration,
      fileOverwrite: Boolean,
      filename: Option[String] = None): Unit = {
    if (!targetDir.exists() && !targetDir.mkdir()) {
      throw new IOException(s"Failed to create directory ${targetDir.getPath}")
    }
    val dest = new File(targetDir, filename.getOrElse(path.getName))
    if (fs.getFileStatus(path).isFile) {
      val in = fs.open(path)
      try {
        downloadFile(path.toString, in, dest, fileOverwrite)
      } finally {
        in.close()
      }
    } else {
      fs.listStatus(path).foreach { fileStatus =>
        fetchHcfsFile(fileStatus.getPath(), dest, fs, conf, hadoopConf, fileOverwrite)
      }
    }
  }

  /**
   * Validate that a given URI is actually a valid URL as well.
   * @param uri The URI to validate
   */
  @throws[MalformedURLException]("when the URI is an invalid URL")
  def validateURL(uri: URI): Unit = {
    Option(uri.getScheme).getOrElse("file") match {
      case "http" | "https" | "ftp" =>
        try {
          uri.toURL
        } catch {
          case e: MalformedURLException =>
            val ex = new MalformedURLException(s"URI (${uri.toString}) is not a valid URL.")
            ex.initCause(e)
            throw ex
        }
      case _ => // will not be turned into a URL anyway
    }
  }

  /**
   * Get the path of a temporary directory.  Spark's local directories can be configured through
   * multiple settings, which are used with the following precedence:
   *
   *   - If called from inside of a YARN container, this will return a directory chosen by YARN.
   *   - If the SPARK_LOCAL_DIRS environment variable is set, this will return a directory from it.
   *   - Otherwise, if the spark.local.dir is set, this will return a directory from it.
   *   - Otherwise, this will return java.io.tmpdir.
   *
   * Some of these configuration options might be lists of multiple paths, but this method will
   * always return a single directory. The return directory is chosen randomly from the array
   * of directories it gets from getOrCreateLocalRootDirs.
   */
  def getLocalDir(conf: SparkConf): String = {
    val localRootDirs = getOrCreateLocalRootDirs(conf)
    if (localRootDirs.isEmpty) {
      val configuredLocalDirs = getConfiguredLocalDirs(conf)
      throw new IOException(
        s"Failed to get a temp directory under [${configuredLocalDirs.mkString(",")}].")
    } else {
      localRootDirs(scala.util.Random.nextInt(localRootDirs.length))
    }
  }

  private[spark] def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.
    conf.getenv("CONTAINER_ID") != null
  }

  /**
   * Returns if the current codes are running in a Spark task, e.g., in executors.
   */
  def isInRunningSparkTask: Boolean = TaskContext.get() != null

  /**
   * Gets or creates the directories listed in spark.local.dir or SPARK_LOCAL_DIRS,
   * and returns only the directories that exist / could be created.
   *
   * If no directories could be created, this will return an empty list.
   *
   * This method will cache the local directories for the application when it's first invoked.
   * So calling it multiple times with a different configuration will always return the same
   * set of directories.
   */
  private[spark] def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    if (localRootDirs == null) {
      this.synchronized {
        if (localRootDirs == null) {
          localRootDirs = getOrCreateLocalRootDirsImpl(conf)
        }
      }
    }
    localRootDirs
  }

  /**
   * Return the configured local directories where Spark can write files. This
   * method does not create any directories on its own, it only encapsulates the
   * logic of locating the local directories according to deployment mode.
   */
  def getConfiguredLocalDirs(conf: SparkConf): Array[String] = {
    if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available. Note this assumes that Yarn has
      // created the directories already, and that they are secured so that only the
      // user has access to them.
      randomizeInPlace(getYarnLocalDirs(conf).split(","))
    } else if (conf.getenv("SPARK_EXECUTOR_DIRS") != null) {
      conf.getenv("SPARK_EXECUTOR_DIRS").split(File.pathSeparator)
    } else if (conf.getenv("SPARK_LOCAL_DIRS") != null) {
      conf.getenv("SPARK_LOCAL_DIRS").split(",")
    } else {
      // In non-Yarn mode (or for the driver in yarn-client mode), we cannot trust the user
      // configuration to point to a secure directory. So create a subdirectory with restricted
      // permissions under each listed directory.
      conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")).split(",")
    }
  }

  private def getOrCreateLocalRootDirsImpl(conf: SparkConf): Array[String] = {
    val configuredLocalDirs = getConfiguredLocalDirs(conf)
    val uris = configuredLocalDirs.filter { root =>
      // Here, we guess if the given value is a URI at its best - check if scheme is set.
      Try(new URI(root).getScheme != null).getOrElse(false)
    }
    if (uris.nonEmpty) {
      logWarning(
        log"The configured local directories are not expected to be URIs; " +
          log"however, got suspicious values [" +
          log"${MDC(LogKeys.URIS, uris.mkString(", "))}]. " +
          log"Please check your configured local directories.")
    }

    configuredLocalDirs.flatMap { root =>
      try {
        val rootDir = new File(root)
        if (rootDir.exists || rootDir.mkdirs()) {
          val dir = createTempDir(root)
          chmod700(dir)
          Some(dir.getAbsolutePath)
        } else {
          logError(log"Failed to create dir in ${MDC(PATH, root)}. Ignoring this directory.")

          None
        }
      } catch {
        case e: IOException =>
          logError(
            log"Failed to create local root dir in ${MDC(PATH, root)}. Ignoring this directory.")
          None
      }
    }
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(conf: SparkConf): String = {
    val localDirs = Option(conf.getenv("LOCAL_DIRS")).getOrElse("")

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }

  /** Used by unit tests. Do not call from other places. */
  private[spark] def clearLocalRootDirs(): Unit = {
    localRootDirs = null
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassTag](seq: IterableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.iterator.toArray).toImmutableArraySeq
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i + 1)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  /**
   * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
   * Note, this is typically not used from within core spark.
   */
  private lazy val localIpAddress: InetAddress = findLocalInetAddress()

  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning(log"Your hostname, ${MDC(HOST, InetAddress.getLocalHost.getHostName)}, " +
              log"resolves to a loopback address: ${MDC(HOST_PORT, address.getHostAddress)}; " +
              log"using ${MDC(HOST_PORT2, strippedAddress.getHostAddress)} instead (on interface " +
              log"${MDC(NETWORK_IF, ni.getName)})")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning(log"Your hostname, ${MDC(HOST, InetAddress.getLocalHost.getHostName)}, " +
          log"resolves to a loopback address: ${MDC(HOST_PORT, address.getHostAddress)}, " +
          log"but we couldn't find any external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  private var customHostname: Option[String] = sys.env.get("SPARK_LOCAL_HOSTNAME")

  /**
   * Allow setting a custom host name
   */
  def setCustomHostname(hostname: String): Unit = {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's FQDN.
   */
  def localCanonicalHostName(): String = {
    addBracketsIfNeeded(customHostname.getOrElse(localIpAddress.getCanonicalHostName))
  }

  /**
   * Get the local machine's hostname.
   * In case of IPv6, getHostAddress may return '0:0:0:0:0:0:0:1'.
   */
  def localHostName(): String = {
    addBracketsIfNeeded(customHostname.getOrElse(localIpAddress.getHostAddress))
  }

  /**
   * Get the local machine's URI.
   */
  def localHostNameForURI(): String = {
    addBracketsIfNeeded(customHostname.getOrElse(InetAddresses.toUriString(localIpAddress)))
  }

  private[spark] def addBracketsIfNeeded(addr: String): String = {
    if (addr.contains(":") && !addr.contains("[")) {
      "[" + addr + "]"
    } else {
      addr
    }
  }

  /**
   * Normalize IPv6 IPs and no-op on all other hosts.
   */
  private[spark] def normalizeIpIfNeeded(host: String): String = {
    // Is this a v6 address. We ask users to add [] around v6 addresses as strs but
    // there not always there. If it's just 0-9 and : and [] we treat it as a v6 address.
    // This means some invalid addresses are treated as v6 addresses, but since they are
    // not valid hostnames it doesn't matter.
    // See https://www.rfc-editor.org/rfc/rfc1123#page-13 for context around valid hostnames.
    val addressRe = """^\[{0,1}([0-9:]+?:[0-9]*)\]{0,1}$""".r
    host match {
      case addressRe(unbracketed) =>
        addBracketsIfNeeded(InetAddresses.toAddrString(InetAddresses.forString(unbracketed)))
      case _ =>
        host
    }
  }

  /**
   * Checks if the host contains only valid hostname/ip without port
   * NOTE: Incase of IPV6 ip it should be enclosed inside []
   */
  def checkHost(host: String): Unit = {
    if (host != null && host.split(":").length > 2) {
      assert(host.startsWith("[") && host.endsWith("]"),
        s"Expected hostname or IPv6 IP enclosed in [] but got $host")
    } else {
      assert(host != null && host.indexOf(':') == -1, s"Expected hostname or IP but got $host")
    }
  }

  def checkHostPort(hostPort: String): Unit = {
    if (hostPort != null && hostPort.split(":").length > 2) {
      assert(hostPort != null && hostPort.indexOf("]:") != -1,
        s"Expected host and port but got $hostPort")
    } else {
      assert(hostPort != null && hostPort.indexOf(':') != -1,
        s"Expected host and port but got $hostPort")
    }
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String, Int) = {
    // Check cache first.
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    def setDefaultPortValue: (String, Int) = {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      retval
    }
    // checks if the hostport contains IPV6 ip and parses the host, port
    if (hostPort != null && hostPort.split(":").length > 2) {
      val index: Int = hostPort.lastIndexOf("]:")
      if (-1 == index) {
        return setDefaultPortValue
      }
      val port = hostPort.substring(index + 2).trim()
      val retval = (hostPort.substring(0, index + 1).trim(), if (port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort, retval)
    } else {
      val index: Int = hostPort.lastIndexOf(':')
      if (-1 == index) {
        return setDefaultPortValue
      }
      val port = hostPort.substring(index + 1).trim()
      val retval = (hostPort.substring(0, index).trim(), if (port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort, retval)
    }

    hostPortParseResults.get(hostPort)
  }

  /**
   * Return the string to tell how long has passed in milliseconds.
   * @param startTimeNs - a timestamp in nanoseconds returned by `System.nanoTime`.
   */
  def getUsedTimeNs(startTimeNs: Long): String = {
    s"${TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)} ms"
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  override def deleteRecursively(file: File): Unit = {
    super.deleteRecursively(file)
    if (file != null) {
      ShutdownHookManager.removeShutdownDeleteDir(file)
    }
  }

  /**
   * Determines if a directory contains any files newer than cutoff seconds.
   *
   * @param dir must be the path to a directory, or IllegalArgumentException is thrown
   * @param cutoff measured in seconds. Returns true if there are any files or directories in the
   *               given directory whose last modified time is later than this many seconds ago
   */
  def doesDirectoryContainAnyNewFiles(dir: File, cutoff: Long): Boolean = {
    if (!dir.isDirectory) {
      throw new IllegalArgumentException(s"$dir is not a directory!")
    }
    val filesAndDirs = dir.listFiles()
    val cutoffTimeInMillis = System.currentTimeMillis - (cutoff * 1000)

    filesAndDirs.exists(_.lastModified() > cutoffTimeInMillis) ||
    filesAndDirs.filter(_.isDirectory).exists(
      subdir => doesDirectoryContainAnyNewFiles(subdir, cutoff)
    )
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in bytes.
   */
  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m, 500g) to gibibytes for internal use.
   *
   * If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  def byteStringAsGb(str: String): Long = {
    JavaUtils.byteStringAsGb(str)
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of mebibytes.
   */
  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MiB, because when no units are specified the unit
    // is assumed to be bytes
    (JavaUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }

  private[this] val siByteSizes =
    Array(1L << 60, 1L << 50, 1L << 40, 1L << 30, 1L << 20, 1L << 10, 1)
  private[this] val siByteSuffixes =
    Array("EiB", "PiB", "TiB", "GiB", "MiB", "KiB", "B")
  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = {
    var i = 0
    while (i < siByteSizes.length - 1 && size < 2 * siByteSizes(i)) i += 1
    "%.1f %s".formatLocal(Locale.US, size.toDouble / siByteSizes(i), siByteSuffixes(i))
  }

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    if (size.isValidLong) {
      // Common case, most sizes fit in 64 bits and all ops on BigInt are order(s) of magnitude
      // slower than Long/Double.
      bytesToString(size.toLong)
    } else if (size < BigInt(2L << 10) * EiB) {
      "%.1f EiB".formatLocal(Locale.US, BigDecimal(size) / EiB)
    } else {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    }
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute
    val locale = Locale.US

    ms match {
      case t if t < second =>
        "%d ms".formatLocal(locale, t)
      case t if t < minute =>
        "%.1f s".formatLocal(locale, t.toFloat / second)
      case t if t < hour =>
        "%.1f m".formatLocal(locale, t.toFloat / minute)
      case t =>
        "%.2f h".formatLocal(locale, t.toFloat / hour)
    }
  }

  /**
   * Convert a quantity in megabytes to a human-readable string such as "4.0 MiB".
   */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Execute a command and return the process running the command.
   */
  def executeCommand(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): Process = {
    val builder = new ProcessBuilder(command: _*).directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    if (redirectStderr) {
      val threadName = "redirect stderr for command " + command(0)
      def log(s: String): Unit = logInfo(log"${MDC(LINE, s)}")
      processStreamByLine(threadName, process.getErrorStream, log)
    }
    process
  }

  /**
   * Execute a command and get its output, throwing an exception if it yields a code other than 0.
   */
  def executeAndGetOutput(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): String = {
    val process = executeCommand(command, workingDir, extraEnvironment, redirectStderr)
    val output = new StringBuilder
    val threadName = "read stdout for " + command(0)
    def appendToOutput(s: String): Unit = output.append(s).append("\n")
    val stdoutThread = processStreamByLine(threadName, process.getInputStream, appendToOutput)
    val exitCode = process.waitFor()
    stdoutThread.join()   // Wait for it to finish reading output
    if (exitCode != 0) {
      logError(log"Process ${MDC(COMMAND, command)} exited with code " +
        log"${MDC(EXIT_CODE, exitCode)}: ${MDC(COMMAND_OUTPUT, output)}")
      throw new SparkException(s"Process $command exited with code $exitCode")
    }
    output.toString
  }

  /**
   * Return and start a daemon thread that processes the content of the input stream line by line.
   */
  def processStreamByLine(
      threadName: String,
      inputStream: InputStream,
      processLine: String => Unit): Thread = {
    val t = new Thread(threadName) {
      override def run(): Unit = {
        for (line <- Source.fromInputStream(inputStream).getLines()) {
          processLine(line)
        }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  /**
   * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
   * default UncaughtExceptionHandler
   *
   * NOTE: This method is to be called by the spark-started JVM process.
   */
  def tryOrExit(block: => Unit): Unit = {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => sparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /**
   * Execute a block of code that evaluates to Unit, stop SparkContext if there is any uncaught
   * exception
   *
   * NOTE: This method is to be called by the driver-side components to avoid stopping the
   * user-started JVM process completely; in contrast, tryOrExit is to be called in the
   * spark-started JVM process .
   */
  def tryOrStopSparkContext(sc: SparkContext)(block: => Unit): Unit = {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if (sc != null) {
          logError(log"uncaught error in thread ${MDC(THREAD_NAME, currentThreadName)}, " +
              log"stopping SparkContext", t)
          sc.stopInNewThread()
        }
        if (!NonFatal(t)) {
          logError(
            log"throw uncaught fatal error in thread ${MDC(THREAD_NAME, currentThreadName)}", t)
          throw t
        }
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit): Unit = {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(
          log"Uncaught exception in thread ${MDC(THREAD_NAME, Thread.currentThread().getName)}", t)
    }
  }

  /**
   * Execute a block of code and call the failure callbacks in the catch block. If exceptions occur
   * in either the catch or the finally block, they are appended to the list of suppressed
   * exceptions in original exception which is then rethrown.
   *
   * This is primarily an issue with `catch { abort() }` or `finally { out.close() }` blocks,
   * where the abort/close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `abort` or `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinallyAndFailureCallbacks[T](block: => T)
      (catchBlock: => Unit = (), finallyBlock: => Unit = ()): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case cause: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = cause
        try {
          logError("Aborting task", originalThrowable)
          if (TaskContext.get() != null) {
            TaskContext.get().markTaskFailed(originalThrowable)
          }
          catchBlock
        } catch {
          case t: Throwable =>
            if (originalThrowable != t) {
              originalThrowable.addSuppressed(t)
              logWarning(log"Suppressing exception in catch: ${MDC(ERROR, t.getMessage)}", t)
            }
        }
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(log"Suppressing exception in finally: ${MDC(ERROR, t.getMessage)}", t)
          throw originalThrowable
      }
    }
  }

  val TRY_WITH_CALLER_STACKTRACE_FULL_STACKTRACE =
    "Full stacktrace of original doTryWithCallerStacktrace caller"

  val TRY_WITH_CALLER_STACKTRACE_TRY_STACKTRACE =
    "Stacktrace under doTryWithCallerStacktrace"

  /**
   * Use Try with stacktrace substitution for the caller retrieving the error.
   *
   * Normally in case of failure, the exception would have the stacktrace of the caller that
   * originally called doTryWithCallerStacktrace. However, we want to replace the part above
   * this function with the stacktrace of the caller who calls getTryWithCallerStacktrace.
   * So here we save the part of the stacktrace below doTryWithCallerStacktrace, and
   * getTryWithCallerStacktrace will stitch it with the new stack trace of the caller.
   * The full original stack trace is kept in ex.getSuppressed.
   *
   * @param f Code block to be wrapped in Try
   * @return Try with Success or Failure of the code block. Use with getTryWithCallerStacktrace.
   */
  def doTryWithCallerStacktrace[T](f: => T): Try[T] = {
    val t = Try {
      f
    }
    t match {
      case Failure(ex) =>
        // Note: we remove the common suffix instead of e.g. finding the call to this function, to
        // account for recursive calls with multiple doTryWithCallerStacktrace on the stack trace.
        val origStackTrace = ex.getStackTrace
        val currentStackTrace = Thread.currentThread().getStackTrace
        val commonSuffixLen = origStackTrace.reverse.zip(currentStackTrace.reverse).takeWhile {
          case (exElem, currentElem) => exElem == currentElem
        }.length
        val belowEx = new Exception(TRY_WITH_CALLER_STACKTRACE_TRY_STACKTRACE)
        belowEx.setStackTrace(origStackTrace.dropRight(commonSuffixLen))
        ex.addSuppressed(belowEx)

        // keep the full original stack trace in a suppressed exception.
        val fullEx = new Exception(TRY_WITH_CALLER_STACKTRACE_FULL_STACKTRACE)
        fullEx.setStackTrace(origStackTrace)
        ex.addSuppressed(fullEx)
      case Success(_) => // nothing
    }
    t
  }

  /**
   * Retrieve the result of Try that was created by doTryWithCallerStacktrace.
   *
   * In case of failure, the resulting exception has a stack trace that combines the stack trace
   * below the original doTryWithCallerStacktrace which triggered it, with the caller stack trace
   * of the current caller of getTryWithCallerStacktrace.
   *
   * Full stack trace of the original doTryWithCallerStacktrace caller can be retrieved with
   * ```
   * ex.getSuppressed.find { e =>
   * e.getMessage == Utils.TRY_WITH_CALLER_STACKTRACE_FULL_STACKTRACE
   * }
   * ```
   *
   *
   * @param t Try from doTryWithCallerStacktrace
   * @return Result of the Try or rethrows the failure exception with modified stacktrace.
   */
  def getTryWithCallerStacktrace[T](t: Try[T]): T = t match {
    case Failure(ex) =>
      val belowStacktrace = ex.getSuppressed.find { e =>
        // added in doTryWithCallerStacktrace
        e.getMessage == TRY_WITH_CALLER_STACKTRACE_TRY_STACKTRACE
      }.getOrElse {
        // If we don't have the expected stacktrace information, just rethrow
        throw ex
      }.getStackTrace
      // We are modifying and throwing the original exception. It would be better if we could
      // return a copy, but we can't easily clone it and preserve. If this is accessed from
      // multiple threads that then look at the stack trace, this could break.
      ex.setStackTrace(belowStacktrace ++ Thread.currentThread().getStackTrace.drop(1))
      throw ex
    case Success(s) => s
  }

  // A regular expression to match classes of the internal Spark API's
  // that we want to skip when finding the call site of a method.
  private val SPARK_CORE_CLASS_REGEX =
    """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  private val SPARK_SQL_CLASS_REGEX = """^org\.apache\.spark\.sql.*""".r

  /** Default filtering function for finding call sites using `getCallSite`. */
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined ||
      SPARK_SQL_CLASS_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkClass || isScalaClass
  }

  /**
   * When called inside a class in the spark package, returns the name of the user code class
   * (outside the spark package) that called into Spark, as well as which Spark method they called.
   * This is used, for example, to tell users where in their code each RDD got created.
   *
   * @param skipClass Function that is used to exclude non-user-code classes.
   */
  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction): CallSite = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    val callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
      // When running under some profilers, the current stack trace might contain some bogus
      // frames. This is intended to ensure that we don't crash in these situations by
      // ignoring any frames that we can't examine.
      if (ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if (ste.getMethodName == "<init>") {
              // Spark method is a constructor; get its class name
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
            } else {
              ste.getMethodName
            }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            if (ste.getFileName != null) {
              firstUserFile = ste.getFileName
              if (ste.getLineNumber >= 0) {
                firstUserLine = ste.getLineNumber
              }
            }
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.
        "Spark JDBC Server Query"
      } else {
        s"$lastSparkMethod at $firstUserFile:$firstUserLine"
      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    CallSite(shortForm, longForm)
  }

  private var compressedLogFileLengthCache: LoadingCache[String, java.lang.Long] = null
  private def getCompressedLogFileLengthCache(
      sparkConf: SparkConf): LoadingCache[String, java.lang.Long] = this.synchronized {
    if (compressedLogFileLengthCache == null) {
      val compressedLogFileLengthCacheSize = sparkConf.get(
        UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF)
      compressedLogFileLengthCache = CacheBuilder.newBuilder()
        .maximumSize(compressedLogFileLengthCacheSize)
        .build[String, java.lang.Long](new CacheLoader[String, java.lang.Long]() {
        override def load(path: String): java.lang.Long = {
          Utils.getCompressedFileLength(new File(path))
        }
      })
    }
    compressedLogFileLengthCache
  }

  /**
   * Return the file length, if the file is compressed it returns the uncompressed file length.
   * It also caches the uncompressed file size to avoid repeated decompression. The cache size is
   * read from workerConf.
   */
  def getFileLength(file: File, workConf: SparkConf): Long = {
    if (file.getName.endsWith(".gz")) {
      getCompressedLogFileLengthCache(workConf).get(file.getAbsolutePath)
    } else {
      file.length
    }
  }

  /** Return uncompressed file length of a compressed file. */
  private def getCompressedFileLength(file: File): Long = {
    var gzInputStream: GZIPInputStream = null
    try {
      // Uncompress .gz file to determine file size.
      var fileSize = 0L
      gzInputStream = new GZIPInputStream(new FileInputStream(file))
      val bufSize = 1024
      val buf = new Array[Byte](bufSize)
      var numBytes = ByteStreams.read(gzInputStream, buf, 0, bufSize)
      while (numBytes > 0) {
        fileSize += numBytes
        numBytes = ByteStreams.read(gzInputStream, buf, 0, bufSize)
      }
      fileSize
    } catch {
      case e: Throwable =>
        logError(log"Cannot get file length of ${MDC(PATH, file)}", e)
        throw e
    } finally {
      if (gzInputStream != null) {
        gzInputStream.close()
      }
    }
  }

  /** Return a string containing part of a file from byte 'start' to 'end'. */
  def offsetBytes(path: String, length: Long, start: Long, end: Long): String = {
    val file = new File(path)
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = if (path.endsWith(".gz")) {
      new GZIPInputStream(new FileInputStream(file))
    } else {
      new FileInputStream(file)
    }

    try {
      ByteStreams.skipFully(stream, effectiveStart)
      ByteStreams.readFully(stream, buff)
    } finally {
      stream.close()
    }
    Source.fromBytes(buff).mkString
  }

  /**
   * Return a string containing data across a set of files. The `startIndex`
   * and `endIndex` is based on the cumulative size of all the files take in
   * the given order. See figure below for more details.
   */
  def offsetBytes(files: Seq[File], fileLengths: Seq[Long], start: Long, end: Long): String = {
    assert(files.length == fileLengths.length)
    val startIndex = math.max(start, 0)
    val endIndex = math.min(end, fileLengths.sum)
    val fileToLength = CUtils.toMap(files, fileLengths)

    logDebug("Log files: \n" + fileToLength.mkString("\n"))

    val stringBuffer = new StringBuffer((endIndex - startIndex).toInt)
    var sum = 0L
    files.zip(fileLengths).foreach { case (file, fileLength) =>
      val startIndexOfFile = sum
      val endIndexOfFile = sum + fileToLength(file)
      logDebug(s"Processing file $file, " +
        s"with start index = $startIndexOfFile, end index = $endIndex")

      /*
                                      ____________
       range 1:                      |            |
                                     |   case A   |

       files:   |==== file 1 ====|====== file 2 ======|===== file 3 =====|

                     |   case B  .       case C       .    case D    |
       range 2:      |___________.____________________.______________|
       */

      if (startIndex <= startIndexOfFile  && endIndex >= endIndexOfFile) {
        // Case C: read the whole file
        stringBuffer.append(offsetBytes(file.getAbsolutePath, fileLength, 0, fileToLength(file)))
      } else if (startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        // Case A and B: read from [start of required range] to [end of file / end of range]
        val effectiveStartIndex = startIndex - startIndexOfFile
        val effectiveEndIndex = math.min(endIndex - startIndexOfFile, fileToLength(file))
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, fileLength, effectiveStartIndex, effectiveEndIndex))
      } else if (endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        // Case D: read from [start of file] to [end of require range]
        val effectiveStartIndex = math.max(startIndex - startIndexOfFile, 0)
        val effectiveEndIndex = endIndex - startIndexOfFile
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, fileLength, effectiveStartIndex, effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $file, string built is ${stringBuffer.toString}")
    }
    stringBuffer.toString
  }

  /**
   * Clone an object using a Spark serializer.
   */
  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T = {
    serializer.deserialize[T](serializer.serialize(value))
  }

  private def isSpace(c: Char): Boolean = {
    " \t\r\n".indexOf(c) != -1
  }

  /**
   * Split a string of potentially quoted arguments from the command line the way that a shell
   * would do it to determine arguments to a command. For example, if the string is 'a "b c" d',
   * then it would be parsed as three arguments: 'a', 'b c' and 'd'.
   */
  def splitCommandString(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var inWord = false
    var inSingleQuote = false
    var inDoubleQuote = false
    val curWord = new StringBuilder
    def endWord(): Unit = {
      buf += curWord.toString
      curWord.clear()
    }
    var i = 0
    while (i < s.length) {
      val nextChar = s.charAt(i)
      if (inDoubleQuote) {
        if (nextChar == '"') {
          inDoubleQuote = false
        } else if (nextChar == '\\') {
          if (i < s.length - 1) {
            // Append the next character directly, because only " and \ may be escaped in
            // double quotes after the shell's own expansion
            curWord.append(s.charAt(i + 1))
            i += 1
          }
        } else {
          curWord.append(nextChar)
        }
      } else if (inSingleQuote) {
        if (nextChar == '\'') {
          inSingleQuote = false
        } else {
          curWord.append(nextChar)
        }
        // Backslashes are not treated specially in single quotes
      } else if (nextChar == '"') {
        inWord = true
        inDoubleQuote = true
      } else if (nextChar == '\'') {
        inWord = true
        inSingleQuote = true
      } else if (!isSpace(nextChar)) {
        curWord.append(nextChar)
        inWord = true
      } else if (inWord && isSpace(nextChar)) {
        endWord()
        inWord = false
      }
      i += 1
    }
    if (inWord || inDoubleQuote || inSingleQuote) {
      endWord()
    }
    buf.toSeq
  }

 /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  // Handles idiosyncrasies with hash (add more as required)
  // This method should be kept in sync with
  // org.apache.spark.network.util.JavaUtils#nonNegativeHash().
  def nonNegativeHash(obj: AnyRef): Int = {

    // Required ?
    if (obj eq null) return 0

    val hash = obj.hashCode
    // math.abs fails for Int.MinValue
    val hashAbs = if (Int.MinValue != hash) math.abs(hash) else 0

    // Nothing else to guard against ?
    hashAbs
  }

  /**
   * Returns the system properties map that is thread-safe to iterator over. It gets the
   * properties which have been set explicitly, as well as those for which only a default value
   * has been defined.
   */
  def getSystemProperties: Map[String, String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
  }

  /**
   * Method executed for repeating a task for side effects.
   * Unlike a for comprehension, it permits JVM JIT optimization
   */
  def times(numIters: Int)(f: => Unit): Unit = {
    var i = 0
    while (i < numIters) {
      f
      i += 1
    }
  }

  /**
   * Timing method based on iterations that permit JVM JIT optimization.
   *
   * @param numIters number of iterations
   * @param f function to be executed. If prepare is not None, the running time of each call to f
   *          must be an order of magnitude longer than one nanosecond for accurate timing.
   * @param prepare function to be executed before each call to f. Its running time doesn't count.
   * @return the total time across all iterations (not counting preparation time) in nanoseconds.
   */
  def timeIt(numIters: Int)(f: => Unit, prepare: Option[() => Unit] = None): Long = {
    if (prepare.isEmpty) {
      val startNs = System.nanoTime()
      times(numIters)(f)
      System.nanoTime() - startNs
    } else {
      var i = 0
      var sum = 0L
      while (i < numIters) {
        prepare.get.apply()
        val startNs = System.nanoTime()
        f
        sum += System.nanoTime() - startNs
        i += 1
      }
      sum
    }
  }

  /**
   * Counts the number of elements of an iterator.
   */
  def getIteratorSize(iterator: Iterator[_]): Long = {
    if (iterator.knownSize >= 0) iterator.knownSize.toLong
    else {
      var count = 0L
      while (iterator.hasNext) {
        count += 1L
        iterator.next()
      }
      count
    }
  }

  /**
   * Generate a zipWithIndex iterator, avoid index value overflowing problem
   * in scala's zipWithIndex
   */
  def getIteratorZipWithIndex[T](iter: Iterator[T], startIndex: Long): Iterator[(T, Long)] = {
    new Iterator[(T, Long)] {
      require(startIndex >= 0, "startIndex should be >= 0.")
      var index: Long = startIndex - 1L
      def hasNext: Boolean = iter.hasNext
      def next(): (T, Long) = {
        index += 1L
        (iter.next(), index)
      }
    }
  }

  /**
   * Creates a symlink.
   *
   * @param src absolute path to the source
   * @param dst relative path for the destination
   */
  def symlink(src: File, dst: File): Unit = {
    if (!src.isAbsolute()) {
      throw new IOException("Source must be absolute")
    }
    if (dst.isAbsolute()) {
      throw new IOException("Destination must be relative")
    }
    Files.createSymbolicLink(dst.toPath, src.toPath)
  }

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
   */
  def getHadoopFileSystem(path: URI, conf: Configuration): FileSystem = {
    FileSystem.get(path, conf)
  }

  /**
   * Return a Hadoop FileSystem with the scheme encoded in the given path.
   */
  def getHadoopFileSystem(path: String, conf: Configuration): FileSystem = {
    getHadoopFileSystem(new URI(path), conf)
  }

  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * Whether the underlying operating system is Mac OS X.
   */
  val isMac = SystemUtils.IS_OS_MAC_OSX

  /**
   * Whether the underlying Java version is at least 21.
   */
  val isJavaVersionAtLeast21 = SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_21)

  /**
   * Whether the underlying operating system is Mac OS X and processor is Apple Silicon.
   */
  val isMacOnAppleSilicon = SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64")

  /**
   * Whether the underlying JVM prefer IPv6 addresses.
   */
  val preferIPv6 = "true".equals(System.getProperty("java.net.preferIPv6Addresses"))

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDrive = "([a-zA-Z])".r

  /**
   * Terminates a process waiting for at most the specified duration.
   *
   * @return the process exit value if it was successfully terminated, else None
   */
  def terminateProcess(process: Process, timeoutMs: Long): Option[Int] = {
    // Politely destroy first
    process.destroy()
    if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
      // Successful exit
      Option(process.exitValue())
    } else {
      try {
        process.destroyForcibly()
      } catch {
        case NonFatal(e) => logWarning("Exception when attempting to kill process", e)
      }
      // Wait, again, although this really should return almost immediately
      if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
        Option(process.exitValue())
      } else {
        logWarning("Timed out waiting to forcibly kill process")
        None
      }
    }
  }

  /**
   * Return the stderr of a process after waiting for the process to terminate.
   * If the process does not terminate within the specified timeout, return None.
   */
  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)
    if (terminated) {
      Some(Source.fromInputStream(process.getErrorStream).getLines().mkString("\n"))
    } else {
      None
    }
  }

  /**
   * Execute the given block, logging and re-throwing any uncaught exception.
   * This is particularly useful for wrapping code that runs in a thread, to ensure
   * that exceptions are printed, and to avoid having to catch Throwable.
   */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(
          log"Uncaught exception in thread ${MDC(THREAD_NAME, Thread.currentThread().getName)}", t)
        throw t
    }
  }

  /** Executes the given block in a Try, logging any uncaught exceptions. */
  def tryLog[T](f: => T): Try[T] = {
    try {
      val res = f
      scala.util.Success(res)
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(
          log"Uncaught exception in thread ${MDC(THREAD_NAME, Thread.currentThread().getName)}", t)
        scala.util.Failure(t)
    }
  }

  /** Returns true if the given exception was fatal. See docs for scala.util.control.NonFatal. */
  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) |
           _: InterruptedException |
           _: NotImplementedError |
           _: ControlThrowable |
           _: LinkageError =>
        false
      case _ =>
        true
    }
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").filter(_.trim.nonEmpty).map { p => Utils.resolveURI(p) }.mkString(",")
    }
  }

  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val uri = resolveURI(p)
        Option(uri.getScheme).getOrElse("file") match {
          case windowsDrive(d) if windows => false
          case "local" | "file" => false
          case _ => true
        }
      }
    }
  }

  /**
   * Load default Spark properties from the given file. If no file is provided,
   * use the common defaults file. This mutates state in the given SparkConf and
   * in this JVM's system properties if the config specified in the file is not
   * already set. Return the path of the properties file used.
   */
  def loadDefaultSparkProperties(conf: SparkConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("spark.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  /**
   * Implements the same logic as JDK `java.lang.String#trim` by removing leading and trailing
   * non-printable characters less or equal to '\u0020' (SPACE) but preserves natural line
   * delimiters according to [[java.util.Properties]] load method. The natural line delimiters are
   * removed by JDK during load. Therefore any remaining ones have been specifically provided and
   * escaped by the user, and must not be ignored
   *
   * @param str
   * @return the trimmed value of str
   */
  private[util] def trimExceptCRLF(str: String): String = {
    val nonSpaceOrNaturalLineDelimiter: Char => Boolean = { ch =>
      ch > ' ' || ch == '\r' || ch == '\n'
    }

    val firstPos = str.indexWhere(nonSpaceOrNaturalLineDelimiter)
    val lastPos = str.lastIndexWhere(nonSpaceOrNaturalLineDelimiter)
    if (firstPos >= 0 && lastPos >= 0) {
      str.substring(firstPos, lastPos + 1)
    } else {
      ""
    }
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala
        .map { k => (k, trimExceptCRLF(properties.getProperty(k))) }
        .toMap

    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  /** Return the path of the default Spark properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("SPARK_CONF_DIR")
      .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      ""
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }

  private implicit class Lock(lock: LockInfo) {
    def lockString: String = {
      lock match {
        case monitor: MonitorInfo => s"Monitor(${monitor.toString})"
        case _ => s"Lock(${lock.toString})"
      }
    }
  }

  /** Return a thread dump of all threads' stacktraces.  Used to capture dumps for the web UI */
  def getThreadDump(): Array[ThreadStackTrace] = {
    // We need to filter out null values here because dumpAllThreads() may return null array
    // elements for threads that are dead / don't exist.
    val threadInfos = ManagementFactory.getThreadMXBean.dumpAllThreads(true, true).filter(_ != null)
    threadInfos.sortWith { case (threadTrace1, threadTrace2) =>
        val v1 = if (threadTrace1.getThreadName.contains("Executor task launch")) 1 else 0
        val v2 = if (threadTrace2.getThreadName.contains("Executor task launch")) 1 else 0
        if (v1 == v2) {
          val name1 = threadTrace1.getThreadName().toLowerCase(Locale.ROOT)
          val name2 = threadTrace2.getThreadName().toLowerCase(Locale.ROOT)
          val nameCmpRes = name1.compareTo(name2)
          if (nameCmpRes == 0) {
            threadTrace1.getThreadId < threadTrace2.getThreadId
          } else {
            nameCmpRes < 0
          }
        } else {
          v1 > v2
        }
    }.map(threadInfoToThreadStackTrace)
  }

  /** Return a heap dump. Used to capture dumps for the web UI */
  def getHeapHistogram(): Array[String] = {
    val pid = String.valueOf(ProcessHandle.current().pid())
    val jmap = System.getProperty("java.home") + "/bin/jmap"
    val builder = new ProcessBuilder(jmap, "-histo:live", pid)
    val p = builder.start()
    val rows = ArrayBuffer.empty[String]
    Utils.tryWithResource(new BufferedReader(new InputStreamReader(p.getInputStream()))) { r =>
      var line = ""
      while (line != null) {
        if (line.nonEmpty) rows += line
        line = r.readLine()
      }
    }
    rows.toArray
  }

  def getThreadDumpForThread(threadId: Long): Option[ThreadStackTrace] = {
    if (threadId <= 0) {
      None
    } else {
      // The Int.MaxValue here requests the entire untruncated stack trace of the thread:
      val threadInfo =
        Option(ManagementFactory.getThreadMXBean.getThreadInfo(threadId, Int.MaxValue))
      threadInfo.map(threadInfoToThreadStackTrace)
    }
  }

  private def threadInfoToThreadStackTrace(threadInfo: ThreadInfo): ThreadStackTrace = {
    val threadState = threadInfo.getThreadState
    val monitors = threadInfo.getLockedMonitors.map(m => m.getLockedStackDepth -> m.toString).toMap
    val stackTrace = StackTrace(threadInfo.getStackTrace.zipWithIndex.map { case (frame, idx) =>
      val locked = if (idx == 0 && threadInfo.getLockInfo != null) {
        threadState match {
          case Thread.State.BLOCKED =>
            s"\t-  blocked on ${threadInfo.getLockInfo}\n"
          case Thread.State.WAITING | Thread.State.TIMED_WAITING =>
            s"\t-  waiting on ${threadInfo.getLockInfo}\n"
          case _ => ""
        }
      } else ""
      val locking = monitors.get(idx).map(mi => s"\t-  locked $mi\n").getOrElse("")
      s"${frame.toString}\n$locked$locking"
    }.toImmutableArraySeq)

    val synchronizers = threadInfo.getLockedSynchronizers.map(_.toString)
    val monitorStrs = monitors.values.toSeq
    ThreadStackTrace(
      threadInfo.getThreadId,
      threadInfo.getThreadName,
      threadState,
      stackTrace,
      if (threadInfo.getLockOwnerId < 0) None else Some(threadInfo.getLockOwnerId),
      Option(threadInfo.getLockInfo).map(_.lockString).getOrElse(""),
      (synchronizers ++ monitorStrs).toImmutableArraySeq,
      synchronizers.toImmutableArraySeq,
      monitorStrs,
      Option(threadInfo.getLockName),
      Option(threadInfo.getLockOwnerName),
      threadInfo.isSuspended,
      threadInfo.isInNative,
      threadInfo.isDaemon,
      threadInfo.getPriority)
  }

  /**
   * Convert all spark properties set in the given SparkConf to a sequence of java options.
   */
  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean) = _ => true): Seq[String] = {
    conf.getAll
      .filter { case (k, _) => filterKey(k) }
      .map { case (k, v) => s"-D$k=$v" }
      .toImmutableArraySeq
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  def portMaxRetries(conf: SparkConf): Int = {
    val maxRetries = conf.getOption("spark.port.maxRetries").map(_.toInt)
    if (conf.contains(IS_TESTING)) {
      // Set a higher number of retries for tests...
      maxRetries.getOrElse(100)
    } else {
      maxRetries.getOrElse(16)
    }
  }

  /**
   * Returns the user port to try when trying to bind a service. Handles wrapping and skipping
   * privileged ports.
   */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }

  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Use a shared configuration for the maximum number of port retries.
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: SparkConf,
      serviceName: String = ""): (T, Int) = {
    startServiceOnPort(startPort, startService, portMaxRetries(conf), serviceName)
  }

  /**
   * Attempt to start a service on the given port, or fail after a number of attempts.
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param startPort The initial port to start the service on.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param maxRetries The maximum number of retries when binding to a port.
   * @param serviceName Name of the service.
   * @return (service: T, port: Int)
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      maxRetries: Int,
      serviceName: String): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        userPort(startPort, offset)
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(log"Successfully started service${MDC(SERVICE_NAME, serviceString)}" +
          log" on port ${MDC(PORT, port)}.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = if (startPort == 0) {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (on a random free port)! " +
                s"Consider explicitly setting the appropriate binding address for " +
                s"the service$serviceString (for example ${DRIVER_BIND_ADDRESS.key} " +
                s"for SparkDriver) to the correct binding address."
            } else {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                s"the appropriate port for the service$serviceString (for example spark.ui.port " +
                s"for SparkUI) to an available port or increasing spark.port.maxRetries."
            }
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          if (startPort == 0) {
            // As startPort 0 is for a random free port, it is most possibly binding address is
            // not correct.
            logWarning(log"Service${MDC(SERVICE_NAME, serviceString)} " +
              log"could not bind on a random free port. " +
              log"You may check whether configuring an appropriate binding address.")
          } else {
            logWarning(log"Service${MDC(SERVICE_NAME, serviceString)} " +
              log"could not bind on port ${MDC(PORT, tryPort)}. " +
              log"Attempting port ${MDC(PORT2, tryPort + 1)}.")
          }
      }
    }
    // Should never happen
    throw new SparkException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
   * Return whether the exception is caused by an address-port collision when binding.
   */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: MultiException =>
        e.getThrowables.asScala.exists(isBindCollision)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: IOException =>
        (e.getMessage != null && e.getMessage.startsWith("Failed to bind to address")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
   * configure a new log4j level
   */
  def setLogLevel(l: Level): Unit = {
    val (ctx, loggerConfig) = getLogContext
    loggerConfig.setLevel(l)
    ctx.updateLoggers()

    // Setting threshold to null as rootLevel will define log level for spark-shell
    Logging.sparkShellThresholdLevel = null
  }


  def setLogLevelIfNeeded(newLogLevel: String): Unit = {
    if (newLogLevel != Utils.getLogLevel) {
      Utils.setLogLevel(Level.toLevel(newLogLevel))
    }
  }

  private lazy val getLogContext: (LoggerContext, LoggerConfig) = {
    val ctx = LogManager.getContext(false).asInstanceOf[LoggerContext]
    (ctx, ctx.getConfiguration().getLoggerConfig(LogManager.ROOT_LOGGER_NAME))
  }

  /**
   * Get current log level
   */
  def getLogLevel: String = {
    val (_, loggerConfig) = getLogContext
    loggerConfig.getLevel.name
  }

  /**
   * Return the current system LD_LIBRARY_PATH name
   */
  def libraryPathEnvName: String = {
    if (isWindows) {
      "PATH"
    } else if (isMac) {
      "DYLD_LIBRARY_PATH"
    } else {
      "LD_LIBRARY_PATH"
    }
  }

  /**
   * Return the prefix of a command that appends the given library paths to the
   * system-specific library path environment variable. On Unix, for instance,
   * this returns the string LD_LIBRARY_PATH="path1:path2:$LD_LIBRARY_PATH".
   */
  def libraryPathEnvPrefix(libraryPaths: Seq[String]): String = {
    val libraryPathScriptVar = if (isWindows) {
      s"%${libraryPathEnvName}%"
    } else {
      "$" + libraryPathEnvName
    }
    val libraryPath = (libraryPaths :+ libraryPathScriptVar).mkString("\"",
      File.pathSeparator, "\"")
    val ampersand = if (Utils.isWindows) {
      " &"
    } else {
      ""
    }
    s"$libraryPathEnvName=$libraryPath$ampersand"
  }

  /**
   * Return the value of a config either through the SparkConf or the Hadoop configuration.
   * We Check whether the key is set in the SparkConf before look at any Hadoop configuration.
   * If the key is set in SparkConf, no matter whether it is running on YARN or not,
   * gets the value from SparkConf.
   * Only when the key is not set in SparkConf and running on YARN,
   * gets the value from Hadoop configuration.
   */
  def getSparkOrYarnConfig(conf: SparkConf, key: String, default: String): String = {
    if (conf.contains(key)) {
      conf.get(key, default)
    } else if (conf.get(SparkLauncher.SPARK_MASTER, null) == "yarn") {
      new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(conf)).get(key, default)
    } else {
      default
    }
  }

  /**
   * Return a pair of host and port extracted from the `sparkUrl`.
   *
   * A spark url (`spark://host:port`) is a special URI that its scheme is `spark` and only contains
   * host and port.
   *
   * @throws org.apache.spark.SparkException if sparkUrl is invalid.
   */
  @throws(classOf[SparkException])
  def extractHostPortFromSparkUrl(sparkUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "spark" ||
        host == null ||
        port < 0 ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new SparkException("Invalid master URL: " + sparkUrl)
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new SparkException("Invalid master URL: " + sparkUrl, e)
    }
  }

  /**
   * Returns the current user name. This is the currently logged in user, unless that's been
   * overridden by the `SPARK_USER` environment variable.
   */
  def getCurrentUserName(): String = {
    Option(System.getenv("SPARK_USER"))
      .getOrElse(UserGroupInformation.getCurrentUser().getShortUserName())
  }

  val EMPTY_USER_GROUPS = Set.empty[String]

  // Returns the groups to which the current user belongs.
  def getCurrentUserGroups(sparkConf: SparkConf, username: String): Set[String] = {
    val groupProviderClassName = sparkConf.get(USER_GROUPS_MAPPING)
    if (groupProviderClassName != "") {
      try {
        val groupMappingServiceProvider = classForName(groupProviderClassName).
          getConstructor().newInstance().
          asInstanceOf[org.apache.spark.security.GroupMappingServiceProvider]
        val currentUserGroups = groupMappingServiceProvider.getGroups(username)
        return currentUserGroups
      } catch {
        case e: Exception =>
          logError(log"Error getting groups for user=${MDC(USER_NAME, username)}", e)
      }
    }
    EMPTY_USER_GROUPS
  }

  /**
   * Split the comma delimited string of master URLs into a list.
   * For instance, "spark://abc,def" becomes [spark://abc, spark://def].
   */
  def parseStandaloneMasterUrls(masterUrls: String): Array[String] = {
    masterUrls.stripPrefix("spark://").split(",").map("spark://" + _)
  }

  /** An identifier that backup masters use in their responses. */
  val BACKUP_STANDALONE_MASTER_PREFIX = "Current state is not alive"

  /** Return true if the response message is sent from a backup Master on standby. */
  def responseFromBackup(msg: String): Boolean = {
    msg.startsWith(BACKUP_STANDALONE_MASTER_PREFIX)
  }

  /**
   * To avoid calling `Utils.getCallSite` for every single RDD we create in the body,
   * set a dummy call site that RDDs use instead. This is for performance optimization.
   */
  def withDummyCallSite[T](sc: SparkContext)(body: => T): T = {
    val oldShortCallSite = sc.getLocalProperty(CallSite.SHORT_FORM)
    val oldLongCallSite = sc.getLocalProperty(CallSite.LONG_FORM)
    try {
      sc.setLocalProperty(CallSite.SHORT_FORM, "")
      sc.setLocalProperty(CallSite.LONG_FORM, "")
      body
    } finally {
      // Restore the old ones here
      sc.setLocalProperty(CallSite.SHORT_FORM, oldShortCallSite)
      sc.setLocalProperty(CallSite.LONG_FORM, oldLongCallSite)
    }
  }

  /**
   * Return whether the specified file is a parent directory of the child file.
   */
  @tailrec
  def isInDirectory(parent: File, child: File): Boolean = {
    if (child == null || parent == null) {
      return false
    }
    if (!child.exists() || !parent.exists() || !parent.isDirectory()) {
      return false
    }
    if (parent.equals(child)) {
      return true
    }
    isInDirectory(parent, child.getParentFile)
  }


  /**
   *
   * @return whether it is local mode
   */
  def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master == "local" || master.startsWith("local[")
  }

  /**
   * Push based shuffle can only be enabled when below conditions are met:
   *   - the application is submitted to run in YARN mode
   *   - external shuffle service enabled
   *   - IO encryption disabled
   *   - serializer(such as KryoSerializer) supports relocation of serialized objects
   */
  def isPushBasedShuffleEnabled(conf: SparkConf,
      isDriver: Boolean,
      checkSerializer: Boolean = true): Boolean = {
    val pushBasedShuffleEnabled = conf.get(PUSH_BASED_SHUFFLE_ENABLED)
    if (pushBasedShuffleEnabled) {
      val canDoPushBasedShuffle = {
        val isTesting = conf.get(IS_TESTING).getOrElse(false)
        val isShuffleServiceAndYarn = conf.get(SHUFFLE_SERVICE_ENABLED) &&
            conf.get(SparkLauncher.SPARK_MASTER, null) == "yarn"
        lazy val serializerIsSupported = {
          if (checkSerializer) {
            Option(SparkEnv.get)
              .map(_.serializer)
              .filter(_ != null)
              .getOrElse(instantiateSerializerFromConf[Serializer](SERIALIZER, conf, isDriver))
              .supportsRelocationOfSerializedObjects
          } else {
            // if no need to check Serializer, always set serializerIsSupported as true
            true
          }
        }
        // TODO: [SPARK-36744] needs to support IO encryption for push-based shuffle
        val ioEncryptionDisabled = !conf.get(IO_ENCRYPTION_ENABLED)
        (isShuffleServiceAndYarn || isTesting) && ioEncryptionDisabled && serializerIsSupported
      }
      if (!canDoPushBasedShuffle) {
        logWarning(log"Push-based shuffle can only be enabled when the application is submitted " +
          log"to run in YARN mode, with external shuffle service enabled, IO encryption " +
          log"disabled, and relocation of serialized objects supported.")
      }

      canDoPushBasedShuffle
    } else {
      false
    }
  }

  // Create an instance of Serializer or ShuffleManager with the given name,
  // possibly initializing it with our conf
  def instantiateSerializerOrShuffleManager[T](className: String,
      conf: SparkConf,
      isDriver: Boolean): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, java.lang.Boolean.valueOf(isDriver))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  // Create an instance of Serializer named by the given SparkConf property
  // if the property is not set, possibly initializing it with our conf
  def instantiateSerializerFromConf[T](propertyName: ConfigEntry[String],
      conf: SparkConf,
      isDriver: Boolean): T = {
    instantiateSerializerOrShuffleManager[T](
      conf.get(propertyName), conf, isDriver)
  }

  /**
   * Return whether dynamic allocation is enabled in the given conf.
   */
  def isDynamicAllocationEnabled(conf: SparkConf): Boolean = {
    val dynamicAllocationEnabled = conf.get(DYN_ALLOCATION_ENABLED)
    dynamicAllocationEnabled &&
      (!isLocalMaster(conf) || conf.get(DYN_ALLOCATION_TESTING))
  }

  def isStreamingDynamicAllocationEnabled(conf: SparkConf): Boolean = {
    val streamingDynamicAllocationEnabled = conf.get(STREAMING_DYN_ALLOCATION_ENABLED)
    streamingDynamicAllocationEnabled &&
      (!isLocalMaster(conf) || conf.get(STREAMING_DYN_ALLOCATION_TESTING))
  }

  /**
   * Return the initial number of executors for dynamic allocation.
   */
  def getDynamicAllocationInitialExecutors(conf: SparkConf): Int = {
    if (conf.get(DYN_ALLOCATION_INITIAL_EXECUTORS) < conf.get(DYN_ALLOCATION_MIN_EXECUTORS)) {
      logWarning(log"${MDC(CONFIG, DYN_ALLOCATION_INITIAL_EXECUTORS.key)} less than " +
        log"${MDC(CONFIG2, DYN_ALLOCATION_MIN_EXECUTORS.key)} is invalid, ignoring its setting, " +
        log"please update your configs.")
    }

    if (conf.get(EXECUTOR_INSTANCES).getOrElse(0) < conf.get(DYN_ALLOCATION_MIN_EXECUTORS)) {
      logWarning(log"${MDC(CONFIG, EXECUTOR_INSTANCES.key)} less than " +
        log"${MDC(CONFIG2, DYN_ALLOCATION_MIN_EXECUTORS.key)} is invalid, ignoring its setting, " +
        log"please update your configs.")
    }

    val initialExecutors = Seq(
      conf.get(DYN_ALLOCATION_MIN_EXECUTORS),
      conf.get(DYN_ALLOCATION_INITIAL_EXECUTORS),
      conf.get(EXECUTOR_INSTANCES).getOrElse(0)).max

    logInfo(log"Using initial executors = ${MDC(NUM_EXECUTORS, initialExecutors)}, max of " +
      log"${MDC(CONFIG, DYN_ALLOCATION_INITIAL_EXECUTORS.key)}," +
      log"${MDC(CONFIG2, DYN_ALLOCATION_MIN_EXECUTORS.key)} and" +
      log" ${MDC(CONFIG3, EXECUTOR_INSTANCES.key)}")
    initialExecutors
  }

  /**
   * Returns a path of temporary file which is in the same directory with `path`.
   */
  def tempFileWith(path: File): File = {
    new File(path.getAbsolutePath + "." + UUID.randomUUID())
  }

  /**
   * Returns the name of this JVM process. This is OS dependent but typically (OSX, Linux, Windows),
   * this is formatted as PID@hostname.
   */
  def getProcessName(): String = {
    ManagementFactory.getRuntimeMXBean().getName()
  }

  /**
   * Utility function that should be called early in `main()` for daemons to set up some common
   * diagnostic state.
   */
  def initDaemon(log: Logger): Unit = {
    log.info(s"Started daemon with process name: ${Utils.getProcessName()}")
    SignalUtils.registerLogger(log)
  }

  /**
   * Utility function to enable or disable structured logging based on system properties.
   * This is designed for a code path which we cannot use SparkConf yet, and should be used before
   * the first invocation of `Logging.log()`. For example, this should be used before `initDaemon`.
   */
  def resetStructuredLogging(): Unit = {
    if (System.getProperty(STRUCTURED_LOGGING_ENABLED.key, "false").equals("false")) {
      Logging.disableStructuredLogging()
    } else {
      Logging.enableStructuredLogging()
    }
  }

  /**
   * Return the jar files pointed by the "spark.jars" property. Spark internally will distribute
   * these jars through file server. In the YARN mode, it will return an empty list, since YARN
   * has its own mechanism to distribute jars.
   */
  def getUserJars(conf: SparkConf): Seq[String] = {
    conf.get(JARS).filter(_.nonEmpty)
  }

  /**
   * Return the local jar files which will be added to REPL's classpath. These jar files are
   * specified by --jars (spark.jars) or --packages, remote jars will be downloaded to local by
   * SparkSubmit at first.
   */
  def getLocalUserJarsForShell(conf: SparkConf): Seq[String] = {
    val localJars = conf.getOption("spark.repl.local.jars")
    localJars.map(_.split(",")).map(_.filter(_.nonEmpty)).toSeq.flatten
  }

  private[spark] val REDACTION_REPLACEMENT_TEXT = "*********(redacted)"

  /**
   * Redact the sensitive values in the given map. If a map key matches the redaction pattern then
   * its value is replaced with a dummy text.
   */
  def redact(conf: SparkConf,
      kvs: scala.collection.Seq[(String, String)]): scala.collection.Seq[(String, String)] = {
    val redactionPattern = conf.get(SECRET_REDACTION_PATTERN)
    redact(redactionPattern, kvs)
  }

  /**
   * Redact the sensitive values in the given map. If a map key matches the redaction pattern then
   * its value is replaced with a dummy text.
   */
  def redact[K, V](regex: Option[Regex],
      kvs: scala.collection.Seq[(K, V)]): scala.collection.Seq[(K, V)] = {
    regex match {
      case None => kvs
      case Some(r) => redact(r, kvs)
    }
  }

  /**
   * Redact the sensitive information in the given string.
   */
  def redact(regex: Option[Regex], text: String): String = {
    regex match {
      case None => text
      case Some(r) =>
        if (text == null || text.isEmpty) {
          text
        } else {
          r.replaceAllIn(text, REDACTION_REPLACEMENT_TEXT)
        }
    }
  }

  private def redact[K, V](redactionPattern: Regex,
      kvs: scala.collection.Seq[(K, V)]): scala.collection.Seq[(K, V)] = {
    // If the sensitive information regex matches with either the key or the value, redact the value
    // While the original intent was to only redact the value if the key matched with the regex,
    // we've found that especially in verbose mode, the value of the property may contain sensitive
    // information like so:
    // "sun.java.command":"org.apache.spark.deploy.SparkSubmit ... \
    // --conf spark.executorEnv.HADOOP_CREDSTORE_PASSWORD=secret_password ...
    //
    // And, in such cases, simply searching for the sensitive information regex in the key name is
    // not sufficient. The values themselves have to be searched as well and redacted if matched.
    // This does mean we may be accounting more false positives - for example, if the value of an
    // arbitrary property contained the term 'password', we may redact the value from the UI and
    // logs. In order to work around it, user would have to make the spark.redaction.regex property
    // more specific.
    kvs.map {
      case (key: String, value: String) =>
        redactionPattern.findFirstIn(key)
          .orElse(redactionPattern.findFirstIn(value))
          .map { _ => (key, REDACTION_REPLACEMENT_TEXT) }
          .getOrElse((key, value))
      case (key, value: String) =>
        redactionPattern.findFirstIn(value)
          .map { _ => (key, REDACTION_REPLACEMENT_TEXT) }
          .getOrElse((key, value))
      case (key, value) =>
        (key, value)
    }.asInstanceOf[scala.collection.Seq[(K, V)]]
  }

  /**
   * Looks up the redaction regex from within the key value pairs and uses it to redact the rest
   * of the key value pairs. No care is taken to make sure the redaction property itself is not
   * redacted. So theoretically, the property itself could be configured to redact its own value
   * when printing.
   */
  def redact(kvs: Map[String, String]): scala.collection.Seq[(String, String)] = {
    val redactionPattern = kvs.getOrElse(
      SECRET_REDACTION_PATTERN.key,
      SECRET_REDACTION_PATTERN.defaultValueString
    ).r
    redact(redactionPattern, kvs.toArray)
  }

  def redactCommandLineArgs(conf: SparkConf, commands: Seq[String]): Seq[String] = {
    val redactionPattern = conf.get(SECRET_REDACTION_PATTERN)
    commands.map {
      case PATTERN_FOR_COMMAND_LINE_ARG(key, value) =>
        val (_, newValue) = redact(redactionPattern, Seq((key, value))).head
        s"-D$key=$newValue"

      case cmd => cmd
    }
  }

  def stringToSeq(str: String): Seq[String] = {
    str.split(",").map(_.trim()).filter(_.nonEmpty).toImmutableArraySeq
  }

  /**
   * Create instances of extension classes.
   *
   * The classes in the given list must:
   * - Be sub-classes of the given base class.
   * - Provide either a no-arg constructor, or a 1-arg constructor that takes a SparkConf.
   *
   * The constructors are allowed to throw "UnsupportedOperationException" if the extension does not
   * want to be registered; this allows the implementations to check the Spark configuration (or
   * other state) and decide they do not need to be added. A log message is printed in that case.
   * Other exceptions are bubbled up.
   */
  def loadExtensions[T <: AnyRef](
      extClass: Class[T], classes: Seq[String], conf: SparkConf): Seq[T] = {
    classes.flatMap { name =>
      try {
        val klass = classForName[T](name)
        require(extClass.isAssignableFrom(klass),
          s"$name is not a subclass of ${extClass.getName()}.")

        val ext = Try(klass.getConstructor(classOf[SparkConf])) match {
          case Success(ctor) =>
            ctor.newInstance(conf)

          case Failure(_) =>
            klass.getConstructor().newInstance()
        }

        Some(ext)
      } catch {
        case _: NoSuchMethodException =>
          throw new SparkException(
            s"$name did not have a zero-argument constructor or a" +
              " single-argument constructor that accepts SparkConf. Note: if the class is" +
              " defined inside of another Scala class, then its constructors may accept an" +
              " implicit parameter that references the enclosing class; in this case, you must" +
              " define the class as a top-level class in order to prevent this extra" +
              " parameter from breaking Spark's ability to find a valid constructor.")

        case e: InvocationTargetException =>
          e.getCause() match {
            case uoe: UnsupportedOperationException =>
              logDebug(s"Extension $name not being initialized.", uoe)
              logInfo(log"Extension ${MDC(CLASS_NAME, name)} not being initialized.")
              None

            case null => throw e

            case cause => throw cause
          }
      }
    }
  }

  /**
   * Check the validity of the given Kubernetes master URL and return the resolved URL. Prefix
   * "k8s://" is appended to the resolved URL as the prefix is used by KubernetesClusterManager
   * in canCreate to determine if the KubernetesClusterManager should be used.
   */
  def checkAndGetK8sMasterUrl(rawMasterURL: String): String = {
    require(rawMasterURL.startsWith("k8s://"),
      "Kubernetes master URL must start with k8s://.")
    val masterWithoutK8sPrefix = rawMasterURL.substring("k8s://".length)

    // To handle master URLs, e.g., k8s://host:port.
    if (!masterWithoutK8sPrefix.contains("://")) {
      val resolvedURL = s"https://$masterWithoutK8sPrefix"
      logInfo(log"No scheme specified for kubernetes master URL, so defaulting to https." +
        log" Resolved URL is ${MDC(LogKeys.URL, resolvedURL)}.")
      return s"k8s://$resolvedURL"
    }

    val masterScheme = new URI(masterWithoutK8sPrefix).getScheme

    val resolvedURL = Option(masterScheme).map(_.toLowerCase(Locale.ROOT)) match {
      case Some("https") =>
        masterWithoutK8sPrefix
      case Some("http") =>
        logWarning(log"Kubernetes master URL uses HTTP instead of HTTPS.")
        masterWithoutK8sPrefix
      case _ =>
        throw new IllegalArgumentException("Invalid Kubernetes master scheme: " + masterScheme
          + " found in URL: " + masterWithoutK8sPrefix)
    }

    s"k8s://$resolvedURL"
  }

  /**
   * Replaces all the {{EXECUTOR_ID}} occurrences with the Executor Id
   * and {{APP_ID}} occurrences with the App Id.
   */
  def substituteAppNExecIds(opt: String, appId: String, execId: String): String = {
    opt.replace("{{APP_ID}}", appId).replace("{{EXECUTOR_ID}}", execId)
  }

  /**
   * Replaces all the {{APP_ID}} occurrences with the App Id.
   */
  def substituteAppId(opt: String, appId: String): String = {
    opt.replace("{{APP_ID}}", appId)
  }

  def createSecret(conf: SparkConf): String = {
    val bits = conf.get(AUTH_SECRET_BIT_LENGTH)
    val rnd = new SecureRandom()
    val secretBytes = new Array[Byte](bits / JByte.SIZE)
    rnd.nextBytes(secretBytes)
    Hex.encodeHexString(secretBytes)
  }

  /**
   * Regular expression matching full width characters.
   *
   * Looked at all the 0x0000-0xFFFF characters (unicode) and showed them under Xshell.
   * Found all the full width characters, then get the regular expression.
   */
  private val fullWidthRegex = ("""[""" +
    // scalastyle:off nonascii
    "\u1100-\u115F" +
    "\u2E80-\uA4CF" +
    "\uAC00-\uD7A3" +
    "\uF900-\uFAFF" +
    "\uFE10-\uFE19" +
    "\uFE30-\uFE6F" +
    "\uFF00-\uFF60" +
    "\uFFE0-\uFFE6" +
    // scalastyle:on nonascii
    """]""").r

  /**
   * Return the number of half widths in a given string. Note that a full width character
   * occupies two half widths.
   *
   * For a string consisting of 1 million characters, the execution of this method requires
   * about 50ms.
   */
  def stringHalfWidth(str: String): Int = {
    if (str == null) 0 else str.length + fullWidthRegex.findAllIn(str).size
  }

  def sanitizeDirName(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

  def isClientMode(conf: SparkConf): Boolean = {
    "client".equals(conf.get(SparkLauncher.DEPLOY_MODE, "client"))
  }

  /** Returns whether the URI is a "local:" URI. */
  def isLocalUri(uri: String): Boolean = {
    uri.startsWith(s"$LOCAL_SCHEME:")
  }

  /** Create a UriBuilder from URI object. */
  def getUriBuilder(uri: URI): UriBuilder = {
    // scalastyle:off uribuilder
    UriBuilder.fromUri(uri)
    // scalastyle:on uribuilder
  }

  /** Create a UriBuilder from URI string. */
  def getUriBuilder(uri: String): UriBuilder = {
    // scalastyle:off uribuilder
    UriBuilder.fromUri(uri)
    // scalastyle:on uribuilder
  }

  /** Check whether the file of the path is splittable. */
  def isFileSplittable(path: Path, codecFactory: CompressionCodecFactory): Boolean = {
    val codec = codecFactory.getCodec(path)
    codec == null || codec.isInstanceOf[SplittableCompressionCodec]
  }

  /** Create a new properties object with the same values as `props` */
  def cloneProperties(props: Properties): Properties = {
    if (props == null) {
      return props
    }
    val resultProps = new Properties()
    props.forEach((k, v) => resultProps.put(k, v))
    resultProps
  }

  /**
   * Convert a sequence of `Path`s to a metadata string. When the length of metadata string
   * exceeds `stopAppendingThreshold`, stop appending paths for saving memory.
   */
  def buildLocationMetadata(paths: Seq[Path], stopAppendingThreshold: Int): String = {
    val metadata = new StringBuilder(s"(${paths.length} paths)[")
    var index: Int = 0
    while (index < paths.length && metadata.length < stopAppendingThreshold) {
      if (index > 0) {
        metadata.append(", ")
      }
      metadata.append(paths(index).toString)
      index += 1
    }
    if (paths.length > index) {
      if (index > 0) {
        metadata.append(", ")
      }
      metadata.append("...")
    }
    metadata.append("]")
    metadata.toString
  }

  /**
   * Convert MEMORY_OFFHEAP_SIZE to MB Unit, return 0 if MEMORY_OFFHEAP_ENABLED is false.
   */
  def executorOffHeapMemorySizeAsMb(sparkConf: SparkConf): Int = {
    val sizeInMB = Utils.memoryStringToMb(sparkConf.get(MEMORY_OFFHEAP_SIZE).toString)
    checkOffHeapEnabled(sparkConf, sizeInMB).toInt
  }

  /**
   * return 0 if MEMORY_OFFHEAP_ENABLED is false.
   */
  def checkOffHeapEnabled(sparkConf: SparkConf, offHeapSize: Long): Long = {
    if (sparkConf.get(MEMORY_OFFHEAP_ENABLED)) {
      require(offHeapSize > 0,
        s"${MEMORY_OFFHEAP_SIZE.key} must be > 0 when ${MEMORY_OFFHEAP_ENABLED.key} == true")
      offHeapSize
    } else {
      0
    }
  }

  /** Returns a string message about delegation token generation failure */
  def createFailedToGetTokenMessage(serviceName: String, e: scala.Throwable): MessageWithContext = {
    log"Failed to get token from service ${MDC(SERVICE_NAME, serviceName)} " +
      log"due to ${MDC(ERROR, e)}. If ${MDC(SERVICE_NAME, serviceName)} is not used, " +
      log"set spark.security.credentials.${MDC(SERVICE_NAME, serviceName)}.enabled to false."
  }

  /**
   * Decompress a zip file into a local dir. File names are read from the zip file. Note, we skip
   * addressing the directory here. Also, we rely on the caller side to address any exceptions.
   */
  def unzipFilesFromFile(fs: FileSystem, dfsZipFile: Path, localDir: File): Seq[File] = {
    val files = new ArrayBuffer[File]()
    val in = new ZipInputStream(fs.open(dfsZipFile))
    var out: OutputStream = null
    try {
      var entry = in.getNextEntry()
      while (entry != null) {
        if (!entry.isDirectory) {
          val fileName = localDir.toPath.resolve(entry.getName).getFileName.toString
          val outFile = new File(localDir, fileName)
          files += outFile
          out = new FileOutputStream(outFile)
          IOUtils.copy(in, out)
          out.close()
          in.closeEntry()
        }
        entry = in.getNextEntry()
      }
      in.close() // so that any error in closing does not get ignored
      logInfo(log"Unzipped from ${MDC(PATH, dfsZipFile)}\n\t${MDC(PATHS, files.mkString("\n\t"))}")
    } finally {
      // Close everything no matter what happened
      IOUtils.closeQuietly(in)
      IOUtils.closeQuietly(out)
    }
    files.toSeq
  }

  /**
   * Return the median number of a long array
   *
   * @param sizes
   * @param alreadySorted
   * @return
   */
  def median(sizes: Array[Long], alreadySorted: Boolean): Long = {
    val len = sizes.length
    val sortedSize = if (alreadySorted) sizes else sizes.sorted
    len match {
      case _ if (len % 2 == 0) =>
        math.max((sortedSize(len / 2) + sortedSize(len / 2 - 1)) / 2, 1)
      case _ => math.max(sortedSize(len / 2), 1)
    }
  }

  /**
   * Check if a command is available.
   */
  def checkCommandAvailable(command: String): Boolean = {
    // To avoid conflicts with java.lang.Process
    import scala.sys.process.{Process, ProcessLogger}

    val attempt = if (Utils.isWindows) {
      Try(Process(Seq(
        "cmd.exe", "/C", s"where $command")).run(ProcessLogger(_ => ())).exitValue())
    } else {
      Try(Process(Seq(
        "sh", "-c", s"command -v $command")).run(ProcessLogger(_ => ())).exitValue())
    }
    attempt.isSuccess && attempt.get == 0
  }

  /**
   * Return whether we are using G1GC or not
   */
  lazy val isG1GC: Boolean = {
    Try {
      val clazz = Utils.classForName("com.sun.management.HotSpotDiagnosticMXBean")
        .asInstanceOf[Class[_ <: PlatformManagedObject]]
      val vmOptionClazz = Utils.classForName("com.sun.management.VMOption")
      val hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(clazz)
      val vmOptionMethod = clazz.getMethod("getVMOption", classOf[String])
      val valueMethod = vmOptionClazz.getMethod("getValue")

      val useG1GCObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseG1GC")
      val useG1GC = valueMethod.invoke(useG1GCObject).asInstanceOf[String]
      "true".equals(useG1GC)
    }.getOrElse(false)
  }
}

private[util] object CallerContext extends Logging {
  val callerContextEnabled: Boolean =
    SparkHadoopUtil.get.conf.getBoolean("hadoop.caller.context.enabled", false)
}

/**
 * An utility class used to set up Spark caller contexts to HDFS and Yarn. The `context` will be
 * constructed by parameters passed in.
 * When Spark applications run on Yarn and HDFS, its caller contexts will be written into Yarn RM
 * audit log and hdfs-audit.log. That can help users to better diagnose and understand how
 * specific applications impacting parts of the Hadoop system and potential problems they may be
 * creating (e.g. overloading NN). As HDFS mentioned in HDFS-9184, for a given HDFS operation, it's
 * very helpful to track which upper level job issues it.
 *
 * @param from who sets up the caller context (TASK, CLIENT, APPMASTER)
 *
 * The parameters below are optional:
 * @param upstreamCallerContext caller context the upstream application passes in
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param jobId id of the job this task belongs to
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskId task id
 * @param taskAttemptNumber task attempt id
 */
private[spark] class CallerContext(
  from: String,
  upstreamCallerContext: Option[String] = None,
  appId: Option[String] = None,
  appAttemptId: Option[String] = None,
  jobId: Option[Int] = None,
  stageId: Option[Int] = None,
  stageAttemptId: Option[Int] = None,
  taskId: Option[Long] = None,
  taskAttemptNumber: Option[Int] = None) extends Logging {

  private val context = prepareContext("SPARK_" +
    from +
    appId.map("_" + _).getOrElse("") +
    appAttemptId.map("_" + _).getOrElse("") +
    jobId.map("_JId_" + _).getOrElse("") +
    stageId.map("_SId_" + _).getOrElse("") +
    stageAttemptId.map("_" + _).getOrElse("") +
    taskId.map("_TId_" + _).getOrElse("") +
    taskAttemptNumber.map("_" + _).getOrElse("") +
    upstreamCallerContext.map("_" + _).getOrElse(""))

  private def prepareContext(context: String): String = {
    // The default max size of Hadoop caller context is 128
    lazy val len = SparkHadoopUtil.get.conf.getInt("hadoop.caller.context.max.size", 128)
    if (context == null || context.length <= len) {
      context
    } else {
      val finalContext = context.substring(0, len)
      logWarning(log"Truncated Spark caller context from ${MDC(CONTEXT, context)} " +
        log"to ${MDC(FINAL_CONTEXT, finalContext)}")
      finalContext
    }
  }

  /**
   * Set up the caller context [[context]] by invoking Hadoop CallerContext API of
   * [[HadoopCallerContext]].
   */
  def setCurrentContext(): Unit = if (CallerContext.callerContextEnabled) {
    val hdfsContext = new HadoopCallerContextBuilder(context).build()
    HadoopCallerContext.setCurrent(hdfsContext)
  }
}

/**
 * A utility class to redirect the child process's stdout or stderr.
 */
private[spark] class RedirectThread(
    in: InputStream,
    out: OutputStream,
    name: String,
    propagateEof: Boolean = false)
  extends Thread(name) {

  setDaemon(true)
  override def run(): Unit = {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      Utils.tryWithSafeFinally {
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      } {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
}

/**
 * An [[OutputStream]] that will store the last 10 kilobytes (by default) written to it
 * in a circular buffer. The current contents of the buffer can be accessed using
 * the toString method.
 */
private[spark] class CircularBuffer(sizeInBytes: Int = 10240) extends java.io.OutputStream {
  private var pos: Int = 0
  private var isBufferFull = false
  private val buffer = new Array[Byte](sizeInBytes)

  def write(input: Int): Unit = {
    buffer(pos) = input.toByte
    pos = (pos + 1) % buffer.length
    isBufferFull = isBufferFull || (pos == 0)
  }

  override def toString: String = {
    if (!isBufferFull) {
      return new String(buffer, 0, pos, StandardCharsets.UTF_8)
    }

    val nonCircularBuffer = new Array[Byte](sizeInBytes)
    System.arraycopy(buffer, pos, nonCircularBuffer, 0, buffer.length - pos)
    System.arraycopy(buffer, 0, nonCircularBuffer, buffer.length - pos, pos)
    new String(nonCircularBuffer, StandardCharsets.UTF_8)
  }
}
