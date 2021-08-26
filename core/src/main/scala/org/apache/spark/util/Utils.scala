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
import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, ThreadInfo}
import java.lang.reflect.InvocationTargetException
import java.math.{MathContext, RoundingMode}
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, FileChannel, WritableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.SecureRandom
import java.util.{Locale, Properties, Random, UUID}
import java.util.concurrent._
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.zip.{GZIPInputStream, ZipInputStream}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.matching.Regex

import _root_.io.netty.channel.unix.Errors.NativeIoException
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.collect.Interners
import com.google.common.io.{ByteStreams, Files => GFiles}
import com.google.common.net.InetAddresses
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.{RunJar, StringUtils}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.eclipse.jetty.util.MultiException
import org.slf4j.Logger

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Streaming._
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.internal.config.UI._
import org.apache.spark.internal.config.Worker._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.status.api.v1.{StackTrace, ThreadStackTrace}
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
private[spark] object Utils extends Logging {
  val random = new Random()

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

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /** Deserialize an object using Java serialization and the given ClassLoader */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      }
    }
    ois.readObject.asInstanceOf[T]
  }

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
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
   * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
   * loaded Spark.
   *
   * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
   * active loader when setting up ClassLoader delegation chains.
   */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    Try { classForName(clazz, initialize = false) }.isSuccess
  }

  // scalastyle:off classforname
  /**
   * Preferred alternative to Class.forName(className), as well as
   * Class.forName(className, initialize, loader) with current thread's ContextClassLoader.
   */
  def classForName[C](
      className: String,
      initialize: Boolean = true,
      noSparkClassLoader: Boolean = false): Class[C] = {
    if (!noSparkClassLoader) {
      Class.forName(className, initialize, getContextOrSparkClassLoader).asInstanceOf[Class[C]]
    } else {
      Class.forName(className, initialize, Thread.currentThread().getContextClassLoader).
        asInstanceOf[Class[C]]
    }
    // scalastyle:on classforname
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

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.DataOutput]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: DataOutput): Unit = {
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val originalPosition = bb.position()
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
      bb.position(originalPosition)
    }
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.OutputStream]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: OutputStream): Unit = {
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val originalPosition = bb.position()
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
      bb.position(originalPosition)
    }
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
   * Create a directory given the abstract pathname
   * @return true, if the directory is successfully created; otherwise, return false.
   */
  def createDirectory(dir: File): Boolean = {
    try {
      // SPARK-35907: The check was required by File.mkdirs() because it could sporadically
      // fail silently. After switching to Files.createDirectories(), ideally, there should
      // no longer be silent fails. But the check is kept for the safety concern. We can
      // remove the check when we're sure that Files.createDirectories() would never fail silently.
      Files.createDirectories(dir.toPath)
      if ( !dir.exists() || !dir.isDirectory) {
        logError(s"Failed to create directory " + dir)
      }
      dir.isDirectory
    } catch {
      case e: Exception =>
        logError(s"Failed to create directory " + dir, e)
        false
    }
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        // SPARK-35907:
        // This could throw more meaningful exception information if directory creation failed.
        Files.createDirectories(dir.toPath)
      } catch {
        case e @ (_ : IOException | _ : SecurityException) =>
          logError(s"Failed to create directory $dir", e)
          dir = null
      }
    }

    dir.getCanonicalFile
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }

  /**
   * Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
   * copying is disabled by default unless explicitly set transferToEnabled as true,
   * the parameter transferToEnabled should be configured by spark.file.transferTo = [true|false].
   */
  def copyStream(
      in: InputStream,
      out: OutputStream,
      closeStreams: Boolean = false,
      transferToEnabled: Boolean = false): Long = {
    tryWithSafeFinally {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]
        && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val size = inChannel.size()
        copyFileStreamNIO(inChannel, outChannel, 0, size)
        size
      } else {
        var count = 0L
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
        count
      }
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
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

  def copyFileStreamNIO(
      input: FileChannel,
      output: WritableByteChannel,
      startPosition: Long,
      bytesToCopy: Long): Unit = {
    val outputInitialState = output match {
      case outputFileChannel: FileChannel =>
        Some((outputFileChannel.position(), outputFileChannel))
      case _ => None
    }
    var count = 0L
    // In case transferTo method transferred less data than we have required.
    while (count < bytesToCopy) {
      count += input.transferTo(count + startPosition, bytesToCopy - count, output)
    }
    assert(count == bytesToCopy,
      s"request to copy $bytesToCopy bytes, but actually copied $count bytes.")

    // Check the position after transferTo loop to see if it is in the right position and
    // give user information if not.
    // Position will not be increased to the expected length after calling transferTo in
    // kernel version 2.6.32, this issue can be seen in
    // https://bugs.openjdk.java.net/browse/JDK-7052359
    // This will lead to stream corruption issue when using sort-based shuffle (SPARK-3948).
    outputInitialState.foreach { case (initialPos, outputFileChannel) =>
      val finalPos = outputFileChannel.position()
      val expectedPos = initialPos + bytesToCopy
      assert(finalPos == expectedPos,
        s"""
           |Current position $finalPos do not equal to expected position $expectedPos
           |after transferTo, please check your kernel version to see if it is 2.6.32,
           |this is a kernel bug which will lead to unexpected behavior when using transferTo.
           |You can set spark.file.transferTo = false to disable this NIO feature.
         """.stripMargin)
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
    new URI("file", null, "localhost", -1, "/" + fileName, null, null).getRawPath.substring(1)
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
        logInfo("Untarring " + fileName)
        executeAndGetOutput(Seq("tar", "-xzf", fileName), targetDir)
      } else if (fileName.endsWith(".tar")) {
        logWarning(
          "Untarring behavior will be deprecated at spark.files and " +
            "SparkContext.addFile. Consider using spark.archives or SparkContext.addArchive " +
            "instead.")
        logInfo("Untarring " + fileName)
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
    val lowerSrc = StringUtils.toLowerCase(source.getName)
    if (lowerSrc.endsWith(".jar")) {
      RunJar.unJar(source, dest, RunJar.MATCH_ANY)
    } else if (lowerSrc.endsWith(".zip")) {
      FileUtil.unZip(source, dest)
    } else if (
      lowerSrc.endsWith(".tar.gz") || lowerSrc.endsWith(".tgz") || lowerSrc.endsWith(".tar")) {
      FileUtil.unTar(source, dest)
    } else {
      logWarning(s"Cannot unpack $source, just copying it to $dest.")
      copyRecursive(source, dest)
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
    logInfo(s"Fetching $url to $tempFile")

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
            s"File $destFile exists and does not match contents of $url, replacing it with $url"
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
          "%s has been previously copied to %s".format(
            sourceFile.getAbsolutePath,
            destFile.getAbsolutePath
          )
        )
        return
      }
    }

    // The file does not exist in the target directory. Copy or move it there.
    if (removeSourceFile) {
      Files.move(sourceFile.toPath, destFile.toPath)
    } else {
      logInfo(s"Copying ${sourceFile.getAbsolutePath} to ${destFile.getAbsolutePath}")
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
        val uc = new URL(url).openConnection()
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
    if (fs.isFile(path)) {
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
    val shuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)
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
    } else if (conf.getenv("MESOS_SANDBOX") != null && !shuffleServiceEnabled) {
      // Mesos already creates a directory per Mesos task. Spark should use that directory
      // instead so all temporary files are automatically cleaned up when the Mesos task ends.
      // Note that we don't want this if the shuffle service is enabled because we want to
      // continue to serve shuffle files after the executors that wrote them have already exited.
      Array(conf.getenv("MESOS_SANDBOX"))
    } else {
      if (conf.getenv("MESOS_SANDBOX") != null && shuffleServiceEnabled) {
        logInfo("MESOS_SANDBOX available but not using provided Mesos sandbox because " +
          s"${config.SHUFFLE_SERVICE_ENABLED.key} is enabled.")
      }
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
        "The configured local directories are not expected to be URIs; however, got suspicious " +
        s"values [${uris.mkString(", ")}]. Please check your configured local directories.")
    }

    configuredLocalDirs.flatMap { root =>
      try {
        val rootDir = new File(root)
        if (rootDir.exists || rootDir.mkdirs()) {
          val dir = createTempDir(root)
          chmod700(dir)
          Some(dir.getAbsolutePath)
        } else {
          logError(s"Failed to create dir in $root. Ignoring this directory.")
          None
        }
      } catch {
        case e: IOException =>
          logError(s"Failed to create local root dir in $root. Ignoring this directory.")
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
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
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
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " +
              strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  private var customHostname: Option[String] = sys.env.get("SPARK_LOCAL_HOSTNAME")

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
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
    customHostname.getOrElse(localIpAddress.getCanonicalHostName)
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddress.getHostAddress)
  }

  /**
   * Get the local machine's URI.
   */
  def localHostNameForURI(): String = {
    customHostname.getOrElse(InetAddresses.toUriString(localIpAddress))
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
   * Lists files recursively.
   */
  def recursiveList(f: File): Array[File] = {
    require(f.isDirectory)
    val result = f.listFiles.toBuffer
    val dirList = result.filter(_.isDirectory)
    while (dirList.nonEmpty) {
      val curDir = dirList.remove(0)
      val files = curDir.listFiles()
      result ++= files
      dirList ++= files.filter(_.isDirectory)
    }
    result.toArray
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File): Unit = {
    if (file != null) {
      JavaUtils.deleteRecursively(file)
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

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    val PiB = 1L << 50
    val TiB = 1L << 40
    val GiB = 1L << 30
    val MiB = 1L << 20
    val KiB = 1L << 10

    if (size >= BigInt(1L << 11) * EiB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EiB) {
          (BigDecimal(size) / EiB, "EiB")
        } else if (size >= 2 * PiB) {
          (BigDecimal(size) / PiB, "PiB")
        } else if (size >= 2 * TiB) {
          (BigDecimal(size) / TiB, "TiB")
        } else if (size >= 2 * GiB) {
          (BigDecimal(size) / GiB, "GiB")
        } else if (size >= 2 * MiB) {
          (BigDecimal(size) / MiB, "MiB")
        } else if (size >= 2 * KiB) {
          (BigDecimal(size) / KiB, "KiB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
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
      def log(s: String): Unit = logInfo(s)
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
      logError(s"Process $command exited with code $exitCode: $output")
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
          logError(s"uncaught error in thread $currentThreadName, stopping SparkContext", t)
          sc.stopInNewThread()
        }
        if (!NonFatal(t)) {
          logError(s"throw uncaught fatal error in thread $currentThreadName", t)
          throw t
        }
    }
  }

  /**
   * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
   * exceptions as IOException. This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit): Unit = {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  /**
   * Execute a block of code, then a finally block, but if exceptions happen in
   * the finally block, do not suppress the original exception.
   *
   * This is primarily an issue with `finally { out.close() }` blocks, where
   * close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
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
              logWarning(s"Suppressing exception in catch: ${t.getMessage}", t)
            }
        }
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
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
        logError(s"Cannot get file length of ${file}", e)
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
    val fileToLength = files.zip(fileLengths).toMap
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
   * Counts the number of elements of an iterator using a while loop rather than calling
   * [[scala.collection.Iterator#size]] because it uses a for loop, which is slightly slower
   * in the current version of Scala.
   */
  def getIteratorSize(iterator: Iterator[_]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
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


  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    getSimpleName(obj.getClass).replace("$", "")
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
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDrive = "([a-zA-Z])".r

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting: Boolean = {
    // Scala's `sys.env` creates a ton of garbage by constructing Scala immutable maps, so
    // we directly use the Java APIs instead.
    System.getenv("SPARK_TESTING") != null || System.getProperty(IS_TESTING.key) != null
  }

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
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
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
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
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

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getCanonicalFile().toURI()
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").filter(_.trim.nonEmpty).map { p => Utils.resolveURI(p) }.mkString(",")
    }
  }

  /** Check whether a path is an absolute URI. */
  def isAbsoluteURI(path: String): Boolean = {
    try {
      val uri = new URI(path: String)
      uri.isAbsolute
    } catch {
      case _: URISyntaxException =>
        false
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
   * Updates Spark config with properties from a set of Properties.
   * Provided properties have the highest priority.
   */
  def updateSparkConfigFromProperties(
      conf: SparkConf,
      properties: Map[String, String]) : Unit = {
    properties.filter { case (k, v) =>
      k.startsWith("spark.")
    }.foreach { case (k, v) =>
      conf.set(k, v)
    }
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
        case monitor: MonitorInfo =>
          s"Monitor(${lock.getClassName}@${lock.getIdentityHashCode}})"
        case _ =>
          s"Lock(${lock.getClassName}@${lock.getIdentityHashCode}})"
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
    val monitors = threadInfo.getLockedMonitors.map(m => m.getLockedStackFrame -> m).toMap
    val stackTrace = StackTrace(threadInfo.getStackTrace.map { frame =>
      monitors.get(frame) match {
        case Some(monitor) =>
          monitor.getLockedStackFrame.toString + s" => holding ${monitor.lockString}"
        case None =>
          frame.toString
      }
    })

    // use a set to dedup re-entrant locks that are held at multiple places
    val heldLocks =
      (threadInfo.getLockedSynchronizers ++ threadInfo.getLockedMonitors).map(_.lockString).toSet

    ThreadStackTrace(
      threadId = threadInfo.getThreadId,
      threadName = threadInfo.getThreadName,
      threadState = threadInfo.getThreadState,
      stackTrace = stackTrace,
      blockedByThreadId =
        if (threadInfo.getLockOwnerId < 0) None else Some(threadInfo.getLockOwnerId),
      blockedByLock = Option(threadInfo.getLockInfo).map(_.lockString).getOrElse(""),
      holdingLocks = heldLocks.toSeq)
  }

  /**
   * Convert all spark properties set in the given SparkConf to a sequence of java options.
   */
  def sparkJavaOpts(conf: SparkConf, filterKey: (String => Boolean) = _ => true): Seq[String] = {
    conf.getAll
      .filter { case (k, _) => filterKey(k) }
      .map { case (k, v) => s"-D$k=$v" }
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
   * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
   *
   * @param startPort The initial port to start the service on.
   * @param startService Function to start service on a given port.
   *                     This is expected to throw java.net.BindException on port collision.
   * @param conf A SparkConf used to get the maximum number of retries when binding to a port.
   * @param serviceName Name of the service.
   * @return (service: T, port: Int)
   */
  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: SparkConf,
      serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = portMaxRetries(conf)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        userPort(startPort, offset)
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
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
            logWarning(s"Service$serviceString could not bind on a random free port. " +
              "You may check whether configuring an appropriate binding address.")
          } else {
            logWarning(s"Service$serviceString could not bind on port $tryPort. " +
              s"Attempting port ${tryPort + 1}.")
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
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
   * configure a new log4j level
   */
  def setLogLevel(l: org.apache.log4j.Level): Unit = {
    val rootLogger = org.apache.log4j.Logger.getRootLogger()
    rootLogger.setLevel(l)
    // Setting threshold to null as rootLevel will define log level for spark-shell
    Logging.sparkShellThresholdLevel = null
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
        case e: Exception => logError(s"Error getting groups for user=$username", e)
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
   * Push based shuffle can only be enabled when the application is submitted
   * to run in YARN mode, with external shuffle service enabled
   */
  def isPushBasedShuffleEnabled(conf: SparkConf): Boolean = {
    conf.get(PUSH_BASED_SHUFFLE_ENABLED) &&
      (conf.get(IS_TESTING).getOrElse(false) ||
        (conf.get(SHUFFLE_SERVICE_ENABLED) &&
          conf.get(SparkLauncher.SPARK_MASTER, null) == "yarn"))
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
      logWarning(s"${DYN_ALLOCATION_INITIAL_EXECUTORS.key} less than " +
        s"${DYN_ALLOCATION_MIN_EXECUTORS.key} is invalid, ignoring its setting, " +
          "please update your configs.")
    }

    if (conf.get(EXECUTOR_INSTANCES).getOrElse(0) < conf.get(DYN_ALLOCATION_MIN_EXECUTORS)) {
      logWarning(s"${EXECUTOR_INSTANCES.key} less than " +
        s"${DYN_ALLOCATION_MIN_EXECUTORS.key} is invalid, ignoring its setting, " +
          "please update your configs.")
    }

    val initialExecutors = Seq(
      conf.get(DYN_ALLOCATION_MIN_EXECUTORS),
      conf.get(DYN_ALLOCATION_INITIAL_EXECUTORS),
      conf.get(EXECUTOR_INSTANCES).getOrElse(0)).max

    logInfo(s"Using initial executors = $initialExecutors, max of " +
      s"${DYN_ALLOCATION_INITIAL_EXECUTORS.key}, ${DYN_ALLOCATION_MIN_EXECUTORS.key} and " +
        s"${EXECUTOR_INSTANCES.key}")
    initialExecutors
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
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
  def redact(conf: SparkConf, kvs: Seq[(String, String)]): Seq[(String, String)] = {
    val redactionPattern = conf.get(SECRET_REDACTION_PATTERN)
    redact(redactionPattern, kvs)
  }

  /**
   * Redact the sensitive values in the given map. If a map key matches the redaction pattern then
   * its value is replaced with a dummy text.
   */
  def redact[K, V](regex: Option[Regex], kvs: Seq[(K, V)]): Seq[(K, V)] = {
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

  private def redact[K, V](redactionPattern: Regex, kvs: Seq[(K, V)]): Seq[(K, V)] = {
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
    }.asInstanceOf[Seq[(K, V)]]
  }

  /**
   * Looks up the redaction regex from within the key value pairs and uses it to redact the rest
   * of the key value pairs. No care is taken to make sure the redaction property itself is not
   * redacted. So theoretically, the property itself could be configured to redact its own value
   * when printing.
   */
  def redact(kvs: Map[String, String]): Seq[(String, String)] = {
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
    str.split(",").map(_.trim()).filter(_.nonEmpty)
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
              logInfo(s"Extension $name not being initialized.")
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
      logInfo("No scheme specified for kubernetes master URL, so defaulting to https. Resolved " +
        s"URL is $resolvedURL.")
      return s"k8s://$resolvedURL"
    }

    val masterScheme = new URI(masterWithoutK8sPrefix).getScheme

    val resolvedURL = Option(masterScheme).map(_.toLowerCase(Locale.ROOT)) match {
      case Some("https") =>
        masterWithoutK8sPrefix
      case Some("http") =>
        logWarning("Kubernetes master URL uses HTTP instead of HTTPS.")
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
   * Returns true if and only if the underlying class is a member class.
   *
   * Note: jdk8u throws a "Malformed class name" error if a given class is a deeply-nested
   * inner class (See SPARK-34607 for details). This issue has already been fixed in jdk9+, so
   * we can remove this helper method safely if we drop the support of jdk8u.
   */
  def isMemberClass(cls: Class[_]): Boolean = {
    try {
      cls.isMemberClass
    } catch {
      case _: InternalError =>
        // We emulate jdk8u `Class.isMemberClass` below:
        //   public boolean isMemberClass() {
        //     return getSimpleBinaryName() != null && !isLocalOrAnonymousClass();
        //   }
        // `getSimpleBinaryName()` returns null if a given class is a top-level class,
        // so we replace it with `cls.getEnclosingClass != null`. The second condition checks
        // if a given class is not a local or an anonymous class, so we replace it with
        // `cls.getEnclosingMethod == null` because `cls.getEnclosingMethod()` return a value
        // only in either case (JVM Spec 4.8.6).
        //
        // Note: The newer jdk evaluates `!isLocalOrAnonymousClass()` first,
        // we reorder the conditions to follow it.
        cls.getEnclosingMethod == null && cls.getEnclosingClass != null
    }
  }

  /**
   * Safer than Class obj's getSimpleName which may throw Malformed class name error in scala.
   * This method mimics scalatest's getSimpleNameOfAnObjectsClass.
   */
  def getSimpleName(cls: Class[_]): String = {
    try {
      cls.getSimpleName
    } catch {
      // TODO: the value returned here isn't even quite right; it returns simple names
      // like UtilsSuite$MalformedClassObject$MalformedClass instead of MalformedClass
      // The exact value may not matter much as it's used in log statements
      case _: InternalError =>
        stripDollars(stripPackages(cls.getName))
    }
  }

  /**
   * Remove the packages from full qualified class name
   */
  private def stripPackages(fullyQualifiedName: String): String = {
    fullyQualifiedName.split("\\.").takeRight(1)(0)
  }

  /**
   * Remove trailing dollar signs from qualified class name,
   * and return the trailing part after the last dollar sign in the middle
   */
  private def stripDollars(s: String): String = {
    val lastDollarIndex = s.lastIndexOf('$')
    if (lastDollarIndex < s.length - 1) {
      // The last char is not a dollar sign
      if (lastDollarIndex == -1 || !s.contains("$iw")) {
        // The name does not have dollar sign or is not an interpreter
        // generated class, so we should return the full string
        s
      } else {
        // The class name is interpreter generated,
        // return the part after the last dollar sign
        // This is the same behavior as getClass.getSimpleName
        s.substring(lastDollarIndex + 1)
      }
    }
    else {
      // The last char is a dollar sign
      // Find last non-dollar char
      val lastNonDollarChar = s.reverse.find(_ != '$')
      lastNonDollarChar match {
        case None => s
        case Some(c) =>
          val lastNonDollarIndex = s.lastIndexOf(c)
          if (lastNonDollarIndex == -1) {
            s
          } else {
            // Strip the trailing dollar signs
            // Invoke stripDollars again to get the simple name
            stripDollars(s.substring(0, lastNonDollarIndex + 1))
          }
      }
    }
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

  def executorTimeoutMs(conf: SparkConf): Long = {
    // "spark.network.timeout" uses "seconds", while `spark.storage.blockManagerSlaveTimeoutMs` uses
    // "milliseconds"
    conf.get(config.STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT)
      .getOrElse(Utils.timeStringAsMs(s"${conf.get(Network.NETWORK_TIMEOUT)}s"))
  }

  /** Returns a string message about delegation token generation failure */
  def createFailedToGetTokenMessage(serviceName: String, e: scala.Throwable): String = {
    val message = "Failed to get token from service %s due to %s. " +
      "If %s is not used, set spark.security.credentials.%s.enabled to false."
    message.format(serviceName, e, serviceName, serviceName)
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
      logInfo(s"Unzipped from $dfsZipFile\n\t${files.mkString("\n\t")}")
    } finally {
      // Close everything no matter what happened
      JavaUtils.closeQuietly(in)
      JavaUtils.closeQuietly(out)
    }
    files.toSeq
  }
}

private[util] object CallerContext extends Logging {
  val callerContextSupported: Boolean = {
    SparkHadoopUtil.get.conf.getBoolean("hadoop.caller.context.enabled", false) && {
      try {
        Utils.classForName("org.apache.hadoop.ipc.CallerContext")
        Utils.classForName("org.apache.hadoop.ipc.CallerContext$Builder")
        true
      } catch {
        case _: ClassNotFoundException =>
          false
        case NonFatal(e) =>
          logWarning("Fail to load the CallerContext class", e)
          false
      }
    }
  }
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
      logWarning(s"Truncated Spark caller context from $context to $finalContext")
      finalContext
    }
  }

  /**
   * Set up the caller context [[context]] by invoking Hadoop CallerContext API of
   * [[org.apache.hadoop.ipc.CallerContext]], which was added in hadoop 2.8.
   */
  def setCurrentContext(): Unit = {
    if (CallerContext.callerContextSupported) {
      try {
        val callerContext = Utils.classForName("org.apache.hadoop.ipc.CallerContext")
        val builder: Class[AnyRef] =
          Utils.classForName("org.apache.hadoop.ipc.CallerContext$Builder")
        val builderInst = builder.getConstructor(classOf[String]).newInstance(context)
        val hdfsContext = builder.getMethod("build").invoke(builderInst)
        callerContext.getMethod("setCurrent", callerContext).invoke(null, hdfsContext)
      } catch {
        case NonFatal(e) =>
          logWarning("Fail to set Spark caller context", e)
      }
    }
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
