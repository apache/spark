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
import java.lang.management.ManagementFactory
import java.net._
import java.nio.ByteBuffer
import java.util.{Properties, Locale, Random, UUID}
import java.util.concurrent.{ThreadFactory, ConcurrentHashMap, Executors, ThreadPoolExecutor}
import javax.net.ssl.HttpsURLConnection

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.{ControlThrowable, NonFatal}

import com.google.common.io.{ByteStreams, Files}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.PropertyConfigurator
import org.eclipse.jetty.util.MultiException
import org.json4s._
import tachyon.client.{TachyonFS, TachyonFile}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

/** CallSite represents a place in user code. It can have a short and a long form. */
private[spark] case class CallSite(shortForm: String, longForm: String)

private[spark] object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
}

/**
 * Various utility methods used by Spark.
 */
private[spark] object Utils extends Logging {
  val random = new Random()

  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  @volatile private var localRootDirs: Array[String] = null

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
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, loader)
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
    Try { Class.forName(clazz, false, getContextOrSparkClassLoader) }.isSuccess
  }

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
  }

  /**
   * Primitive often used when writing [[java.nio.ByteBuffer]] to [[java.io.DataOutput]]
   */
  def writeByteBuffer(bb: ByteBuffer, out: ObjectOutput): Unit = {
    if (bb.hasArray) {
      out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val bbval = new Array[Byte](bb.remaining())
      bb.get(bbval)
      out.write(bbval)
    }
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()
  private val shutdownDeleteTachyonPaths = new scala.collection.mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dirs") {
    override def run(): Unit = Utils.logUncaughtExceptions {
      logDebug("Shutdown hook called")
      shutdownDeletePaths.foreach { dirPath =>
        try {
          Utils.deleteRecursively(new File(dirPath))
        } catch {
          case e: Exception => logError(s"Exception while deleting Spark temp dir: $dirPath", e)
        }
      }
    }
  })

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Register the tachyon path to be deleted via shutdown hook
  def registerShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths += absolutePath
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteTachyonDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in Exception and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    val retval = shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
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
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
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
    registerShutdownDeleteDir(dir)
    dir
  }

  /** Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
    * copying is disabled by default unless explicitly set transferToEnabled as true,
    * the parameter transferToEnabled should be configured by spark.file.transferTo = [true|false].
    */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false,
                 transferToEnabled: Boolean = false): Long =
  {
    var count = 0L
    try {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]
        && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val initialPos = outChannel.position()
        val size = inChannel.size()

        // In case transferTo method transferred less data than we have required.
        while (count < size) {
          count += inChannel.transferTo(count, size - count, outChannel)
        }

        // Check the position after transferTo loop to see if it is in the right position and
        // give user information if not.
        // Position will not be increased to the expected length after calling transferTo in
        // kernel version 2.6.32, this issue can be seen in
        // https://bugs.openjdk.java.net/browse/JDK-7052359
        // This will lead to stream corruption issue when using sort-based shuffle (SPARK-3948).
        val finalPos = outChannel.position()
        assert(finalPos == initialPos + size,
          s"""
             |Current position $finalPos do not equal to expected position ${initialPos + size}
             |after transferTo, please check your kernel version to see if it is 2.6.32,
             |this is a kernel bug which will lead to unexpected behavior when using transferTo.
             |You can set spark.file.transferTo = false to disable this NIO feature.
           """.stripMargin)
      } else {
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
      }
      count
    } finally {
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
   * Construct a URI container information used for authentication.
   * This also sets the default authenticator to properly negotiation the
   * user/password based on the URI.
   *
   * Note this relies on the Authenticator.setDefault being set properly to decode
   * the user name and password. This is currently set in the SecurityManager.
   */
  def constructURIForAuthentication(uri: URI, securityMgr: SecurityManager): URI = {
    val userCred = securityMgr.getSecretKey()
    if (userCred == null) throw new Exception("Secret key is null with authentication on")
    val userInfo = securityMgr.getHttpUser()  + ":" + userCred
    new URI(uri.getScheme(), userInfo, uri.getHost(), uri.getPort(), uri.getPath(),
      uri.getQuery(), uri.getFragment())
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
   */
  def fetchFile(
      url: String,
      targetDir: File,
      conf: SparkConf,
      securityMgr: SecurityManager,
      hadoopConf: Configuration,
      timestamp: Long,
      useCache: Boolean) {
    val fileName = url.split("/").last
    val targetFile = new File(targetDir, fileName)
    val fetchCacheEnabled = conf.getBoolean("spark.files.useFetchCache", defaultValue = true)
    if (useCache && fetchCacheEnabled) {
      val cachedFileName = s"${url.hashCode}${timestamp}_cache"
      val lockFileName = s"${url.hashCode}${timestamp}_lock"
      val localDir = new File(getLocalDir(conf))
      val lockFile = new File(localDir, lockFileName)
      val raf = new RandomAccessFile(lockFile, "rw")
      // Only one executor entry.
      // The FileLock is only used to control synchronization for executors download file,
      // it's always safe regardless of lock type (mandatory or advisory).
      val lock = raf.getChannel().lock()
      val cachedFile = new File(localDir, cachedFileName)
      try {
        if (!cachedFile.exists()) {
          doFetchFile(url, localDir, cachedFileName, conf, securityMgr, hadoopConf)
        }
      } finally {
        lock.release()
      }
      copyFile(
        url,
        cachedFile,
        targetFile,
        conf.getBoolean("spark.files.overwrite", false)
      )
    } else {
      doFetchFile(url, targetDir, fileName, conf, securityMgr, hadoopConf)
    }

    // Decompress the file if it's a .tar or .tar.gz
    if (fileName.endsWith(".tar.gz") || fileName.endsWith(".tgz")) {
      logInfo("Untarring " + fileName)
      executeAndGetOutput(Seq("tar", "-xzf", fileName), targetDir)
    } else if (fileName.endsWith(".tar")) {
      logInfo("Untarring " + fileName)
      executeAndGetOutput(Seq("tar", "-xf", fileName), targetDir)
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")

    // Windows does not grant read permission by default to non-admin users
    // Add read permission to owner explicitly
    if (isWindows) {
      FileUtil.chmod(targetFile.getAbsolutePath, "u+r")
    }
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
      Files.move(sourceFile, destFile)
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
      Files.equal(file1, file2)
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
      Files.copy(source, dest)
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
  private def doFetchFile(
      url: String,
      targetDir: File,
      filename: String,
      conf: SparkConf,
      securityMgr: SecurityManager,
      hadoopConf: Configuration) {
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    val fileOverwrite = conf.getBoolean("spark.files.overwrite", defaultValue = false)
    Option(uri.getScheme).getOrElse("file") match {
      case "http" | "https" | "ftp" =>
        var uc: URLConnection = null
        if (securityMgr.isAuthenticationEnabled()) {
          logDebug("fetchFile with security enabled")
          val newuri = constructURIForAuthentication(uri, securityMgr)
          uc = newuri.toURL().openConnection()
          uc.setAllowUserInteraction(false)
        } else {
          logDebug("fetchFile not using security")
          uc = new URL(url).openConnection()
        }
        Utils.setupSecureURLConnection(uc, securityMgr)

        val timeout = conf.getInt("spark.files.fetchTimeout", 60) * 1000
        uc.setConnectTimeout(timeout)
        uc.setReadTimeout(timeout)
        uc.connect()
        val in = uc.getInputStream()
        downloadFile(url, in, targetFile, fileOverwrite)
      case "file" =>
        // In the case of a local file, copy the local file to the target directory.
        // Note the difference between uri vs url.
        val sourceFile = if (uri.isAbsolute) new File(uri) else new File(url)
        copyFile(url, sourceFile, targetFile, fileOverwrite)
      case _ =>
        val fs = getHadoopFileSystem(uri, hadoopConf)
        val path = new Path(uri)
        fetchHcfsFile(path, targetDir, fs, conf, hadoopConf, fileOverwrite,
                      filename = Some(filename))
    }
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
   * Get the path of a temporary directory.  Spark's local directories can be configured through
   * multiple settings, which are used with the following precedence:
   *
   *   - If called from inside of a YARN container, this will return a directory chosen by YARN.
   *   - If the SPARK_LOCAL_DIRS environment variable is set, this will return a directory from it.
   *   - Otherwise, if the spark.local.dir is set, this will return a directory from it.
   *   - Otherwise, this will return java.io.tmpdir.
   *
   * Some of these configuration options might be lists of multiple paths, but this method will
   * always return a single directory.
   */
  def getLocalDir(conf: SparkConf): String = {
    getOrCreateLocalRootDirs(conf)(0)
  }

  private[spark] def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.
    // For Hadoop 0.23.X, we check for YARN_LOCAL_DIRS (we use this below in getYarnLocalDirs())
    // For Hadoop 2.X, we check for CONTAINER_ID.
    conf.getenv("CONTAINER_ID") != null || conf.getenv("YARN_LOCAL_DIRS") != null
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

  private def getOrCreateLocalRootDirsImpl(conf: SparkConf): Array[String] = {
    if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available. Note this assumes that Yarn has
      // created the directories already, and that they are secured so that only the
      // user has access to them.
      getYarnLocalDirs(conf).split(",")
    } else if (conf.getenv("SPARK_EXECUTOR_DIRS") != null) {
      conf.getenv("SPARK_EXECUTOR_DIRS").split(File.pathSeparator)
    } else {
      // In non-Yarn mode (or for the driver in yarn-client mode), we cannot trust the user
      // configuration to point to a secure directory. So create a subdirectory with restricted
      // permissions under each listed directory.
      Option(conf.getenv("SPARK_LOCAL_DIRS"))
        .getOrElse(conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")))
        .split(",")
        .flatMap { root =>
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
        .toArray
    }
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(conf: SparkConf): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(conf.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(conf.getenv("LOCAL_DIRS"))
      .getOrElse(""))

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
      val j = rand.nextInt(i)
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
  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)

  private def findLocalIpAddress(): String = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      defaultIpOverride
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toList
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse
        for (ni <- reOrderedNetworkIFs) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
               !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = sys.env.get("SPARK_LOCAL_HOSTNAME")

  /**
   * Allow setting a custom host name because when we run on Mesos we need to use the same
   * hostname it reports to the master.
   */
  def setCustomHostname(hostname: String) {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
   * Get the local machine's hostname.
   */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  def checkHostPort(hostPort: String, message: String = "") {
    assert(hostPort.indexOf(':') != -1, message)
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String,  Int) = {
    // Check cache first.
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    val indx: Int = hostPort.lastIndexOf(':')
    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ...
    // but then hadoop does not support ipv6 right now.
    // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
    if (-1 == indx) {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      return retval
    }

    val retval = (hostPort.substring(0, indx).trim(), hostPort.substring(indx + 1).trim().toInt)
    hostPortParseResults.putIfAbsent(hostPort, retval)
    hostPortParseResults.get(hostPort)
  }

  private val daemonThreadFactoryBuilder: ThreadFactoryBuilder =
    new ThreadFactoryBuilder().setDaemon(true)

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    daemonThreadFactoryBuilder.setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  /**
   * Return the string to tell how long has passed in milliseconds.
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  /**
   * Delete a file or directory and its contents recursively.
   * Don't follow directories if they are symlinks.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          shutdownDeletePaths.synchronized {
            shutdownDeletePaths.remove(file.getAbsolutePath)
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(dir: TachyonFile, client: TachyonFS) {
    if (!client.delete(dir.getPath(), true)) {
      throw new IOException("Failed to delete the tachyon dir: " + dir)
    }
  }

  /**
   * Check to see if file is a symbolic link.
   */
  def isSymlink(file: File): Boolean = {
    if (file == null) throw new NullPointerException("File must not be null")
    if (isWindows) return false
    val fileInCanonicalDir = if (file.getParent() == null) {
      file
    } else {
      new File(file.getParentFile().getCanonicalFile(), file.getName())
    }

    !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile())
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
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of megabytes.
   */
  def memoryStringToMb(str: String): Int = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      (lower.substring(0, lower.length-1).toLong / 1024).toInt
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length-1).toInt
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length-1).toInt * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length-1).toInt * 1024 * 1024
    } else {// no suffix, so it's just a number in bytes
      (lower.toLong / 1024 / 1024).toInt
    }
  }

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def bytesToString(size: Long): String = {
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    val (value, unit) = {
      if (size >= 2*TB) {
        (size.asInstanceOf[Double] / TB, "TB")
      } else if (size >= 2*GB) {
        (size.asInstanceOf[Double] / GB, "GB")
      } else if (size >= 2*MB) {
        (size.asInstanceOf[Double] / MB, "MB")
      } else if (size >= 2*KB) {
        (size.asInstanceOf[Double] / KB, "KB")
      } else {
        (size.asInstanceOf[Double], "B")
      }
    }
    "%.1f %s".formatLocal(Locale.US, value, unit)
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute

    ms match {
      case t if t < second =>
        "%d ms".format(t)
      case t if t < minute =>
        "%.1f s".format(t.toFloat / second)
      case t if t < hour =>
        "%.1f m".format(t.toFloat / minute)
      case t =>
        "%.2f h".format(t.toFloat / hour)
    }
  }

  /**
   * Convert a quantity in megabytes to a human-readable string such as "4.0 MB".
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
    val output = new StringBuffer
    val threadName = "read stdout for " + command(0)
    def appendToOutput(s: String): Unit = output.append(s)
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
      override def run() {
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
  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => SparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /**
   * Execute a block of code that evaluates to Unit, stop SparkContext is there is any uncaught 
   * exception
   *  
   * NOTE: This method is to be called by the driver-side components to avoid stopping the 
   * user-started JVM process completely; in contrast, tryOrExit is to be called in the 
   * spark-started JVM process .
   */
  def tryOrStopSparkContext(sc: SparkContext)(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if (sc != null) {
          logError(s"uncaught error in thread $currentThreadName, stopping SparkContext", t)
          sc.stop()
        }
        if (!NonFatal(t)) {
          logError(s"throw uncaught fatal error in thread $currentThreadName", t)
          throw t
        }
    }
  }

  /**
   * Execute a block of code that evaluates to Unit, re-throwing any non-fatal uncaught
   * exceptions as IOException.  This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException(block: => Unit) {
    try {
      block
    } catch {
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
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
      case e: IOException => throw e
      case NonFatal(t) => throw new IOException(t)
    }
  }

  /** Default filtering function for finding call sites using `getCallSite`. */
  private def coreExclusionFunction(className: String): Boolean = {
    // A regular expression to match classes of the "core" Spark API that we want to skip when
    // finding the call site of a method.
    val SPARK_CORE_CLASS_REGEX =
      """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkCoreClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkCoreClass || isScalaClass
  }

  /**
   * When called inside a class in the spark package, returns the name of the user code class
   * (outside the spark package) that called into Spark, as well as which Spark method they called.
   * This is used, for example, to tell users where in their code each RDD got created.
   *
   * @param skipClass Function that is used to exclude non-user-code classes.
   */
  def getCallSite(skipClass: String => Boolean = coreExclusionFunction): CallSite = {
    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    var callStack = new ArrayBuffer[String]() :+ "<unknown>"

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
            firstUserLine = ste.getLineNumber
            firstUserFile = ste.getFileName
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    CallSite(
      shortForm = s"$lastSparkMethod at $firstUserFile:$firstUserLine",
      longForm = callStack.take(callStackDepth).mkString("\n"))
  }

  /** Return a string containing part of a file from byte 'start' to 'end'. */
  def offsetBytes(path: String, start: Long, end: Long): String = {
    val file = new File(path)
    val length = file.length()
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = new FileInputStream(file)

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
  def offsetBytes(files: Seq[File], start: Long, end: Long): String = {
    val fileLengths = files.map { _.length }
    val startIndex = math.max(start, 0)
    val endIndex = math.min(end, fileLengths.sum)
    val fileToLength = files.zip(fileLengths).toMap
    logDebug("Log files: \n" + fileToLength.mkString("\n"))

    val stringBuffer = new StringBuffer((endIndex - startIndex).toInt)
    var sum = 0L
    for (file <- files) {
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
        stringBuffer.append(offsetBytes(file.getAbsolutePath, 0, fileToLength(file)))
      } else if (startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        // Case A and B: read from [start of required range] to [end of file / end of range]
        val effectiveStartIndex = startIndex - startIndexOfFile
        val effectiveEndIndex = math.min(endIndex - startIndexOfFile, fileToLength(file))
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, effectiveStartIndex, effectiveEndIndex))
      } else if (endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        // Case D: read from [start of file] to [end of require range]
        val effectiveStartIndex = math.max(startIndex - startIndexOfFile, 0)
        val effectiveEndIndex = endIndex - startIndexOfFile
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, effectiveStartIndex, effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $file, string built is ${stringBuffer.toString}}")
    }
    stringBuffer.toString
  }

  /**
   * Clone an object using a Spark serializer.
   */
  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T = {
    serializer.deserialize[T](serializer.serialize(value))
  }

  /**
   * Detect whether this thread might be executing a shutdown hook. Will always return true if
   * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
   * if System.exit was just called by a concurrent thread).
   *
   * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
   * an IllegalStateException.
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
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
    def endWord() {
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
    buf
  }

 /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  // Handles idiosyncracies with hash (add more as required)
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

  /** Returns the system properties map that is thread-safe to iterator over. It gets the
    * properties which have been set explicitly, as well as those for which only a default value
    * has been defined. */
  def getSystemProperties: Map[String, String] = {
    val sysProps = for (key <- System.getProperties.stringPropertyNames()) yield
      (key, System.getProperty(key))

    sysProps.toMap
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
   * @param numIters number of iterations
   * @param f function to be executed. If prepare is not None, the running time of each call to f
   *          must be an order of magnitude longer than one millisecond for accurate timing.
   * @param prepare function to be executed before each call to f. Its running time doesn't count.
   * @return the total time across all iterations (not couting preparation time)
   */
  def timeIt(numIters: Int)(f: => Unit, prepare: Option[() => Unit] = None): Long = {
    if (prepare.isEmpty) {
      val start = System.currentTimeMillis
      times(numIters)(f)
      System.currentTimeMillis - start
    } else {
      var i = 0
      var sum = 0L
      while (i < numIters) {
        prepare.get.apply()
        val start = System.currentTimeMillis
        f
        sum += System.currentTimeMillis - start
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
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

  /**
   * Creates a symlink. Note jdk1.7 has Files.createSymbolicLink but not used here
   * for jdk1.6 support.  Supports windows by doing copy, everything else uses "ln -sf".
   * @param src absolute path to the source
   * @param dst relative path for the destination
   */
  def symlink(src: File, dst: File) {
    if (!src.isAbsolute()) {
      throw new IOException("Source must be absolute")
    }
    if (dst.isAbsolute()) {
      throw new IOException("Destination must be relative")
    }
    var cmdSuffix = ""
    val linkCmd = if (isWindows) {
      // refer to http://technet.microsoft.com/en-us/library/cc771254.aspx
      cmdSuffix = " /s /e /k /h /y /i"
      "cmd /c xcopy "
    } else {
      cmdSuffix = ""
      "ln -sf "
    }
    import scala.sys.process._
    (linkCmd + src.getAbsolutePath() + " " + dst.getPath() + cmdSuffix) lines_!
    ProcessLogger(line => logInfo(line))
  }


  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  /** Return an option that translates JNothing to None */
  def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  /** Return an empty JSON object */
  def emptyJson: JsonAST.JObject = JObject(List[JField]())

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
   * Return the absolute path of a file in the given directory.
   */
  def getFilePath(dir: File, fileName: String): Path = {
    assert(dir.isDirectory)
    val path = new File(dir, fileName).getAbsolutePath
    new Path(path)
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
   * Format a Windows path such that it can be safely passed to a URI.
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting: Boolean = {
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }

  /**
   * Strip the directory from a path name
   */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }

  /**
   * Wait for a process to terminate for at most the specified duration.
   * Return whether the process actually terminated after the given timeout.
   */
  def waitForProcess(process: Process, timeoutMs: Long): Boolean = {
    var terminated = false
    val startTime = System.currentTimeMillis
    while (!terminated) {
      try {
        process.exitValue()
        terminated = true
      } catch {
        case e: IllegalThreadStateException =>
          // Process not terminated yet
          if (System.currentTimeMillis - startTime > timeoutMs) {
            return false
          }
          Thread.sleep(100)
      }
    }
    true
  }

  /**
   * Return the stderr of a process after waiting for the process to terminate.
   * If the process does not terminate within the specified timeout, return None.
   */
  def getStderr(process: Process, timeoutMs: Long): Option[String] = {
    val terminated = Utils.waitForProcess(process, timeoutMs)
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
      case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable =>
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
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash, but this is inconsistent with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path

    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }

    Option(uri.getScheme) match {
      case Some(windowsDrive(d)) if windows =>
        new URI("file:/" + uri.toString.stripPrefix("/"))
      case None =>
        // Preserve fragments for HDFS file name substitution (denoted by "#")
        // For instance, in "abc.py#xyz.py", "xyz.py" is the name observed by the application
        val fragment = uri.getFragment
        val part = new File(uri.getPath).toURI
        new URI(part.getScheme, part.getPath, fragment)
      case Some(other) =>
        uri
    }
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p, testWindows) }.mkString(",")
    }
  }

  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val formattedPath = if (windows) formatWindowsPath(p) else p
        val uri = new URI(formattedPath)
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

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
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

  /** Return a thread dump of all threads' stacktraces.  Used to capture dumps for the web UI */
  def getThreadDump(): Array[ThreadStackTrace] = {
    // We need to filter out null values here because dumpAllThreads() may return null array
    // elements for threads that are dead / don't exist.
    val threadInfos = ManagementFactory.getThreadMXBean.dumpAllThreads(true, true).filter(_ != null)
    threadInfos.sortBy(_.getThreadId).map { case threadInfo =>
      val stackTrace = threadInfo.getStackTrace.map(_.toString).mkString("\n")
      ThreadStackTrace(threadInfo.getThreadId, threadInfo.getThreadName,
        threadInfo.getThreadState, stackTrace)
    }
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
    if (conf.contains("spark.testing")) {
      // Set a higher number of retries for tests...
      maxRetries.getOrElse(100)
    } else {
      maxRetries.getOrElse(16)
    }
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
        // If the new port wraps around, do not try a privilege port
        ((startPort + offset - 1024) % (65536 - 1024)) + 1024
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              s"${e.getMessage}: Service$serviceString failed after $maxRetries retries!"
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          logWarning(s"Service$serviceString could not bind on port $tryPort. " +
            s"Attempting port ${tryPort + 1}.")
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
      case e: MultiException => e.getThrowables.exists(isBindCollision)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
   * config a log4j properties used for testsuite
   */
  def configTestLog4j(level: String): Unit = {
    val pro = new Properties()
    pro.put("log4j.rootLogger", s"$level, console")
    pro.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    pro.put("log4j.appender.console.target", "System.err")
    pro.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    pro.put("log4j.appender.console.layout.ConversionPattern",
      "%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    PropertyConfigurator.configure(pro)
  }

  /**
   * If the given URL connection is HttpsURLConnection, it sets the SSL socket factory and
   * the host verifier from the given security manager.
   */
  def setupSecureURLConnection(urlConnection: URLConnection, sm: SecurityManager): URLConnection = {
    urlConnection match {
      case https: HttpsURLConnection =>
        sm.sslSocketFactory.foreach(https.setSSLSocketFactory)
        sm.hostnameVerifier.foreach(https.setHostnameVerifier)
        https
      case connection => connection
    }
  }

  def invoke(
      clazz: Class[_],
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  // Limit of bytes for total size of results (default is 1GB)
  def getMaxResultSize(conf: SparkConf): Long = {
    memoryStringToMb(conf.get("spark.driver.maxResultSize", "1g")).toLong << 20
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
   * Return the value of a config either through the SparkConf or the Hadoop configuration
   * if this is Yarn mode. In the latter case, this defaults to the value set through SparkConf
   * if the key is not set in the Hadoop configuration.
   */
  def getSparkOrYarnConfig(conf: SparkConf, key: String, default: String): String = {
    val sparkValue = conf.get(key, default)
    if (SparkHadoopUtil.get.isYarnMode) {
      SparkHadoopUtil.get.newConfiguration(conf).get(key, sparkValue)
    } else {
      sparkValue
    }
  }

  /**
   * Return a pair of host and port extracted from the `sparkUrl`.
   *
   * A spark url (`spark://host:port`) is a special URI that its scheme is `spark` and only contains
   * host and port.
   *
   * @throws SparkException if `sparkUrl` is invalid.
   */
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
  override def run() {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      try {
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      } finally {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
}
