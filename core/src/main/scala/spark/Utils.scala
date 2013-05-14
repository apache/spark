package spark

import java.io._
import java.net.{InetAddress, URL, URI, NetworkInterface, Inet4Address, ServerSocket}
import java.util.{Locale, Random, UUID}
import java.util.concurrent.{ConcurrentHashMap, Executors, ThreadFactory, ThreadPoolExecutor}
import org.apache.hadoop.fs.{Path, FileSystem, FileUtil}
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.collection.JavaConversions._
import scala.io.Source
import com.google.common.io.Files
import com.google.common.util.concurrent.ThreadFactoryBuilder
import spark.serializer.SerializerInstance
import spark.deploy.SparkHadoopUtil
import java.util.regex.Pattern

/**
 * Various utility methods used by Spark.
 */
private object Utils extends Logging {
  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    return bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    return ois.readObject.asInstanceOf[T]
  }

  /** Deserialize an object using Java serialization and the given ClassLoader */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    return ois.readObject.asInstanceOf[T]
  }

  def isAlpha(c: Char): Boolean = {
    (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
  }

  /** Split a string into words at non-alphabetic characters */
  def splitWords(s: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    var i = 0
    while (i < s.length) {
      var j = i
      while (j < s.length && isAlpha(s.charAt(j))) {
        j += 1
      }
      if (j > i) {
        buf += s.substring(i, j)
      }
      i = j
      while (i < s.length && !isAlpha(s.charAt(i))) {
        i += 1
      }
    }
    return buf
  }


  private val shutdownDeletePaths = new collection.mutable.HashSet[String]()

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true; else false
  // This is to ensure that two shutdown hooks do not try to delete each others paths - resulting in IOException
  // and incomplete cleanup
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {

    val absolutePath = file.getAbsolutePath()

    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.find(path => ! absolutePath.equals(path) && absolutePath.startsWith(path) ).isDefined
    }

    if (retval) logInfo("path = " + file + ", already present as root for deletion.")

    retval
  }

  /** Create a temporary directory inside the given parent directory */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory after " + maxAttempts +
            " attempts!")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: IOException => ; }
    }

    registerShutdownDeleteDir(dir)

    // Add a shutdown hook to delete the temp dir when the JVM exits
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark temp dir " + dir) {
      override def run() {
        // Attempt to delete if some patch which is parent of this is not already registered.
        if (! hasRootAsShutdownDeleteDir(dir)) Utils.deleteRecursively(dir)
      }
    })
    return dir
  }

  /** Copy all data from an InputStream to an OutputStream */
  def copyStream(in: InputStream,
                 out: OutputStream,
                 closeStreams: Boolean = false)
  {
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        out.write(buf, 0, n)
      }
    }
    if (closeStreams) {
      in.close()
      out.close()
    }
  }

  /**
   * Download a file requested by the executor. Supports fetching the file in a variety of ways,
   * including HTTP, HDFS and files on a standard filesystem, based on the URL parameter.
   *
   * Throws SparkException if the target file already exists and has different contents than
   * the requested file.
   */
  def fetchFile(url: String, targetDir: File) {
    val filename = url.split("/").last
    val tempDir = getLocalDir
    val tempFile =  File.createTempFile("fetchFileTemp", null, new File(tempDir))
    val targetFile = new File(targetDir, filename)
    val uri = new URI(url)
    uri.getScheme match {
      case "http" | "https" | "ftp" =>
        logInfo("Fetching " + url + " to " + tempFile)
        val in = new URL(url).openStream()
        val out = new FileOutputStream(tempFile)
        Utils.copyStream(in, out, true)
        if (targetFile.exists && !Files.equal(tempFile, targetFile)) {
          tempFile.delete()
          throw new SparkException("File " + targetFile + " exists and does not match contents of" +
            " " + url)
        } else {
          Files.move(tempFile, targetFile)
        }
      case "file" | null =>
        val sourceFile = if (uri.isAbsolute) {
          new File(uri)
        } else {
          new File(url)
        }
        if (targetFile.exists && !Files.equal(sourceFile, targetFile)) {
          throw new SparkException("File " + targetFile + " exists and does not match contents of" +
            " " + url)
        } else {
          // Remove the file if it already exists
          targetFile.delete()
          // Symlink the file locally.
          if (uri.isAbsolute) {
            // url is absolute, i.e. it starts with "file:///". Extract the source
            // file's absolute path from the url.
            val sourceFile = new File(uri)
            logInfo("Symlinking " + sourceFile.getAbsolutePath + " to " + targetFile.getAbsolutePath)
            FileUtil.symLink(sourceFile.getAbsolutePath, targetFile.getAbsolutePath)
          } else {
            // url is not absolute, i.e. itself is the path to the source file.
            logInfo("Symlinking " + url + " to " + targetFile.getAbsolutePath)
            FileUtil.symLink(url, targetFile.getAbsolutePath)
          }
        }
      case _ =>
        // Use the Hadoop filesystem library, which supports file://, hdfs://, s3://, and others
        val uri = new URI(url)
        val conf = SparkHadoopUtil.newConfiguration()
        val fs = FileSystem.get(uri, conf)
        val in = fs.open(new Path(uri))
        val out = new FileOutputStream(tempFile)
        Utils.copyStream(in, out, true)
        if (targetFile.exists && !Files.equal(tempFile, targetFile)) {
          tempFile.delete()
          throw new SparkException("File " + targetFile + " exists and does not match contents of" +
            " " + url)
        } else {
          Files.move(tempFile, targetFile)
        }
    }
    // Decompress the file if it's a .tar or .tar.gz
    if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xzf", filename), targetDir)
    } else if (filename.endsWith(".tar")) {
      logInfo("Untarring " + filename)
      Utils.execute(Seq("tar", "-xf", filename), targetDir)
    }
    // Make the file executable - That's necessary for scripts
    FileUtil.chmod(targetFile.getAbsolutePath, "a+x")
  }

  /**
   * Get a temporary directory using Spark's spark.local.dir property, if set. This will always
   * return a single directory, even though the spark.local.dir property might be a list of
   * multiple paths.
   */
  def getLocalDir: String = {
    System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir")).split(',')(0)
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassManifest](seq: TraversableOnce[T]): Seq[T] = {
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
        for (ni <- NetworkInterface.getNetworkInterfaces) {
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

  private var customHostname: Option[String] = None

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



  def localHostPort(): String = {
    val retval = System.getProperty("spark.hostPort", null)
    if (retval == null) {
      logErrorWithStack("spark.hostPort not set but invoking localHostPort")
      return localHostName()
    }

    retval
  }

/*
  // Used by DEBUG code : remove when all testing done
  private val ipPattern = Pattern.compile("^[0-9]+(\\.[0-9]+)*$")
  def checkHost(host: String, message: String = "") {
    // Currently catches only ipv4 pattern, this is just a debugging tool - not rigourous !
    // if (host.matches("^[0-9]+(\\.[0-9]+)*$")) {
    if (ipPattern.matcher(host).matches()) {
      Utils.logErrorWithStack("Unexpected to have host " + host + " which matches IP pattern. Message " + message)
    }
    if (Utils.parseHostPort(host)._2 != 0){
      Utils.logErrorWithStack("Unexpected to have host " + host + " which has port in it. Message " + message)
    }
  }

  // Used by DEBUG code : remove when all testing done
  def checkHostPort(hostPort: String, message: String = "") {
    val (host, port) = Utils.parseHostPort(hostPort)
    checkHost(host)
    if (port <= 0){
      Utils.logErrorWithStack("Unexpected to have port " + port + " which is not valid in " + hostPort + ". Message " + message)
    }
  }

  // Used by DEBUG code : remove when all testing done
  def logErrorWithStack(msg: String) {
    try { throw new Exception } catch { case ex: Exception => { logError(msg, ex) } }
    // temp code for debug
    System.exit(-1)
  }
*/

  // Once testing is complete in various modes, replace with this ?
  def checkHost(host: String, message: String = "") {}
  def checkHostPort(hostPort: String, message: String = "") {}

  // Used by DEBUG code : remove when all testing done
  def logErrorWithStack(msg: String) {
    try { throw new Exception } catch { case ex: Exception => { logError(msg, ex) } }
  }

  def getUserNameFromEnvironment(): String = {
    SparkHadoopUtil.getUserNameFromEnvironment
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  private val hostPortParseResults = new ConcurrentHashMap[String, (String, Int)]()
  def parseHostPort(hostPort: String): (String,  Int) = {
    {
      // Check cache first.
      var cached = hostPortParseResults.get(hostPort)
      if (cached != null) return cached
    }

    val indx: Int = hostPort.lastIndexOf(':')
    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ... but then hadoop does not support ipv6 right now.
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

  def addIfNoPort(hostPort: String,  port: Int): String = {
    if (port <= 0) throw new IllegalArgumentException("Invalid port specified " + port)

    // This is potentially broken - when dealing with ipv6 addresses for example, sigh ... but then hadoop does not support ipv6 right now.
    // For now, we assume that if port exists, then it is valid - not check if it is an int > 0
    val indx: Int = hostPort.lastIndexOf(':')
    if (-1 != indx) return hostPort

    hostPort + ":" + port
  }

  private[spark] val daemonThreadFactory: ThreadFactory =
    new ThreadFactoryBuilder().setDaemon(true).build()

  /**
   * Wrapper over newCachedThreadPool.
   */
  def newDaemonCachedThreadPool(): ThreadPoolExecutor =
    Executors.newCachedThreadPool(daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]

  /**
   * Return the string to tell how long has passed in seconds. The passing parameter should be in
   * millisecond.
   */
  def getUsedTimeMs(startTimeMs: Long): String = {
    return " " + (System.currentTimeMillis - startTimeMs) + " ms"
  }

  /**
   * Wrapper over newFixedThreadPool.
   */
  def newDaemonFixedThreadPool(nThreads: Int): ThreadPoolExecutor =
    Executors.newFixedThreadPool(nThreads, daemonThreadFactory).asInstanceOf[ThreadPoolExecutor]

  /**
   * Delete a file or directory and its contents recursively.
   */
  def deleteRecursively(file: File) {
    if (file.isDirectory) {
      for (child <- file.listFiles()) {
        deleteRecursively(child)
      }
    }
    if (!file.delete()) {
      throw new IOException("Failed to delete: " + file)
    }
  }

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of megabytes.
   * This is used to figure out how much memory to claim from Mesos based on the SPARK_MEM
   * environment variable.
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
   * Convert a memory quantity in bytes to a human-readable string such as "4.0 MB".
   */
  def memoryBytesToString(size: Long): String = {
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
   * Convert a memory quantity in megabytes to a human-readable string such as "4.0 MB".
   */
  def memoryMegabytesToString(megabytes: Long): String = {
    memoryBytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Execute a command in the given working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String], workingDir: File) {
    val process = new ProcessBuilder(command: _*)
        .directory(workingDir)
        .redirectErrorStream(true)
        .start()
    new Thread("read stdout for " + command(0)) {
      override def run() {
        for (line <- Source.fromInputStream(process.getInputStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw new SparkException("Process " + command + " exited with code " + exitCode)
    }
  }

  /**
   * Execute a command in the current working directory, throwing an exception if it completes
   * with an exit code other than 0.
   */
  def execute(command: Seq[String]) {
    execute(command, new File("."))
  }


  /**
   * When called inside a class in the spark package, returns the name of the user code class
   * (outside the spark package) that called into Spark, as well as which Spark method they called.
   * This is used, for example, to tell users where in their code each RDD got created.
   */
  def getSparkCallSite: String = {
    val trace = Thread.currentThread.getStackTrace().filter( el =>
      (!el.getMethodName.contains("getStackTrace")))

    // Keep crawling up the stack trace until we find the first function not inside of the spark
    // package. We track the last (shallowest) contiguous Spark method. This might be an RDD
    // transformation, a SparkContext function (such as parallelize), or anything else that leads
    // to instantiation of an RDD. We also track the first (deepest) user method, file, and line.
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var finished = false

    for (el <- trace) {
      if (!finished) {
        if (el.getClassName.startsWith("spark.") && !el.getClassName.startsWith("spark.examples.")) {
          lastSparkMethod = if (el.getMethodName == "<init>") {
            // Spark method is a constructor; get its class name
            el.getClassName.substring(el.getClassName.lastIndexOf('.') + 1)
          } else {
            el.getMethodName
          }
        }
        else {
          firstUserLine = el.getLineNumber
          firstUserFile = el.getFileName
          finished = true
        }
      }
    }
    "%s at %s:%s".format(lastSparkMethod, firstUserFile, firstUserLine)
  }

  /**
   * Try to find a free port to bind to on the local host. This should ideally never be needed,
   * except that, unfortunately, some of the networking libraries we currently rely on (e.g. Spray)
   * don't let users bind to port 0 and then figure out which free port they actually bound to.
   * We work around this by binding a ServerSocket and immediately unbinding it. This is *not*
   * necessarily guaranteed to work, but it's the best we can do.
   */
  def findFreePort(): Int = {
    val socket = new ServerSocket(0)
    val portBound = socket.getLocalPort
    socket.close()
    portBound
  }

  /**
   * Clone an object using a Spark serializer.
   */
  def clone[T](value: T, serializer: SerializerInstance): T = {
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
    return false
  }
}
