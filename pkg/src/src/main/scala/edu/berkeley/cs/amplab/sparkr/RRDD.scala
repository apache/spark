package edu.berkeley.cs.amplab.sparkr

import java.io._
import java.util.{Map => JMap}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, Partition, SparkException, TaskContext, SparkConf}
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD, JavaPairRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

private abstract class BaseRRDD[T: ClassTag, U: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    functionDependencies: Array[Byte],
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Broadcast[Object]])
  extends RDD[U](parent) {
  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {

    val parentIterator = firstParent[T].iterator(split, context)

    val pb = rWorkerProcessBuilder()
    val proc = pb.start()

    val errThread = startStderrThread(proc)

    val tempFile = startStdinThread(proc, parentIterator, split.index)

    // Return an iterator that read lines from the process's stdout
    val inputStream = new BufferedReader(new InputStreamReader(proc.getInputStream))

    try {
      val stdOutFileName = inputStream.readLine().trim()
      val dataStream = openDataStream(stdOutFileName)

      return new Iterator[U] {
        def next(): U = {
          val obj = _nextObj
          if (hasNext) {
            _nextObj = read()
          }
          obj
        }

        var _nextObj = read()

        def hasNext(): Boolean = {
          val hasMore = (_nextObj != null)
          if (!hasMore) {
            // Delete the temporary file we created as we are done reading it
            dataStream.close()
            tempFile.delete()
          }
          hasMore
        }
      }
    } catch {
      case e: Exception =>
        throw new SparkException("R computation failed with\n " + errThread.getLines())
    }
  }

  /**
   * ProcessBuilder used to launch worker R processes.
   */
  private def rWorkerProcessBuilder() = {
    val rCommand = "Rscript"
    val rOptions = "--vanilla"
    val rExecScript = rLibDir + "/SparkR/worker/worker.R"
    val pb = new ProcessBuilder(List(rCommand, rOptions, rExecScript))
    // Unset the R_TESTS environment variable for workers.
    // This is set by R CMD check as startup.Rs
    // (http://svn.r-project.org/R/trunk/src/library/tools/R/testing.R)
    // and confuses worker script which tries to load a non-existent file
    pb.environment().put("R_TESTS", "");
    pb
  }

  /**
   * Start a thread to print the process's stderr to ours
   */
  private def startStderrThread(proc: Process): BufferedStreamThread = {
    val ERR_BUFFER_SIZE = 100
    val errThread = new BufferedStreamThread(proc.getErrorStream, "stderr reader for R",
      ERR_BUFFER_SIZE)
    errThread.setDaemon(true)
    errThread.start()
    errThread
  }

  /**
   * Start a thread to write RDD data to the R process.
   */
  private def startStdinThread[T](
    proc: Process,
    iter: Iterator[T],
    splitIndex: Int) : File = {

    val env = SparkEnv.get
    val conf = env.conf
    val tempDir = RRDD.getLocalDir(conf)
    val tempFile = File.createTempFile("rSpark", "out", new File(tempDir))
    val tempFileIn = File.createTempFile("rSpark", "in", new File(tempDir))

    val tempFileName = tempFile.getAbsolutePath()
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for R") {
      override def run() {
        try {
          SparkEnv.set(env)
          val stream = new BufferedOutputStream(new FileOutputStream(tempFileIn), bufferSize)
          val printOut = new PrintStream(stream)
          val dataOut = new DataOutputStream(stream)

          dataOut.writeInt(splitIndex)

          dataOut.writeInt(func.length)
          dataOut.write(func)

          SerDe.writeString(dataOut, deserializer)
          SerDe.writeString(dataOut, serializer)

          dataOut.writeInt(packageNames.length)
          dataOut.write(packageNames)

          dataOut.writeInt(functionDependencies.length)
          dataOut.write(functionDependencies)

          dataOut.writeInt(broadcastVars.length)
          broadcastVars.foreach { broadcast =>
            // TODO(shivaram): Read a Long in R to avoid this cast
            dataOut.writeInt(broadcast.id.toInt)
            // TODO: Pass a byte array from R to avoid this cast ?
            val broadcastByteArr = broadcast.value.asInstanceOf[Array[Byte]]
            dataOut.writeInt(broadcastByteArr.length)
            dataOut.write(broadcastByteArr, 0, broadcastByteArr.length)
          }

          dataOut.writeInt(numPartitions)

          if (!iter.hasNext) {
            dataOut.writeInt(0)
          } else {
            dataOut.writeInt(1)
          }

          for (elem <- iter) {
            if (deserializer == SerializationFormats.BYTE) {
              val elemArr = elem.asInstanceOf[Array[Byte]]
              dataOut.writeInt(elemArr.length)
              dataOut.write(elemArr, 0, elemArr.length)
            } else if (deserializer == SerializationFormats.ROW) {
                val rowArr = elem.asInstanceOf[Array[Byte]]
                dataOut.write(rowArr, 0, rowArr.length)
            } else if (deserializer == SerializationFormats.STRING) {
                printOut.println(elem)
            }
          }

          printOut.flush()
          dataOut.flush()
          stream.flush()
          stream.close()

          // NOTE: We need to write out the temp file before writing out the 
          // file name to stdin. Otherwise the R process could read partial state
          val streamStd = new BufferedOutputStream(proc.getOutputStream, bufferSize)
          val printOutStd = new PrintStream(streamStd)
          printOutStd.println(tempFileName)
          printOutStd.println(rLibDir)
          printOutStd.println(tempFileIn.getAbsolutePath())
          printOutStd.flush()

          streamStd.close()
        } catch {
          // TODO: We should propogate this error to the task thread
          case e: Exception =>
            System.err.println("R Writer thread got an exception " + e)
            e.printStackTrace()
        }
      }
    }.start()

    tempFile
  }

  protected def openDataStream(stdOutFileName: String): Closeable
  protected def read(): U
}

/**
 * Form an RDD[Int, Array[Byte])] from key-value pairs returned from R.
 * This is used by SparkR's shuffle operations.
 */
private class PairwiseRRDD[T: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    hashFunc: Array[Byte],
    deserializer: String,
    functionDependencies: Array[Byte],
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Object])
  extends BaseRRDD[T, (Int, Array[Byte])](parent, numPartitions, hashFunc, deserializer,
                                          SerializationFormats.BYTE, functionDependencies,
                                          packageNames, rLibDir,
                                          broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {

  private var dataStream: DataInputStream = _

  override protected def openDataStream(stdOutFileName: String) = {
    dataStream = new DataInputStream(new FileInputStream(stdOutFileName))
    dataStream
  }

  override protected def read(): (Int, Array[Byte]) = {
    try {
      val length = dataStream.readInt()

      length match {
        case length if length == 2 =>
          val hashedKey = dataStream.readInt()
          val contentPairsLength = dataStream.readInt()
          val contentPairs = new Array[Byte](contentPairsLength)
          dataStream.read(contentPairs, 0, contentPairsLength)
          (hashedKey, contentPairs)
        case _ => null   // End of input
      }
    } catch {
      case eof: EOFException => {
        throw new SparkException("R worker exited unexpectedly (crashed)", eof)
      }
    }
  }

  lazy val asJavaPairRDD : JavaPairRDD[Int, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

/**
 * An RDD that stores serialized R objects as Array[Byte].
 */
private class RRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    serializeMode: String,
    functionDependencies: Array[Byte],
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Object])
  extends BaseRRDD[T, Array[Byte]](parent, -1, func, deserializer,
                                   serializeMode, functionDependencies, packageNames, rLibDir,
                                   broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {

  private var dataStream: DataInputStream = _

  override protected def openDataStream(stdOutFileName: String) = {
    dataStream = new DataInputStream(new FileInputStream(stdOutFileName))
    dataStream
  }

  override protected def read(): Array[Byte] = {
    try {
      val length = dataStream.readInt()

      length match {
        case length if length > 0 =>
          val obj = new Array[Byte](length)
          dataStream.read(obj, 0, length)
          obj
        case _ => null
      }
    } catch {
      case eof: EOFException => {
        throw new SparkException("R worker exited unexpectedly (crashed)", eof)
      }
    }
  }

  lazy val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}

/**
 * An RDD that stores R objects as Array[String].
 */
private class StringRRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    functionDependencies: Array[Byte],
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Object])
  extends BaseRRDD[T, String](parent, -1, func, deserializer, SerializationFormats.STRING,
                              functionDependencies, packageNames, rLibDir,
                              broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {

  private var dataStream: BufferedReader = _

  override protected def openDataStream(stdOutFileName: String) = {
    dataStream = new BufferedReader(
                     new InputStreamReader(new FileInputStream(stdOutFileName)))
    dataStream
  }

  override protected def read(): String = {
    try {
      dataStream.readLine()
    } catch {
      case e: IOException => {
        throw new SparkException("R worker exited unexpectedly (crashed)", e)
      }
    }
  }

  lazy val asJavaRDD : JavaRDD[String] = JavaRDD.fromRDD(this)
}

private class BufferedStreamThread(
    in: InputStream,
    name: String,
    errBufferSize: Int) extends Thread(name) {
  val lines = new Array[String](errBufferSize)
  var lineIdx = 0
  override def run() {
    for (line <- Source.fromInputStream(in).getLines) {
      lines(lineIdx) = line
      lineIdx = (lineIdx + 1) % errBufferSize
      System.err.println(line)
    }
  }

  def getLines(): String = {
    (0 until errBufferSize).filter { x =>
      lines((x + lineIdx) % errBufferSize) != null
    }.map { x =>
      lines((x + lineIdx) % errBufferSize)
    }.mkString("\n")
  }
}

object RRDD {

  def createSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Array[String],
      sparkEnvirMap: JMap[Object, Object],
      sparkExecutorEnvMap: JMap[Object, Object]): JavaSparkContext = {

    val sparkConf = new SparkConf().setAppName(appName)
                                   .setSparkHome(sparkHome)
                                   .setJars(jars)

    // Override `master` if we have a user-specified value
    if (master != "") {
      sparkConf.setMaster(master)
    } else {
      // If conf has no master set it to "local" to maintain
      // backwards compatibility
      sparkConf.setIfMissing("spark.master", "local")
    }

    for ((name, value) <- sparkEnvirMap) {
      sparkConf.set(name.asInstanceOf[String], value.asInstanceOf[String])
    }
    for ((name, value) <- sparkExecutorEnvMap) {
      sparkConf.setExecutorEnv(name.asInstanceOf[String], value.asInstanceOf[String])
    }
    new JavaSparkContext(sparkConf)
  }

  /**
   * Create an RRDD given a sequence of byte arrays. Used to create RRDD when `parallelize` is
   * called from R.
   */
  def createRDDFromArray(jsc: JavaSparkContext, arr: Array[Array[Byte]]): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(jsc.sc.parallelize(arr, arr.length))
  }

  def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.
    // For Hadoop 0.23.X, we check for YARN_LOCAL_DIRS (we use this below in getYarnLocalDirs())
    // For Hadoop 2.X, we check for CONTAINER_ID.
    System.getenv("CONTAINER_ID") != null || System.getenv("YARN_LOCAL_DIRS") != null
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

  /**
   * Gets or creates the directories listed in spark.local.dir or SPARK_LOCAL_DIRS,
   * and returns only the directories that exist / could be created.
   *
   * If no directories could be created, this will return an empty list.
   */
  def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    val confValue = if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available.
      getYarnLocalDirs(conf)
    } else {
      Option(System.getenv("SPARK_LOCAL_DIRS")).getOrElse(
        conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")))
    }
    val rootDirs = confValue.split(',')

    rootDirs.flatMap { rootDir =>
      val localDir: File = new File(rootDir)
      val foundLocalDir = localDir.exists || localDir.mkdirs()
      if (!foundLocalDir) {
        None
      } else {
        Some(rootDir)
      }
    }
  }

  /** Get the Yarn approved local directories. */
  def getYarnLocalDirs(conf: SparkConf): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(System.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }
}
