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

/**
 * Form an RDD[(Array[Byte], Array[Byte])] from key-value pairs returned from R.
 * This is used by SparkR's shuffle operations.
 */
private class PairwiseRRDD[T: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    hashFunc: Array[Byte],
    dataSerialized: Boolean,
    functionDependencies: Array[Byte],
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Broadcast[Object]])
  extends RDD[(Int, Array[Byte])](parent) {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Array[Byte])] = {

    val parentIterator = firstParent[T].iterator(split, context)

    val pb = RRDD.rWorkerProcessBuilder(rLibDir)
    val proc = pb.start()

    RRDD.startStderrThread(proc)

    val tempFile = RRDD.startStdinThread(rLibDir, proc, hashFunc, dataSerialized,
      functionDependencies, packageNames, broadcastVars,
      parentIterator, numPartitions,
      split.index)

    // Return an iterator that read lines from the process's stdout
    val inputStream = new BufferedReader(new InputStreamReader(proc.getInputStream))
    val stdOutFileName = inputStream.readLine().trim()

    val dataStream = new DataInputStream(new FileInputStream(stdOutFileName))

    return new Iterator[(Int, Array[Byte])] {
      def next(): (Int, Array[Byte]) = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): (Int, Array[Byte]) = {
        try {
          val length = dataStream.readInt()

          length match {
            case length if length == 2 =>
              val hashedKey = dataStream.readInt()
              val contentPairsLength = dataStream.readInt()
              val contentPairs = new Array[Byte](contentPairsLength)
              dataStream.read(contentPairs, 0, contentPairsLength)
              (hashedKey, contentPairs)
            case _ => (0, new Array[Byte](0))   // End of input
          }
        } catch {
          case eof: EOFException => {
            throw new SparkException("R worker exited unexpectedly (crashed)", eof)
          }
        }
      }
      var _nextObj = read()

      def hasNext(): Boolean = {
        val hasMore = !(_nextObj._1 == 0 && _nextObj._2.length == 0)
        if (!hasMore) {
          // Delete the temporary file we created as we are done reading it
          dataStream.close()
          tempFile.delete()
        }
        hasMore
      }
    }
  }

  lazy val asJavaPairRDD : JavaPairRDD[Int, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

/**
 * An RDD that stores serialized R objects as Array[Byte].
 */
class RRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    dataSerialized: Boolean,
    functionDependencies: Array[Byte],
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Broadcast[Object]])
  extends RDD[Array[Byte]](parent) {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

    val parentIterator = firstParent[T].iterator(split, context)

    val pb = RRDD.rWorkerProcessBuilder(rLibDir)
    val proc = pb.start()

    RRDD.startStderrThread(proc)

    // Write -1 in numPartitions to indicate this is a normal RDD
    val tempFile = RRDD.startStdinThread(rLibDir, proc, func, dataSerialized,
      functionDependencies, packageNames, broadcastVars,
      parentIterator, numPartitions = -1, split.index)

    // Return an iterator that read lines from the process's stdout
    val inputStream = new BufferedReader(new InputStreamReader(proc.getInputStream))
    val stdOutFileName = inputStream.readLine().trim()

    val dataStream = new DataInputStream(new FileInputStream(stdOutFileName))

    return new Iterator[Array[Byte]] {
      def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        try {
          val length = dataStream.readInt()

          length match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              dataStream.read(obj, 0, length)
              obj
            case _ =>
              new Array[Byte](0)
          }
        } catch {
          case eof: EOFException => {
            throw new SparkException("R worker exited unexpectedly (crashed)", eof)
          }
        }
      }
      var _nextObj = read()

      def hasNext(): Boolean = {
        val hasMore = _nextObj.length != 0
        if (!hasMore) {
          // Delete the temporary file we created as we are done reading it
          dataStream.close()
          tempFile.delete()
        }
        hasMore
      }
    }
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}


object RRDD {

  def createSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Array[String],
      vars: JMap[Object, Object]): JavaSparkContext = {

    val sparkConf = new SparkConf().setMaster(master)
                                   .setAppName(appName)
                                   .setSparkHome(sparkHome)
                                   .setJars(jars)
    for ( (name, value) <- vars) {
      sparkConf.set(name.asInstanceOf[String], value.asInstanceOf[String])
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

  /**
   * ProcessBuilder used to launch worker R processes.
   */
  def rWorkerProcessBuilder(rLibDir: String) = {
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
  def startStderrThread(proc: Process) {
    new Thread("stderr reader for R") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()
  }


  /**
   * Start a thread to write RDD data to the R process.
   */
  def startStdinThread[T](
      rLibDir: String,
      proc: Process,
      func: Array[Byte],
      dataSerialized: Boolean,
      functionDependencies: Array[Byte],
      packageNames: Array[Byte],
      broadcastVars: Array[Broadcast[Object]],
      iter: Iterator[T],
      numPartitions: Int,
      splitIndex: Int) : File = {

    val tempDir =
      System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir")).split(',')(0)
    val tempFile = File.createTempFile("rSpark", "out", new File(tempDir))
    val tempFileIn = File.createTempFile("rSpark", "in", new File(tempDir))

    val tempFileName = tempFile.getAbsolutePath()
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val env = SparkEnv.get

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for R") {
      override def run() {
        SparkEnv.set(env)
        val streamStd = new BufferedOutputStream(proc.getOutputStream, bufferSize)
        val printOutStd = new PrintStream(streamStd)
        printOutStd.println(tempFileName)
        printOutStd.println(rLibDir)
        printOutStd.println(tempFileIn.getAbsolutePath())
        printOutStd.flush()

        streamStd.close()

        val stream = new BufferedOutputStream(new FileOutputStream(tempFileIn), bufferSize)
        val printOut = new PrintStream(stream)
        val dataOut = new DataOutputStream(stream)

        dataOut.writeInt(splitIndex)

        dataOut.writeInt(func.length)
        dataOut.write(func, 0, func.length)

        dataOut.writeInt(if (dataSerialized) 1 else 0)

        dataOut.writeInt(functionDependencies.length)
        dataOut.write(functionDependencies, 0, functionDependencies.length)

        dataOut.writeInt(packageNames.length)
        dataOut.write(packageNames, 0, packageNames.length)

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
          if (dataSerialized) {
            val elemArr = elem.asInstanceOf[Array[Byte]]
            dataOut.writeInt(elemArr.length)
            dataOut.write(elemArr, 0, elemArr.length)
          } else {
            printOut.println(elem)
          }
        }

        printOut.flush()
        dataOut.flush()
        stream.flush()
        stream.close()
      }
    }.start()

    tempFile
  }
}
