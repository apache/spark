package org.apache.spark.api.r

import java.io._
import scala.io.Source
import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD, JavaPairRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
 * Form an RDD[(Array[Byte], Array[Byte])] from key-value pairs returned from R.
 * This is used by SparkR's shuffle operations.
 */
private class PairwiseRRDD(
    parent: JavaPairRDD[Array[Byte], Array[Byte]],
    numPartitions: Int,
    hashFunc: Array[Byte],
    dataSerialized: Boolean,
    functionDependencies: Array[Byte])
  extends RDD[(Array[Byte], Array[Byte])](parent.rdd) {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[(Array[Byte], Array[Byte])] = {

    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val pb = SparkRHelper.rWorkerProcessBuilder

    val proc = pb.start()
    val env = SparkEnv.get

    val tempDir = Utils.getLocalDir
    val tempFile = File.createTempFile("rSpark", "out", new File(tempDir))
    val tempFileName = tempFile.getAbsolutePath

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for R") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for R") {
      override def run() {
        SparkEnv.set(env)
        val stream = new BufferedOutputStream(proc.getOutputStream, bufferSize)
        val printOut = new PrintStream(stream)
        val dataOut = new DataOutputStream(stream)

        printOut.println(tempFileName)

        dataOut.writeInt(hashFunc.length)
        dataOut.write(hashFunc, 0, hashFunc.length)

        dataOut.writeInt(if (dataSerialized) 1 else 0)

        dataOut.writeInt(functionDependencies.length)
        dataOut.write(functionDependencies, 0, functionDependencies.length)

        dataOut.writeInt(parent.iterator(split, context).length)

        // TODO: is it okay to use parent as opposed to firstParent?
        parent.iterator(split, context).foreach {
          case (keyBytes: Array[Byte], valBytes: Array[Byte]) =>
            if (dataSerialized) {
              dataOut.writeInt(keyBytes.length)
              dataOut.write(keyBytes, 0, keyBytes.length)
              dataOut.writeInt(valBytes.length)
              dataOut.write(valBytes, 0, valBytes.length)
            } else {
              // FIXME: is it possible / do we allow that an RDD[(Array[Byte], Array[Byte])] has dataSerialized == false?
              printOut.println(keyBytes)
              printOut.println(valBytes)
            }
          case _ => throw new SparkException("PairwiseRRDD: unexpected element (not (Array[Byte], Array[Bytes]))")
        }
        stream.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val inputStream = new BufferedReader(new InputStreamReader(proc.getInputStream))
    val stdOutFileName = inputStream.readLine().trim()

    val dataStream = new DataInputStream(new FileInputStream(stdOutFileName))

    return new Iterator[(Array[Byte], Array[Byte])] {
      def next(): (Array[Byte], Array[Byte]) = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): (Array[Byte], Array[Byte]) = {
        try {
          val length = dataStream.readInt()

          length match {
            case length if length == 2 =>
              val hashedKeyLength = dataStream.readInt()
              val hashedKey = new Array[Byte](hashedKeyLength)
              dataStream.read(hashedKey, 0, hashedKeyLength)
              val contentPairsLength = dataStream.readInt()
              val contentPairs = new Array[Byte](contentPairsLength)
              dataStream.read(contentPairs, 0, contentPairsLength)
              (hashedKey, contentPairs)
            case _ => (new Array[Byte](0), new Array[Byte](0))   // End of input
          }
        } catch {
          case eof: EOFException => {
            throw new SparkException("R worker exited unexpectedly (crashed)", eof)
          }
          case e => throw e
        }
      }
      var _nextObj = read()

      def hasNext = !(_nextObj._1.length == 0 && _nextObj._2.length == 0)
    }
  }

  lazy val asJavaPairRDD : JavaPairRDD[Array[Byte], Array[Byte]] = JavaPairRDD.fromRDD(this)

}

/** An RDD that stores serialized R objects as Array[Byte]. */
class RRDD[T: ClassManifest](
    parent: RDD[T],
    func: Array[Byte],
    dataSerialized: Boolean,
    functionDependencies: Array[Byte])
  extends RDD[Array[Byte]](parent) with Logging {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val pb = SparkRHelper.rWorkerProcessBuilder

    val proc = pb.start()
    val env = SparkEnv.get

    val tempDir = Utils.getLocalDir
    val tempFile =  File.createTempFile("rSpark", "out", new File(tempDir))
    val tempFileName = tempFile.getAbsolutePath

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for R") {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for R") {
      override def run() {
        SparkEnv.set(env)
        val stream = new BufferedOutputStream(proc.getOutputStream, bufferSize)
        val printOut = new PrintStream(stream)
        val dataOut = new DataOutputStream(stream)

        printOut.println(tempFileName)

        dataOut.writeInt(func.length)
        dataOut.write(func, 0, func.length)

        dataOut.writeInt(if(dataSerialized) 1 else 0)

        dataOut.writeInt(functionDependencies.length)
        dataOut.write(functionDependencies, 0, functionDependencies.length)

        // Special flag that tells the worker that I am a normal RRDD.
        dataOut.writeInt(-1)

        for (elem <- firstParent[T].iterator(split, context)) {
          if (dataSerialized) {
            val elemArr = elem.asInstanceOf[Array[Byte]]
            dataOut.writeInt(elemArr.length)
            dataOut.write(elemArr, 0, elemArr.length)
          } else {
            printOut.println(elem)
          }
        }
        stream.close()
      }
    }.start()

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
          case e => throw e
        }
      }
      var _nextObj = read()

      def hasNext = _nextObj.length != 0
    }
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}


object RRDD {

  /**
   * Create an RRDD given a sequence of byte arrays. Used to create RRDD when `parallelize` is
   * called from R.
   */
  def createRDDFromArray(jsc: JavaSparkContext, arr: Array[Array[Byte]]): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(jsc.sc.parallelize(arr, arr.length))
  }

  /**
   * Create an RRDD given a sequence of 2-tuples of byte arrays (key-val collections). Used to create RRDD when
   * `parallelize` is called from R.
   * TODO?: change return type into JavaPairRDD[Array[Byte], Array[Byte]]?
   */
  def createRDDFromArray(jsc: JavaSparkContext,
                         arr: Array[Array[Array[Array[Byte]]]]): JavaPairRDD[Array[Byte], Array[Byte]] = {

    val keyValPairs: Seq[(Array[Byte], Array[Byte])] =
      for (
        slice <- arr;
        tup <- slice
      ) yield (tup(0), tup(1))

    JavaPairRDD.fromRDD(jsc.sc.parallelize(keyValPairs, arr.length))

  }

}

object SparkRHelper {

  lazy val rWorkerProcessBuilder = {
    val rCommand = "Rscript"
    val rOptions = "--vanilla"
    val sparkHome = Option(new ProcessBuilder().environment().get("SPARK_HOME")) match {
      case Some(path) => path
      case None => sys.error("SPARK_HOME not set as an environment variable.")
    }
    val rExecScript = sparkHome + "/R/pkg/inst/worker/worker.R"
    new ProcessBuilder(List(rCommand, rOptions, rExecScript))
  }

}
