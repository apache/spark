package org.apache.spark.api.r

import java.io._
import org.apache.spark.api.java.{JavaSparkContext, JavaRDD}

import scala.io.Source
import scala.collection.JavaConversions._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PipedRDD

class RRDD[T: ClassManifest](
    parent: RDD[T],
    func: Array[Byte],
    dataSerialized: Boolean,
    functionDependencies: Array[Byte])
  extends RDD[Array[Byte]](parent) with Logging {

  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {

    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    // val depsFileName = new File(depsFile).getName
    // val localDepsFile = SparkFiles.get(depsFileName)

    val rCommand = "Rscript"
    val rOptions = "--vanilla"
    val sparkHome = new ProcessBuilder().environment().get("SPARK_HOME")
    val rExecScript = sparkHome + "/R/pkg/inst/worker/worker.R"
    val pb = new ProcessBuilder(List(rCommand, rOptions, rExecScript))

    val proc = pb.start()
    val env = SparkEnv.get

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

        dataOut.writeInt(func.length)
        dataOut.write(func, 0, func.length)

        dataOut.writeInt(if(dataSerialized) 1 else 0)

        // dataOut.writeInt(localDepsFile.length)
        // dataOut.writeBytes(localDepsFile)

        dataOut.writeInt(functionDependencies.length)
        dataOut.write(functionDependencies, 0, functionDependencies.length)

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
    //val reader = new BufferedReader(new InputStreamReader(inputStream))
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
          // logError("READ length " + length)
          // val lengthStr = Option(dataStream.readLine()).getOrElse("0").trim()
          // var length = 0
          // try {
          //   length = lengthStr.toInt
          // } catch {
          //   case nfe: NumberFormatException =>
          // }

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
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
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
}
