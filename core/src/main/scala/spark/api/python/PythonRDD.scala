package spark.api.python

import java.io._
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.io.Source

import spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import spark.broadcast.Broadcast
import spark.SparkEnv
import spark.Split
import spark.RDD
import spark.OneToOneDependency
import spark.rdd.PipedRDD


private[spark] class PythonRDD[T: ClassManifest](
  parent: RDD[T], command: Seq[String], envVars: java.util.Map[String, String],
  preservePartitoning: Boolean, pythonExec: String, broadcastVars: java.util.List[Broadcast[Array[Byte]]])
  extends RDD[Array[Byte]](parent.context) {

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String, envVars: java.util.Map[String, String],
    preservePartitoning: Boolean, pythonExec: String,
    broadcastVars: java.util.List[Broadcast[Array[Byte]]]) =
    this(parent, PipedRDD.tokenize(command), envVars, preservePartitoning, pythonExec,
      broadcastVars)

  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Split): Iterator[Array[Byte]] = {
    val SPARK_HOME = new ProcessBuilder().environment().get("SPARK_HOME")

    val pb = new ProcessBuilder(Seq(pythonExec, SPARK_HOME + "/pyspark/pyspark/worker.py"))
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()

    envVars.foreach {
      case (variable, value) => currentEnvVars.put(variable, value)
    }

    val proc = pb.start()
    val env = SparkEnv.get

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + command) {
      override def run() {
        SparkEnv.set(env)
        val out = new PrintWriter(proc.getOutputStream)
        val dOut = new DataOutputStream(proc.getOutputStream)
        dOut.writeInt(broadcastVars.length)
        for (broadcast <- broadcastVars) {
          dOut.writeLong(broadcast.id)
          dOut.writeInt(broadcast.value.length)
          dOut.write(broadcast.value)
          dOut.flush()
        }
        for (elem <- command) {
          out.println(elem)
        }
        out.flush()
        for (elem <- parent.iterator(split)) {
          PythonRDD.writeAsPickle(elem, dOut)
        }
        dOut.flush()
        out.flush()
        proc.getOutputStream.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(proc.getInputStream)
    return new Iterator[Array[Byte]] {
      def next() = {
        val obj = _nextObj
        _nextObj = read()
        obj
      }

      private def read() = {
        try {
          val length = stream.readInt()
          val obj = new Array[Byte](length)
          stream.readFully(obj)
          obj
        } catch {
          case eof: EOFException => {
            val exitStatus = proc.waitFor()
            if (exitStatus != 0) {
              throw new Exception("Subprocess exited with status " + exitStatus)
            }
            new Array[Byte](0)
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

private class PairwiseRDD(prev: RDD[Array[Byte]]) extends
  RDD[(Array[Byte], Array[Byte])](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) =
    prev.iterator(split).grouped(2).map {
      case Seq(a, b) => (a, b)
      case x          => throw new Exception("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Array[Byte], Array[Byte]] = JavaPairRDD.fromRDD(this)
}

private[spark] object PythonRDD {

  /** Strips the pickle PROTO and STOP opcodes from the start and end of a pickle */
  def stripPickle(arr: Array[Byte]) : Array[Byte] = {
    arr.slice(2, arr.length - 1)
  }

  /**
   * Write strings, pickled Python objects, or pairs of pickled objects to a data output stream.
   * The data format is a 32-bit integer representing the pickled object's length (in bytes),
   * followed by the pickled data.
   * @param elem the object to write
   * @param dOut a data output stream
   */
  def writeAsPickle(elem: Any, dOut: DataOutputStream) {
    if (elem.isInstanceOf[Array[Byte]]) {
      val arr = elem.asInstanceOf[Array[Byte]]
      dOut.writeInt(arr.length)
      dOut.write(arr)
    } else if (elem.isInstanceOf[scala.Tuple2[Array[Byte], Array[Byte]]]) {
      val t = elem.asInstanceOf[scala.Tuple2[Array[Byte], Array[Byte]]]
      val length = t._1.length + t._2.length - 3 - 3 + 4  // stripPickle() removes 3 bytes
      dOut.writeInt(length)
      dOut.writeByte(Pickle.PROTO)
      dOut.writeByte(Pickle.TWO)
      dOut.write(PythonRDD.stripPickle(t._1))
      dOut.write(PythonRDD.stripPickle(t._2))
      dOut.writeByte(Pickle.TUPLE2)
      dOut.writeByte(Pickle.STOP)
    } else if (elem.isInstanceOf[String]) {
      // For uniformity, strings are wrapped into Pickles.
      val s = elem.asInstanceOf[String].getBytes("UTF-8")
      val length = 2 + 1 + 4 + s.length + 1
      dOut.writeInt(length)
      dOut.writeByte(Pickle.PROTO)
      dOut.writeByte(Pickle.TWO)
      dOut.write(Pickle.BINUNICODE)
      dOut.writeInt(Integer.reverseBytes(s.length))
      dOut.write(s)
      dOut.writeByte(Pickle.STOP)
    } else {
      throw new Exception("Unexpected RDD type")
    }
  }

  def readRDDFromPickleFile(sc: JavaSparkContext, filename: String, parallelism: Int) :
  JavaRDD[Array[Byte]] = {
    val file = new DataInputStream(new FileInputStream(filename))
    val objs = new collection.mutable.ArrayBuffer[Array[Byte]]
    try {
      while (true) {
        val length = file.readInt()
        val obj = new Array[Byte](length)
        file.readFully(obj)
        objs.append(obj)
      }
    } catch {
      case eof: EOFException => {}
      case e => throw e
    }
    JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
  }

  def writeIteratorToPickleFile[T](items: java.util.Iterator[T], filename: String) {
    val file = new DataOutputStream(new FileOutputStream(filename))
    for (item <- items) {
      writeAsPickle(item, file)
    }
    file.close()
  }
}

private object Pickle {
  def b(x: Int): Byte = x.asInstanceOf[Byte]
  val PROTO: Byte = b(0x80)
  val TWO: Byte = b(0x02)
  val BINUNICODE : Byte = 'X'
  val STOP : Byte = '.'
  val TUPLE2 : Byte = b(0x86)
  val EMPTY_LIST : Byte = ']'
  val MARK : Byte = '('
  val APPENDS : Byte = 'e'
}

private class ExtractValue extends spark.api.java.function.Function[(Array[Byte],
  Array[Byte]), Array[Byte]] {
  override def call(pair: (Array[Byte], Array[Byte])) : Array[Byte] = pair._2
}

private class BytesToString extends spark.api.java.function.Function[Array[Byte], String] {
  override def call(arr: Array[Byte]) : String = new String(arr, "UTF-8")
}
