package spark.api.python

import java.io._

import scala.collection.Map
import scala.collection.JavaConversions._
import scala.io.Source
import spark._
import api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import scala.{collection, Some}
import collection.parallel.mutable
import scala.collection
import scala.Some

trait PythonRDDBase {
  def compute[T](split: Split, envVars: Map[String, String],
    command: Seq[String], parent: RDD[T], pythonExec: String): Iterator[Array[Byte]] = {
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
        for (elem <- command) {
          out.println(elem)
        }
        out.flush()
        val dOut = new DataOutputStream(proc.getOutputStream)
        for (elem <- parent.iterator(split)) {
          if (elem.isInstanceOf[Array[Byte]]) {
            val arr = elem.asInstanceOf[Array[Byte]]
            dOut.writeInt(arr.length)
            dOut.write(arr)
          } else if (elem.isInstanceOf[scala.Tuple2[_, _]]) {
            val t = elem.asInstanceOf[scala.Tuple2[_, _]]
            val t1 = t._1.asInstanceOf[Array[Byte]]
            val t2 = t._2.asInstanceOf[Array[Byte]]
            val length = t1.length + t2.length - 3 - 3 + 4  // stripPickle() removes 3 bytes
            dOut.writeInt(length)
            dOut.writeByte(Pickle.PROTO)
            dOut.writeByte(Pickle.TWO)
            dOut.write(PythonRDD.stripPickle(t1))
            dOut.write(PythonRDD.stripPickle(t2))
            dOut.writeByte(Pickle.TUPLE2)
            dOut.writeByte(Pickle.STOP)
          } else if (elem.isInstanceOf[String]) {
            // For uniformity, strings are wrapped into Pickles.
            val s = elem.asInstanceOf[String].getBytes("UTF-8")
            val length = 2 + 1 + 4 + s.length + 1
            dOut.writeInt(length)
            dOut.writeByte(Pickle.PROTO)
            dOut.writeByte(Pickle.TWO)
            dOut.writeByte(Pickle.BINUNICODE)
            dOut.writeInt(Integer.reverseBytes(s.length))
            dOut.write(s)
            dOut.writeByte(Pickle.STOP)
          } else {
            throw new Exception("Unexpected RDD type")
          }
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
          case eof: EOFException => { new Array[Byte](0) }
          case e => throw e
        }
      }

      var _nextObj = read()

      def hasNext = _nextObj.length != 0
    }
  }
}

class PythonRDD[T: ClassManifest](
  parent: RDD[T], command: Seq[String], envVars: Map[String, String],
  preservePartitoning: Boolean, pythonExec: String)
  extends RDD[Array[Byte]](parent.context) with PythonRDDBase {

  def this(parent: RDD[T], command: Seq[String], preservePartitoning: Boolean, pythonExec: String) =
    this(parent, command, Map(), preservePartitoning, pythonExec)

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String, preservePartitoning: Boolean, pythonExec: String) =
    this(parent, PipedRDD.tokenize(command), preservePartitoning, pythonExec)

  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Split): Iterator[Array[Byte]] =
    compute(split, envVars, command, parent, pythonExec)

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}

class PythonPairRDD[T: ClassManifest] (
  parent: RDD[T], command: Seq[String], envVars: Map[String, String],
  preservePartitoning: Boolean, pythonExec: String)
  extends RDD[(Array[Byte], Array[Byte])](parent.context) with PythonRDDBase {

  def this(parent: RDD[T], command: Seq[String], preservePartitoning: Boolean, pythonExec: String) =
    this(parent, command, Map(), preservePartitoning, pythonExec)

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String, preservePartitoning: Boolean, pythonExec: String) =
    this(parent, PipedRDD.tokenize(command), preservePartitoning, pythonExec)

  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Split): Iterator[(Array[Byte], Array[Byte])] = {
    compute(split, envVars, command, parent, pythonExec).grouped(2).map {
      case Seq(a, b) => (a, b)
      case x          => throw new Exception("PythonPairRDD: unexpected value: " + x)
    }
  }

  val asJavaPairRDD : JavaPairRDD[Array[Byte], Array[Byte]] = JavaPairRDD.fromRDD(this)
}


object PythonRDD {

  /** Strips the pickle PROTO and STOP opcodes from the start and end of a pickle */
  def stripPickle(arr: Array[Byte]) : Array[Byte] = {
    arr.slice(2, arr.length - 1)
  }

  def asPickle(elem: Any) : Array[Byte] = {
    val baos = new ByteArrayOutputStream();
    val dOut = new DataOutputStream(baos);
    if (elem.isInstanceOf[Array[Byte]]) {
      elem.asInstanceOf[Array[Byte]]
    } else if (elem.isInstanceOf[scala.Tuple2[_, _]]) {
      val t = elem.asInstanceOf[scala.Tuple2[_, _]]
      val t1 = t._1.asInstanceOf[Array[Byte]]
      val t2 = t._2.asInstanceOf[Array[Byte]]
      dOut.writeByte(Pickle.PROTO)
      dOut.writeByte(Pickle.TWO)
      dOut.write(PythonRDD.stripPickle(t1))
      dOut.write(PythonRDD.stripPickle(t2))
      dOut.writeByte(Pickle.TUPLE2)
      dOut.writeByte(Pickle.STOP)
      baos.toByteArray()
    } else if (elem.isInstanceOf[String]) {
      // For uniformity, strings are wrapped into Pickles.
      val s = elem.asInstanceOf[String].getBytes("UTF-8")
      dOut.writeByte(Pickle.PROTO)
      dOut.writeByte(Pickle.TWO)
      dOut.write(Pickle.BINUNICODE)
      dOut.writeInt(Integer.reverseBytes(s.length))
      dOut.write(s)
      dOut.writeByte(Pickle.STOP)
      baos.toByteArray()
    } else {
      throw new Exception("Unexpected RDD type")
    }
  }

  def pickleFile(sc: JavaSparkContext, filename: String, parallelism: Int) :
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

  def arrayAsPickle(arr : Any) : Array[Byte] = {
    val pickles : Array[Byte] = arr.asInstanceOf[Array[Any]].map(asPickle).map(stripPickle).flatten

    Array[Byte](Pickle.PROTO, Pickle.TWO, Pickle.EMPTY_LIST, Pickle.MARK) ++ pickles ++
      Array[Byte] (Pickle.APPENDS, Pickle.STOP)
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
class ExtractValue extends spark.api.java.function.Function[(Array[Byte],
  Array[Byte]), Array[Byte]] {

  override def call(pair: (Array[Byte], Array[Byte])) : Array[Byte] = pair._2

}
