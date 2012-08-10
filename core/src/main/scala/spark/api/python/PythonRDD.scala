package spark.api.python

import java.io.PrintWriter

import scala.collection.Map
import scala.collection.JavaConversions._
import scala.io.Source
import spark._
import api.java.{JavaPairRDD, JavaRDD}
import scala.Some

trait PythonRDDBase {
  def compute[T](split: Split, envVars: Map[String, String],
    command: Seq[String], parent: RDD[T], pythonExec: String): Iterator[String]= {
    val currentEnvVars = new ProcessBuilder().environment()
    val SPARK_HOME = currentEnvVars.get("SPARK_HOME")

    val pb = new ProcessBuilder(Seq(pythonExec, SPARK_HOME + "/pyspark/pyspark/worker.py"))
    // Add the environmental variables to the process.
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
        for (elem <- parent.iterator(split)) {
          out.println(PythonRDD.pythonDump(elem))
        }
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val lines: Iterator[String] = Source.fromInputStream(proc.getInputStream).getLines
    wrapIterator(lines, proc)
  }

  def wrapIterator[T](iter: Iterator[T], proc: Process): Iterator[T] = {
    return new Iterator[T] {
      def next() = iter.next()

      def hasNext = {
        if (iter.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            throw new Exception("Subprocess exited with status " + exitStatus)
          }
          false
        }
      }
    }
  }
}

class PythonRDD[T: ClassManifest](
  parent: RDD[T], command: Seq[String], envVars: Map[String, String],
  preservePartitoning: Boolean, pythonExec: String)
  extends RDD[String](parent.context) with PythonRDDBase {

  def this(parent: RDD[T], command: Seq[String], preservePartitoning: Boolean, pythonExec: String) =
    this(parent, command, Map(), preservePartitoning, pythonExec)

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String, preservePartitoning: Boolean, pythonExec: String) =
    this(parent, PipedRDD.tokenize(command), preservePartitoning, pythonExec)

  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Split): Iterator[String] =
    compute(split, envVars, command, parent, pythonExec)

  val asJavaRDD : JavaRDD[String] = JavaRDD.fromRDD(this)
}

class PythonPairRDD[T: ClassManifest] (
  parent: RDD[T], command: Seq[String], envVars: Map[String, String],
  preservePartitoning: Boolean, pythonExec: String)
  extends RDD[(String, String)](parent.context) with PythonRDDBase {

  def this(parent: RDD[T], command: Seq[String], preservePartitoning: Boolean, pythonExec: String) =
    this(parent, command, Map(), preservePartitoning, pythonExec)

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String, preservePartitoning: Boolean, pythonExec: String) =
    this(parent, PipedRDD.tokenize(command), preservePartitoning, pythonExec)

  override def splits = parent.splits

  override val dependencies = List(new OneToOneDependency(parent))

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Split): Iterator[(String, String)] = {
    compute(split, envVars, command, parent, pythonExec).grouped(2).map {
      case Seq(a, b) => (a, b)
      case x          => throw new Exception("Unexpected value: " + x)
    }
  }

  val asJavaPairRDD : JavaPairRDD[String, String] = JavaPairRDD.fromRDD(this)
}

object PythonRDD {
  def pythonDump[T](x: T): String = {
    if (x.isInstanceOf[scala.Option[_]]) {
      val t = x.asInstanceOf[scala.Option[_]]
      t match {
        case None => "*"
        case Some(z) => pythonDump(z)
      }
    } else if (x.isInstanceOf[scala.Tuple2[_, _]]) {
      val t = x.asInstanceOf[scala.Tuple2[_, _]]
      "(" + pythonDump(t._1) + "," + pythonDump(t._2) + ")"
    } else if (x.isInstanceOf[java.util.List[_]]) {
      val objs = asScalaBuffer(x.asInstanceOf[java.util.List[_]]).map(pythonDump)
      "[" + objs.mkString("|") + "]"
    } else {
      x.toString
    }
  }
}
