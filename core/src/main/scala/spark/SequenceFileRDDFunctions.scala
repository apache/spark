package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.HashSet
import java.util.Random
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text

import spark.SparkContext._

/**
 * Extra functions available on RDDs of (key, value) pairs to create a Hadoop SequenceFile,
 * through an implicit conversion. Note that this can't be part of PairRDDFunctions because
 * we need more implicit parameters to convert our keys and values to Writable.
 *
 * Users should import `spark.SparkContext._` at the top of their program to use these functions.
 */
class SequenceFileRDDFunctions[K <% Writable: ClassManifest, V <% Writable : ClassManifest](
    self: RDD[(K, V)])
  extends Logging
  with Serializable {

  private def getWritableClass[T <% Writable: ClassManifest](): Class[_ <: Writable] = {
    val c = {
      if (classOf[Writable].isAssignableFrom(classManifest[T].erasure)) {
        classManifest[T].erasure
      } else {
        // We get the type of the Writable class by looking at the apply method which converts
        // from T to Writable. Since we have two apply methods we filter out the one which
        // is not of the form "java.lang.Object apply(java.lang.Object)"
        implicitly[T => Writable].getClass.getDeclaredMethods().filter(
            m => m.getReturnType().toString != "class java.lang.Object" &&
                 m.getName() == "apply")(0).getReturnType

      }
       // TODO: use something like WritableConverter to avoid reflection
    }
    c.asInstanceOf[Class[_ <: Writable]]
  }

  /**
   * Output the RDD as a Hadoop SequenceFile using the Writable types we infer from the RDD's key
   * and value types. If the key or value are Writable, then we use their classes directly;
   * otherwise we map primitive types such as Int and Double to IntWritable, DoubleWritable, etc,
   * byte arrays to BytesWritable, and Strings to Text. The `path` can be on any Hadoop-supported
   * file system.
   */
  def saveAsSequenceFile(path: String) {
    def anyToWritable[U <% Writable](u: U): Writable = u

    val keyClass = getWritableClass[K]
    val valueClass = getWritableClass[V]
    val convertKey = !classOf[Writable].isAssignableFrom(self.getKeyClass)
    val convertValue = !classOf[Writable].isAssignableFrom(self.getValueClass)

    logInfo("Saving as sequence file of type (" + keyClass.getSimpleName + "," + valueClass.getSimpleName + ")" )
    val format = classOf[SequenceFileOutputFormat[Writable, Writable]]
    if (!convertKey && !convertValue) {
      self.saveAsHadoopFile(path, keyClass, valueClass, format)
    } else if (!convertKey && convertValue) {
      self.map(x => (x._1,anyToWritable(x._2))).saveAsHadoopFile(path, keyClass, valueClass, format)
    } else if (convertKey && !convertValue) {
      self.map(x => (anyToWritable(x._1),x._2)).saveAsHadoopFile(path, keyClass, valueClass, format)
    } else if (convertKey && convertValue) {
      self.map(x => (anyToWritable(x._1),anyToWritable(x._2))).saveAsHadoopFile(path, keyClass, valueClass, format)
    }
  }
}
