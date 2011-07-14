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
import org.apache.hadoop.mapred.HadoopFileWriter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text

import SparkContext._


/**
 * Extra functions available on RDDs of (key, value) pairs to create a Hadoop SequenceFile,
 * through an implicit conversion. Note that this can't be part of PairRDDFunctions because
 * we need more implicit parameters to convert our keys and values to Writable.
 */
@serializable
class SequenceFileRDDFunctions[K <% Writable: ClassManifest, V <% Writable : ClassManifest](self: RDD[(K,V)]) extends Logging { 
  def getWritableClass[T <% Writable: ClassManifest](): Class[_ <: Writable] = {
    val c = {
      if (classOf[Writable].isAssignableFrom(classManifest[T].erasure)) 
        classManifest[T].erasure
      else
        implicitly[T => Writable].getClass.getMethods()(0).getReturnType
       // TODO: use something like WritableConverter to avoid reflection
    }
    c.asInstanceOf[Class[ _ <: Writable]]
  }

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

  def lookup(key: K): Seq[V] = {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        def process(it: Iterator[(K, V)]): Seq[V] = {
          val buf = new ArrayBuffer[V]
          for ((k, v) <- it if k == key)
            buf += v
          buf
        }
        val res = self.context.runJob(self, process _, Array(index))
        res(0)
      case None =>
        throw new UnsupportedOperationException("lookup() called on an RDD without a partitioner")
    }
  }
}
