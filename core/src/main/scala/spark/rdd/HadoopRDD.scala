package spark.rdd

import java.io.EOFException
import java.util.NoSuchElementException

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.ReflectionUtils

import spark.Dependency
import spark.RDD
import spark.SerializableWritable
import spark.SparkContext
import spark.Split

/** 
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class HadoopSplit(rddId: Int, idx: Int, @transient s: InputSplit)
  extends Split
  with Serializable {
  
  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index: Int = idx
}

/**
 * An RDD that reads a Hadoop dataset as specified by a JobConf (e.g. files in HDFS, the local file
 * system, or S3, tables in HBase, etc).
 */
class HadoopRDD[K, V](
    sc: SparkContext,
    @transient conf: JobConf,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends RDD[(K, V)](sc) {
  
  // A Hadoop JobConf can be about 10 KB, which is pretty big, so broadcast it
  val confBroadcast = sc.broadcast(new SerializableWritable(conf))
  
  @transient
  val splits_ : Array[Split] = {
    val inputFormat = createInputFormat(conf)
    val inputSplits = inputFormat.getSplits(conf, minSplits)
    val array = new Array[Split](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopSplit(id, i, inputSplits(i))
    }
    array
  }

  def createInputFormat(conf: JobConf): InputFormat[K, V] = {
    ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
  }

  override def splits = splits_

  override def compute(theSplit: Split) = new Iterator[(K, V)] {
    val split = theSplit.asInstanceOf[HadoopSplit]
    var reader: RecordReader[K, V] = null

    val conf = confBroadcast.value.value
    val fmt = createInputFormat(conf)
    reader = fmt.getRecordReader(split.inputSplit.value, conf, Reporter.NULL)

    val key: K = reader.createKey()
    val value: V = reader.createValue()
    var gotNext = false
    var finished = false

    override def hasNext: Boolean = {
      if (!gotNext) {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eof: EOFException =>
            finished = true
        }
        gotNext = true
      }
      if (finished) {
        reader.close()
      }
      !finished
    }

    override def next: (K, V) = {
      if (!gotNext) {
        finished = !reader.next(key, value)
      }
      if (finished) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      (key, value)
    }
  }

  override def preferredLocations(split: Split) = {
    // TODO: Filtering out "localhost" in case of file:// URLs
    val hadoopSplit = split.asInstanceOf[HadoopSplit]
    hadoopSplit.inputSplit.value.getLocations.filter(_ != "localhost")
  }
  
  override val dependencies: List[Dependency[_]] = Nil
}
