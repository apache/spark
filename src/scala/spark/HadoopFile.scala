package spark

import mesos.SlaveOffer

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.ReflectionUtils

/** A Spark split class that wraps around a Hadoop InputSplit */
@serializable class HadoopSplit(@transient s: InputSplit)
extends Split {
  val inputSplit = new SerializableWritable[InputSplit](s)

  // Hadoop gives each split a unique toString value, so use this as our ID
  override def getId() = "HadoopSplit(" + inputSplit.toString + ")"
}


/**
 * An RDD that reads a Hadoop file (from HDFS, S3, the local filesystem, etc)
 * and represents it as a set of key-value pairs using a given InputFormat.
 */
class HadoopFile[K, V](
  sc: SparkContext,
  path: String,
  inputFormatClass: Class[_ <: InputFormat[K, V]],
  keyClass: Class[K],
  valueClass: Class[V])
extends RDD[(K, V)](sc) {
  @transient val splits_ : Array[Split] = ConfigureLock.synchronized {
    val conf = new JobConf()
    FileInputFormat.setInputPaths(conf, path)
    val inputFormat = createInputFormat(conf)
    val inputSplits = inputFormat.getSplits(conf, sc.scheduler.numCores)
    inputSplits.map(x => new HadoopSplit(x): Split).toArray
  }

  def createInputFormat(conf: JobConf): InputFormat[K, V] = {
    ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
                   .asInstanceOf[InputFormat[K, V]]
  }

  override def splits = splits_

  override def iterator(theSplit: Split) = new Iterator[(K, V)] {
    val split = theSplit.asInstanceOf[HadoopSplit]
    var reader: RecordReader[K, V] = null

    ConfigureLock.synchronized {
      val conf = new JobConf()
      val bufferSize = System.getProperty("spark.buffer.size", "65536")
      conf.set("io.file.buffer.size", bufferSize)
      val fmt = createInputFormat(conf)
      reader = fmt.getRecordReader(split.inputSplit.value, conf, Reporter.NULL)
    }

    val key: K = keyClass.newInstance()
    val value: V = valueClass.newInstance()
    var gotNext = false
    var finished = false

    override def hasNext: Boolean = {
      if (!gotNext) {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eofe: java.io.EOFException =>
            finished = true
        }
        gotNext = true
      }
      !finished
    }

    override def next: (K, V) = {
      if (!gotNext) {
        finished = !reader.next(key, value)
      }
      if (finished) {
        throw new java.util.NoSuchElementException("End of stream")
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
}


/**
 * Convenience class for Hadoop files read using TextInputFormat that
 * represents the file as an RDD of Strings.
 */
class HadoopTextFile(sc: SparkContext, path: String)
extends MappedRDD[String, (LongWritable, Text)](
  new HadoopFile(sc, path, classOf[TextInputFormat],
                 classOf[LongWritable], classOf[Text]),
  { pair: (LongWritable, Text) => pair._2.toString }
)


/**
 * Object used to ensure that only one thread at a time is configuring Hadoop
 * InputFormat classes. Apparently configuring them is not thread safe!
 */
object ConfigureLock {}
