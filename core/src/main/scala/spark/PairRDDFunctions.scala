package spark

import java.io.EOFException
import java.net.URL
import java.io.ObjectInputStream
import java.util.concurrent.atomic.AtomicLong
import java.util.{HashMap => JHashMap}
import java.util.Date
import java.text.SimpleDateFormat

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputCommitter
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.HadoopWriter
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCommitter
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat => NewFileOutputFormat}
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.mapreduce.{RecordWriter => NewRecordWriter}
import org.apache.hadoop.mapreduce.{Job => NewAPIHadoopJob}
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.TaskType

import spark.SparkContext._
import spark.partial.BoundedDouble
import spark.partial.PartialResult

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 */
class PairRDDFunctions[K: ClassManifest, V: ClassManifest](
    self: RDD[(K, V)])
  extends Logging
  with Serializable {

  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numSplits: Int,
      partitioner: Partitioner): RDD[(K, C)] = {
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
    new ShuffledRDD(self, aggregator, partitioner)
  }

  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numSplits: Int): RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, numSplits,
        new HashPartitioner(numSplits))
  }

  def reduceByKey(func: (V, V) => V, numSplits: Int): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, numSplits)
  }
  
  def reduceByKeyLocally(func: (V, V) => V): Map[K, V] = {
    def reducePartition(iter: Iterator[(K, V)]): Iterator[JHashMap[K, V]] = {
      val map = new JHashMap[K, V]
      for ((k, v) <- iter) {
        val old = map.get(k)
        map.put(k, if (old == null) v else func(old, v))
      }
      Iterator(map)
    }

    def mergeMaps(m1: JHashMap[K, V], m2: JHashMap[K, V]): JHashMap[K, V] = {
      for ((k, v) <- m2) {
        val old = m1.get(k)
        m1.put(k, if (old == null) v else func(old, v))
      }
      return m1
    }

    self.mapPartitions(reducePartition).reduce(mergeMaps)
  }

  // Alias for backwards compatibility
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = reduceByKeyLocally(func)

  // TODO: This should probably be a distributed version
  def countByKey(): Map[K, Long] = self.map(_._1).countByValue()

  // TODO: This should probably be a distributed version
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
      : PartialResult[Map[K, BoundedDouble]] = {
    self.map(_._1).countByValueApprox(timeout, confidence)
  }

  def groupByKey(numSplits: Int): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, numSplits)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  def groupByKey(partitioner: Partitioner): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner.numPartitions, partitioner)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  def partitionBy(partitioner: Partitioner): RDD[(K, V)] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner.numPartitions, partitioner)
    bufs.flatMapValues(buf => buf)
  }

  def join[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, W))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += w
        })
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def leftOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, Option[W]))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[V]
        val wbuf = new ArrayBuffer[Option[W]]
        seq.foreach(_ match {
          case Left(v) => vbuf += v
          case Right(w) => wbuf += Some(w)
        })
        if (wbuf.isEmpty) {
          wbuf += None
        }
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def rightOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Option[V], W))] = {
    val vs: RDD[(K, Either[V, W])] = self.map { case (k, v) => (k, Left(v)) }
    val ws: RDD[(K, Either[V, W])] = other.map { case (k, w) => (k, Right(w)) }
    (vs ++ ws).groupByKey(numSplits).flatMap {
      case (k, seq) => {
        val vbuf = new ArrayBuffer[Option[V]]
        val wbuf = new ArrayBuffer[W]
        seq.foreach(_ match {
          case Left(v) => vbuf += Some(v)
          case Right(w) => wbuf += w
        })
        if (vbuf.isEmpty) {
          vbuf += None
        }
        for (v <- vbuf; w <- wbuf) yield (k, (v, w))
      }
    }
  }

  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C) : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultParallelism)
  }

  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(func, defaultParallelism)
  }

  def groupByKey(): RDD[(K, Seq[V])] = {
    groupByKey(defaultParallelism)
  }

  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    join(other, defaultParallelism)
  }

  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, defaultParallelism)
  }

  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, defaultParallelism)
  }

  def defaultParallelism = self.context.defaultParallelism

  def collectAsMap(): Map[K, V] = HashMap(self.collect(): _*)
  
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }
  
  def flatMapValues[U](f: V => Traversable[U]): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesRDD(self, cleanF)
  }
  
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    val part = self.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(defaultParallelism)
    }
    val cg = new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]], other.asInstanceOf[RDD[(_, _)]]),
        part)
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(
        classManifest[K],
        Manifests.seqSeqManifest)
    prfs.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }
  
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val part = self.partitioner match {
      case Some(p) => p
      case None => new HashPartitioner(defaultParallelism)
    }
    new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]],
            other1.asInstanceOf[RDD[(_, _)]], 
            other2.asInstanceOf[RDD[(_, _)]]),
        part).map {
      case (k, Seq(vs, w1s, w2s)) =>
        (k, (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]]))
    }
  }

  def lookup(key: K): Seq[V] = {
    self.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        def process(it: Iterator[(K, V)]): Seq[V] = {
          val buf = new ArrayBuffer[V]
          for ((k, v) <- it if k == key) {
            buf += v
          }
          buf
        }
        val res = self.context.runJob(self, process _, Array(index), false)
        res(0)
      case None =>
        throw new UnsupportedOperationException("lookup() called on an RDD without a partitioner")
    }
  }

  def saveAsHadoopFile [F <: OutputFormat[K, V]] (path: String) (implicit fm: ClassManifest[F]) {
    saveAsHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }
  
  def saveAsNewAPIHadoopFile [F <: NewOutputFormat[K, V]] (path: String) (implicit fm: ClassManifest[F]) {
    saveAsNewAPIHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]]) {
    val job = new NewAPIHadoopJob
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    NewFileOutputFormat.setOutputPath(job, new Path(path))
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    def writeShard(context: spark.TaskContext, iter: Iterator[(K,V)]): Int = {
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = new TaskAttemptID(jobtrackerID,
        stageId, TaskType.REDUCE, context.splitId, context.attemptId)
      val hadoopContext = new TaskAttemptContextImpl(wrappedConf.value, attemptId)
      val format = outputFormatClass.newInstance
      val committer = format.getOutputCommitter(hadoopContext)
      committer.setupTask(hadoopContext)
      val writer = format.getRecordWriter(hadoopContext).asInstanceOf[NewRecordWriter[K,V]]
      while (iter.hasNext) {
        val (k, v) = iter.next
        writer.write(k, v)
      }
      writer.close(hadoopContext)
      committer.commitTask(hadoopContext)
      return 1
    }
    val jobFormat = outputFormatClass.newInstance
    /* apparently we need a TaskAttemptID to construct an OutputCommitter;
     * however we're only going to use this local OutputCommitter for
     * setupJob/commitJob, so we just use a dummy "map" task.
     */
    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, TaskType.MAP, 0, 0)
    val jobTaskContext = new TaskAttemptContextImpl(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    val count = self.context.runJob(self, writeShard _).sum
    jobCommitter.cleanupJob(jobTaskContext)
  }

  def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf) {
    conf.setOutputKeyClass(keyClass)
    conf.setOutputValueClass(valueClass)
    // conf.setOutputFormat(outputFormatClass) // Doesn't work in Scala 2.9 due to what may be a generics bug
    conf.set("mapred.output.format.class", outputFormatClass.getName)
    conf.setOutputCommitter(classOf[FileOutputCommitter])
    FileOutputFormat.setOutputPath(conf, HadoopWriter.createPathFromString(path, conf))
    saveAsHadoopDataset(conf)
  }
  
  def saveAsHadoopDataset(conf: JobConf) {
    val outputFormatClass = conf.getOutputFormat
    val keyClass = conf.getOutputKeyClass
    val valueClass = conf.getOutputValueClass
    if (outputFormatClass == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    
    logInfo("Saving as hadoop file of type (" + keyClass.getSimpleName+ ", " + valueClass.getSimpleName+ ")")

    val writer = new HadoopWriter(conf)
    writer.preSetup()

    def writeToFile(context: TaskContext, iter: Iterator[(K,V)]) {
      writer.setup(context.stageId, context.splitId, context.attemptId)
      writer.open()
      
      var count = 0
      while(iter.hasNext) {
        val record = iter.next
        count += 1
        writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])
      }
    
      writer.close()
      writer.commit()
    }

    self.context.runJob(self, writeToFile _)
    writer.cleanup()
  }

  def getKeyClass() = implicitly[ClassManifest[K]].erasure

  def getValueClass() = implicitly[ClassManifest[V]].erasure
}

class OrderedRDDFunctions[K <% Ordered[K]: ClassManifest, V: ClassManifest](
  self: RDD[(K, V)])
  extends Logging 
  with Serializable {

  def sortByKey(ascending: Boolean = true): RDD[(K,V)] = {
    val rangePartitionedRDD = self.partitionBy(new RangePartitioner(self.splits.size, self, ascending))
    new SortedRDD(rangePartitionedRDD, ascending)
  }
}

class SortedRDD[K <% Ordered[K], V](prev: RDD[(K, V)], ascending: Boolean)
  extends RDD[(K, V)](prev.context) {

  override def splits = prev.splits
  override val partitioner = prev.partitioner
  override val dependencies = List(new OneToOneDependency(prev))
  override def compute(split: Split) = {
    prev.iterator(split).toArray
      .sortWith((x, y) => if (ascending) x._1 < y._1 else x._1 > y._1).iterator
  }
} 
 
class MappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => U) extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) = prev.iterator(split).map{case (k, v) => (k, f(v))}
}

class FlatMappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => Traversable[U])
  extends RDD[(K, U)](prev.context) {
  
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) = {
    prev.iterator(split).toStream.flatMap { 
      case (k, v) => f(v).map(x => (k, x))
    }.iterator
  }
}

object Manifests {
  val seqSeqManifest = classManifest[Seq[Seq[_]]]
}
