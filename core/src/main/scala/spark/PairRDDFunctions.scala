package spark

import java.io.EOFException
import java.io.ObjectInputStream
import java.net.URL
import java.util.{Date, HashMap => JHashMap}
import java.util.concurrent.atomic.AtomicLong
import java.text.SimpleDateFormat

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
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

import spark.partial.BoundedDouble
import spark.partial.PartialResult
import spark.rdd._
import spark.SparkContext._

/**
 * Extra functions available on RDDs of (key, value) pairs through an implicit conversion.
 * Import `spark.SparkContext._` at the top of your program to use these functions.
 */
class PairRDDFunctions[K: ClassManifest, V: ClassManifest](
    self: RDD[(K, V)])
  extends Logging
  with Serializable {

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation 
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   * 
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true): RDD[(K, C)] = {
    val aggregator =
      if (mapSideCombine) {
        new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)
      } else {
        // Don't apply map-side combiner.
        // A sanity check to make sure mergeCombiners is not defined.
        assert(mergeCombiners == null)
        new Aggregator[K, V, C](createCombiner, mergeValue, null, false)
      }
    new ShuffledAggregatedRDD(self, aggregator, partitioner)
  }

  /**
   * Simplified version of combineByKey that hash-partitions the output RDD.
   */
  def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      numSplits: Int): RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numSplits))
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative reduce function, but return the results
   * immediately to the master as a Map. This will also perform the merging locally on each mapper
   * before sending results to a reducer, similarly to a "combiner" in MapReduce.
   */
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

  /** Alias for reduceByKeyLocally */
  def reduceByKeyToDriver(func: (V, V) => V): Map[K, V] = reduceByKeyLocally(func)

  /** Count the number of elements for each key, and return the result to the master as a Map. */
  def countByKey(): Map[K, Long] = self.map(_._1).countByValue()

  /** 
   * (Experimental) Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
      : PartialResult[Map[K, BoundedDouble]] = {
    self.map(_._1).countByValueApprox(timeout, confidence)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numSplits splits.
   */
  def reduceByKey(func: (V, V) => V, numSplits: Int): RDD[(K, V)] = {
    reduceByKey(new HashPartitioner(numSplits), func)
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   */
  def groupByKey(partitioner: Partitioner): RDD[(K, Seq[V])] = {
    def createCombiner(v: V) = ArrayBuffer(v)
    def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
    def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
    val bufs = combineByKey[ArrayBuffer[V]](
      createCombiner _, mergeValue _, mergeCombiners _, partitioner)
    bufs.asInstanceOf[RDD[(K, Seq[V])]]
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numSplits` partitions.
   */
  def groupByKey(numSplits: Int): RDD[(K, Seq[V])] = {
    groupByKey(new HashPartitioner(numSplits))
  }

  /**
   * Return a copy of the RDD partitioned using the specified partitioner. If `mapSideCombine`
   * is true, Spark will group values of the same key together on the map side before the
   * repartitioning, to only send each key over the network once. If a large number of
   * duplicated keys are expected, and the size of the keys are large, `mapSideCombine` should
   * be set to true.
   */
  def partitionBy(partitioner: Partitioner, mapSideCombine: Boolean = false): RDD[(K, V)] = {
    if (mapSideCombine) {
      def createCombiner(v: V) = ArrayBuffer(v)
      def mergeValue(buf: ArrayBuffer[V], v: V) = buf += v
      def mergeCombiners(b1: ArrayBuffer[V], b2: ArrayBuffer[V]) = b1 ++= b2
      val bufs = combineByKey[ArrayBuffer[V]](
        createCombiner _, mergeValue _, mergeCombiners _, partitioner)
      bufs.flatMapValues(buf => buf)
    } else {
      new RepartitionShuffledRDD(self, partitioner)
    }
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
    }
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def leftOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, Option[W]))] = {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        if (ws.isEmpty) {
          vs.iterator.map(v => (v, None))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (v, Some(w))
        }
    }
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, ws) =>
        if (vs.isEmpty) {
          ws.iterator.map(w => (None, w))
        } else {
          for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), w)
        }
    }
  }

  /** 
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the default
   * parallelism level.
   */
  def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C)
      : RDD[(K, C)] = {
    combineByKey(createCombiner, mergeValue, mergeCombiners, defaultPartitioner(self))
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the default parallelism level.
   */
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the default parallelism level.
   */
  def groupByKey(): RDD[(K, Seq[V])] = {
    groupByKey(defaultPartitioner(self))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))] = {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, W))] = {
    join(other, new HashPartitioner(numSplits))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the default level of parallelism.
   */
  def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numSplits` partitions.
   */
  def leftOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (V, Option[W]))] = {
    leftOuterJoin(other, new HashPartitioner(numSplits))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the default parallelism level.
   */
  def rightOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, defaultPartitioner(self, other))
  }

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Option[V], W))] = {
    rightOuterJoin(other, new HashPartitioner(numSplits))
  }

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   */
  def collectAsMap(): Map[K, V] = HashMap(self.collect(): _*)

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U](f: V => U): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesRDD(self, cleanF)
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  def flatMapValues[U](f: V => TraversableOnce[U]): RDD[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesRDD(self, cleanF)
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (Seq[V], Seq[W]))] = {
    val cg = new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]], other.asInstanceOf[RDD[(_, _)]]),
        partitioner)
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classManifest[K], Manifests.seqSeqManifest)
    prfs.mapValues {
      case Seq(vs, ws) =>
        (vs.asInstanceOf[Seq[V]], ws.asInstanceOf[Seq[W]])
    }
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], partitioner: Partitioner)
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    val cg = new CoGroupedRDD[K](
        Seq(self.asInstanceOf[RDD[(_, _)]],
            other1.asInstanceOf[RDD[(_, _)]],
            other2.asInstanceOf[RDD[(_, _)]]),
        partitioner)
    val prfs = new PairRDDFunctions[K, Seq[Seq[_]]](cg)(classManifest[K], Manifests.seqSeqManifest)
    prfs.mapValues {
      case Seq(vs, w1s, w2s) =>
        (vs.asInstanceOf[Seq[V]], w1s.asInstanceOf[Seq[W1]], w2s.asInstanceOf[Seq[W2]])
    }
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: RDD[(K, W)], numSplits: Int): RDD[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, new HashPartitioner(numSplits))
  }

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)], numSplits: Int)
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    cogroup(other1, other2, new HashPartitioner(numSplits))
  }

  /** Alias for cogroup. */
  def groupWith[W](other: RDD[(K, W)]): RDD[(K, (Seq[V], Seq[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
      : RDD[(K, (Seq[V], Seq[W1], Seq[W2]))] = {
    cogroup(other1, other2, defaultPartitioner(self, other1, other2))
  }

  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs. If any of
   * the RDDs already has a partitioner, choose that one, otherwise use a default HashPartitioner.
   */
  def defaultPartitioner(rdds: RDD[_]*): Partitioner = {
    for (r <- rdds if r.partitioner != None) {
      return r.partitioner.get
    }
    return new HashPartitioner(self.context.defaultParallelism)
  }

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   */
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

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   */
  def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassManifest[F]) {
    saveAsHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[K, V]](path: String)(implicit fm: ClassManifest[F]) {
    saveAsNewAPIHadoopFile(path, getKeyClass, getValueClass, fm.erasure.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]]) {
    saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, new Configuration)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a new Hadoop API `OutputFormat`
   * (mapreduce.OutputFormat) object supporting the key and value types K and V in this RDD.
   */
  def saveAsNewAPIHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      conf: Configuration) {
    val job = new NewAPIHadoopJob(conf)
    job.setOutputKeyClass(keyClass)
    job.setOutputValueClass(valueClass)
    val wrappedConf = new SerializableWritable(job.getConfiguration)
    NewFileOutputFormat.setOutputPath(job, new Path(path))
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = self.id
    def writeShard(context: spark.TaskContext, iter: Iterator[(K,V)]): Int = {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt
      /* "reduce task" <split #> <attempt # = spark task #> */
      val attemptId = new TaskAttemptID(jobtrackerID,
        stageId, false, context.splitId, attemptNumber)
      val hadoopContext = new TaskAttemptContext(wrappedConf.value, attemptId)
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
    val jobAttemptId = new TaskAttemptID(jobtrackerID, stageId, true, 0, 0)
    val jobTaskContext = new TaskAttemptContext(wrappedConf.value, jobAttemptId)
    val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    val count = self.context.runJob(self, writeShard _).sum
    jobCommitter.cleanupJob(jobTaskContext)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   */
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

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   */
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
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.splitId, attemptNumber)
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

  private[spark] def getKeyClass() = implicitly[ClassManifest[K]].erasure

  private[spark] def getValueClass() = implicitly[ClassManifest[V]].erasure
}

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. Import `spark.SparkContext._` at the top of your program to use these
 * functions. They will work with any key type that has a `scala.math.Ordered` implementation.
 */
class OrderedRDDFunctions[K <% Ordered[K]: ClassManifest, V: ClassManifest](
  self: RDD[(K, V)])
  extends Logging
  with Serializable {

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(ascending: Boolean = true, numSplits: Int = self.splits.size): RDD[(K,V)] = {
    new ShuffledSortedRDD(self, ascending, numSplits)
  }
}

private[spark]
class MappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => U) extends RDD[(K, U)](prev.context) {
  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner
  override def compute(split: Split) = prev.iterator(split).map{case (k, v) => (k, f(v))}
}

private[spark]
class FlatMappedValuesRDD[K, V, U](prev: RDD[(K, V)], f: V => TraversableOnce[U])
  extends RDD[(K, U)](prev.context) {

  override def splits = prev.splits
  override val dependencies = List(new OneToOneDependency(prev))
  override val partitioner = prev.partitioner

  override def compute(split: Split) = {
    prev.iterator(split).flatMap { case (k, v) => f(v).map(x => (k, x)) }
  }
}

private[spark] object Manifests {
  val seqSeqManifest = classManifest[Seq[Seq[_]]]
}
