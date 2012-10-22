package spark

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import java.net.{URI, URLClassLoader}

import scala.collection.Map
import scala.collection.generic.Growable
import scala.collection.mutable.{ArrayBuffer, HashMap}

import akka.actor.Actor
import akka.actor.Actor._
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.mesos.{Scheduler, MesosNativeLibrary}

import spark.broadcast._
import spark.deploy.LocalSparkCluster
import spark.partial.ApproximateEvaluator
import spark.partial.PartialResult
import spark.rdd.HadoopRDD
import spark.rdd.NewHadoopRDD
import spark.rdd.UnionRDD
import spark.scheduler.ShuffleMapTask
import spark.scheduler.DAGScheduler
import spark.scheduler.TaskScheduler
import spark.scheduler.local.LocalScheduler
import spark.scheduler.cluster.{SparkDeploySchedulerBackend, SchedulerBackend, ClusterScheduler}
import spark.scheduler.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import spark.storage.BlockManagerMaster

/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
 * @param jobName A name for your job, to display on the cluster web UI.
 * @param sparkHome Location where Spark is installed on cluster nodes.
 * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
 *             system or HDFS, HTTP, HTTPS, or FTP URLs.
 * @param environment Environment variables to set on worker nodes.
 */
class SparkContext(
    master: String,
    jobName: String,
    val sparkHome: String,
    jars: Seq[String],
    environment: Map[String, String])
  extends Logging {

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param jobName A name for your job, to display on the cluster web UI
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(master: String, jobName: String, sparkHome: String, jars: Seq[String]) =
    this(master, jobName, sparkHome, jars, Map())

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param jobName A name for your job, to display on the cluster web UI
   */
  def this(master: String, jobName: String) = this(master, jobName, null, Nil, Map())

  // Ensure logging is initialized before we spawn any threads
  initLogging()

  // Set Spark master host and port system properties
  if (System.getProperty("spark.master.host") == null) {
    System.setProperty("spark.master.host", Utils.localIpAddress())
  }
  if (System.getProperty("spark.master.port") == null) {
    System.setProperty("spark.master.port", "0")
  }

  private val isLocal = (master == "local" || master.startsWith("local["))

  // Create the Spark execution environment (cache, map output tracker, etc)
  private[spark] val env = SparkEnv.createFromSystemProperties(
    System.getProperty("spark.master.host"),
    System.getProperty("spark.master.port").toInt,
    true,
    isLocal)
  SparkEnv.set(env)

  // Used to store a URL for each static file/jar together with the file's local timestamp
  private[spark] val addedFiles = HashMap[String, Long]()
  private[spark] val addedJars = HashMap[String, Long]()

  // Add each JAR given through the constructor
  jars.foreach { addJar(_) }

  // Environment variables to pass to our executors
  private[spark] val executorEnvs = HashMap[String, String]()
  for (key <- Seq("SPARK_MEM", "SPARK_CLASSPATH", "SPARK_LIBRARY_PATH", "SPARK_JAVA_OPTS",
       "SPARK_TESTING")) {
    val value = System.getenv(key)
    if (value != null) {
      executorEnvs(key) = value
    }
  }
  executorEnvs ++= environment

  // Create and start the scheduler
  private var taskScheduler: TaskScheduler = {
    // Regular expression used for local[N] master format
    val LOCAL_N_REGEX = """local\[([0-9]+)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """(spark://.*)""".r

    master match {
      case "local" =>
        new LocalScheduler(1, 0, this)

      case LOCAL_N_REGEX(threads) =>
        new LocalScheduler(threads.toInt, 0, this)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        new LocalScheduler(threads.toInt, maxFailures.toInt, this)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new ClusterScheduler(this)
        val backend = new SparkDeploySchedulerBackend(scheduler, this, sparkUrl, jobName)
        scheduler.initialize(backend)
        scheduler

      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure SPARK_MEM <= memoryPerSlave. Otherwise Spark will just hang.
        val memoryPerSlaveInt = memoryPerSlave.toInt
        val sparkMemEnv = System.getenv("SPARK_MEM")
        val sparkMemEnvInt = if (sparkMemEnv != null) Utils.memoryStringToMb(sparkMemEnv) else 512
        if (sparkMemEnvInt > memoryPerSlaveInt) {
          throw new SparkException(
            "Slave memory (%d MB) cannot be smaller than SPARK_MEM (%d MB)".format(
              memoryPerSlaveInt, sparkMemEnvInt))
        }

        val scheduler = new ClusterScheduler(this)
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt)
        val sparkUrl = localCluster.start()
        val backend = new SparkDeploySchedulerBackend(scheduler, this, sparkUrl, jobName)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: SparkDeploySchedulerBackend) => {
          localCluster.stop()
        }
        scheduler

      case _ =>
        MesosNativeLibrary.load()
        val scheduler = new ClusterScheduler(this)
        val coarseGrained = System.getProperty("spark.mesos.coarse", "false").toBoolean
        val backend = if (coarseGrained) {
          new CoarseMesosSchedulerBackend(scheduler, this, master, jobName)
        } else {
          new MesosSchedulerBackend(scheduler, this, master, jobName)
        }
        scheduler.initialize(backend)
        scheduler
    }
  }
  taskScheduler.start()

  private var dagScheduler = new DAGScheduler(taskScheduler)

  // Methods for creating RDDs

  /** Distribute a local Scala collection to form an RDD. */
  def parallelize[T: ClassManifest](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }

  /** Distribute a local Scala collection to form an RDD. */
  def makeRDD[T: ClassManifest](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    parallelize(seq, numSlices)
  }

  /** 
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(path: String, minSplits: Int = defaultMinSplits): RDD[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], minSplits)
      .map(pair => pair._2.toString)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   * etc).
   */
  def hadoopRDD[K, V](
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minSplits: Int = defaultMinSplits
      ): RDD[(K, V)] = {
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minSplits)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minSplits: Int = defaultMinSplits
      ) : RDD[(K, V)] = {
    val conf = new JobConf()
    FileInputFormat.setInputPaths(conf, path)
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    conf.set("io.file.buffer.size", bufferSize)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minSplits)
  }

  /**
   * Smarter version of hadoopFile() that uses class manifests to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minSplits)
   * }}}
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String, minSplits: Int)
      (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F])
      : RDD[(K, V)] = {
    hadoopFile(path,
        fm.erasure.asInstanceOf[Class[F]],
        km.erasure.asInstanceOf[Class[K]],
        vm.erasure.asInstanceOf[Class[V]],
        minSplits)
  }

  /**
   * Smarter version of hadoopFile() that uses class manifests to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F]): RDD[(K, V)] =
    hadoopFile[K, V, F](path, defaultMinSplits)

  /** Get an RDD for a Hadoop file with an arbitrary new API InputFormat. */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](path: String)
      (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F]): RDD[(K, V)] = {
    newAPIHadoopFile(
        path,
        fm.erasure.asInstanceOf[Class[F]],
        km.erasure.asInstanceOf[Class[K]],
        vm.erasure.asInstanceOf[Class[V]],
        new Configuration)
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration): RDD[(K, V)] = {
    val job = new NewHadoopJob(conf)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(this, fClass, kClass, vClass, updatedConf)
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      conf: Configuration,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V]): RDD[(K, V)] = {
    new NewHadoopRDD(this, fClass, kClass, vClass, conf)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types. */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minSplits: Int
      ): RDD[(K, V)] = {
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minSplits)
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types. */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]): RDD[(K, V)] =
    sequenceFile(path, keyClass, valueClass, defaultMinSplits)

  /**
   * Version of sequenceFile() for types implicitly convertible to Writables through a
   * WritableConverter. For example, to access a SequenceFile where the keys are Text and the
   * values are IntWritable, you could simply write
   * {{{
   * sparkContext.sequenceFile[String, Int](path, ...)
   * }}}
   *
   * WritableConverters are provided in a somewhat strange way (by an implicit function) to support
   * both subclasses of Writable and types for which we define a converter (e.g. Int to
   * IntWritable). The most natural thing would've been to have implicit objects for the
   * converters, but then we couldn't have an object for every subclass of Writable (you can't
   * have a parameterized singleton object). We use functions instead to create a new converter
   * for the appropriate type. In addition, we pass the converter a ClassManifest of its type to
   * allow it to figure out the Writable class to use in the subclass case.
   */
   def sequenceFile[K, V](path: String, minSplits: Int = defaultMinSplits)
      (implicit km: ClassManifest[K], vm: ClassManifest[V],
          kcf: () => WritableConverter[K], vcf: () => WritableConverter[V])
      : RDD[(K, V)] = {
    val kc = kcf()
    val vc = vcf()
    val format = classOf[SequenceFileInputFormat[Writable, Writable]]
    val writables = hadoopFile(path, format,
        kc.writableClass(km).asInstanceOf[Class[Writable]],
        vc.writableClass(vm).asInstanceOf[Class[Writable]], minSplits)
    writables.map{case (k,v) => (kc.convert(k), vc.convert(v))}
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
   */
  def objectFile[T: ClassManifest](
      path: String,
      minSplits: Int = defaultMinSplits
      ): RDD[T] = {
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minSplits)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes))
  }

  /** Build the union of a list of RDDs. */
  def union[T: ClassManifest](rdds: Seq[RDD[T]]): RDD[T] = new UnionRDD(this, rdds)

  /** Build the union of a list of RDDs passed as variable-length arguments. */
  def union[T: ClassManifest](first: RDD[T], rest: RDD[T]*): RDD[T] =
    new UnionRDD(this, Seq(first) ++ rest)

  // Methods for creating shared variables

  /**
   * Create an [[spark.Accumulator]] variable of a given type, which tasks can "add" values
   * to using the `+=` method. Only the master can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  /**
   * Create an [[spark.Accumulable]] shared variable, with a `+=` method
   * @tparam T accumulator type
   * @tparam R type that can be added to the accumulator
   */
  def accumulable[T,R](initialValue: T)(implicit param: AccumulableParam[T,R]) =
    new Accumulable(initialValue, param)

  /**
   * Create an accumulator from a "mutable collection" type.
   *
   * Growable and TraversableOnce are the standard APIs that guarantee += and ++=, implemented by
   * standard mutable collections. So you can use this with mutable Map, Set, etc.
   */
  def accumulableCollection[R <% Growable[T] with TraversableOnce[T] with Serializable, T](initialValue: R) = {
    val param = new GrowableAccumulableParam[R,T]
    new Accumulable(initialValue, param)
  }

  /** 
   * Broadcast a read-only variable to the cluster, returning a [[spark.Broadcast]] object for
   * reading it in distributed functions. The variable will be sent to each cluster only once.
   */
  def broadcast[T](value: T) = env.broadcastManager.newBroadcast[T] (value, isLocal)

  /**
   * Add a file to be downloaded into the working directory of this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.
   */
  def addFile(path: String) {
    val uri = new URI(path)
    val key = uri.getScheme match {
      case null | "file" => env.httpFileServer.addFile(new File(uri.getPath))
      case _ => path
    }
    addedFiles(key) = System.currentTimeMillis

    // Fetch the file locally in case the task is executed locally
    val filename = new File(path.split("/").last)
    Utils.fetchFile(path, new File("."))

    logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
  }

  /**
   * Clear the job's list of files added by `addFile` so that they do not get donwloaded to
   * any new nodes.
   */
  def clearFiles() {
    addedFiles.keySet.map(_.split("/").last).foreach { k => new File(k).delete() }
    addedFiles.clear()
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.
   */
  def addJar(path: String) {
    val uri = new URI(path)
    val key = uri.getScheme match {
      case null | "file" => env.httpFileServer.addJar(new File(uri.getPath))
      case _ => path
    }
    addedJars(key) = System.currentTimeMillis
    logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
  }

  /**
   * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
   * any new nodes.
   */
  def clearJars() {
    addedJars.keySet.map(_.split("/").last).foreach { k => new File(k).delete() }
    addedJars.clear()
  }

  /** Shut down the SparkContext. */
  def stop() {
    dagScheduler.stop()
    dagScheduler = null
    taskScheduler = null
    // TODO: Cache.stop()?
    env.stop()
    // Clean up locally linked files
    clearFiles()
    clearJars()
    SparkEnv.set(null)
    ShuffleMapTask.clearCache()
    logInfo("Successfully stopped SparkContext")
  }

  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
   */
  private[spark] def getSparkHome(): Option[String] = {
    if (sparkHome != null) {
      Some(sparkHome)
    } else if (System.getProperty("spark.home") != null) {
      Some(System.getProperty("spark.home"))
    } else if (System.getenv("SPARK_HOME") != null) {
      Some(System.getenv("SPARK_HOME"))
    } else {
      None
    }
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results. This is the main
   * entry point to the scheduler, by which all actions get launched. The allowLocal flag specifies
   * whether the scheduler can run the computation on the master rather than shipping it out to the
   * cluster, for short actions like first().
   */
  def runJob[T, U: ClassManifest](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    val callSite = Utils.getSparkCallSite
    logInfo("Starting job: " + callSite)
    val start = System.nanoTime
    val result = dagScheduler.runJob(rdd, func, partitions, callSite, allowLocal)
    logInfo("Job finished: " + callSite + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * Run a job on a given set of partitions of an RDD, but take a function of type
   * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
   */
  def runJob[T, U: ClassManifest](
      rdd: RDD[T],
      func: Iterator[T] => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    runJob(rdd, (context: TaskContext, iter: Iterator[T]) => func(iter), partitions, allowLocal)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassManifest](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.splits.size, false)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassManifest](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.splits.size, false)
  }

  /**
   * Run a job that can return approximate results.
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long
      ): PartialResult[R] = {
    val callSite = Utils.getSparkCallSite
    logInfo("Starting job: " + callSite)
    val start = System.nanoTime
    val result = dagScheduler.runApproximateJob(rdd, func, evaluator, callSite, timeout)
    logInfo("Job finished: " + callSite + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * Clean a closure to make it ready to serialized and send to tasks
   * (removes unreferenced variables in $outer's, updates REPL variables)
   */
  private[spark] def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }

  /** Default level of parallelism to use when not given by user (e.g. for reduce tasks) */
  def defaultParallelism: Int = taskScheduler.defaultParallelism

  /** Default min number of splits for Hadoop RDDs when not given by user */
  def defaultMinSplits: Int = math.min(defaultParallelism, 2)

  private var nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private var nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()
}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
 */
object SparkContext {
  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double) = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int) = 0
  }

  // TODO: Add AccumulatorParams for other types, e.g. lists and strings

  implicit def rddToPairRDDFunctions[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)]) =
    new PairRDDFunctions(rdd)

  implicit def rddToSequenceFileRDDFunctions[K <% Writable: ClassManifest, V <% Writable: ClassManifest](
      rdd: RDD[(K, V)]) =
    new SequenceFileRDDFunctions(rdd)

  implicit def rddToOrderedRDDFunctions[K <% Ordered[K]: ClassManifest, V: ClassManifest](
      rdd: RDD[(K, V)]) =
    new OrderedRDDFunctions(rdd)

  implicit def doubleRDDToDoubleRDDFunctions(rdd: RDD[Double]) = new DoubleRDDFunctions(rdd)

  implicit def numericRDDToDoubleRDDFunctions[T](rdd: RDD[T])(implicit num: Numeric[T]) =
    new DoubleRDDFunctions(rdd.map(x => num.toDouble(x)))

  // Implicit conversions to common Writable types, for saveAsSequenceFile

  implicit def intToIntWritable(i: Int) = new IntWritable(i)

  implicit def longToLongWritable(l: Long) = new LongWritable(l)

  implicit def floatToFloatWritable(f: Float) = new FloatWritable(f)

  implicit def doubleToDoubleWritable(d: Double) = new DoubleWritable(d)

  implicit def boolToBoolWritable (b: Boolean) = new BooleanWritable(b)

  implicit def bytesToBytesWritable (aob: Array[Byte]) = new BytesWritable(aob)

  implicit def stringToText(s: String) = new Text(s)

  private implicit def arrayToArrayWritable[T <% Writable: ClassManifest](arr: Traversable[T]): ArrayWritable = {
    def anyToWritable[U <% Writable](u: U): Writable = u

    new ArrayWritable(classManifest[T].erasure.asInstanceOf[Class[Writable]],
        arr.map(x => anyToWritable(x)).toArray)
  }

  // Helper objects for converting common types to Writable
  private def simpleWritableConverter[T, W <: Writable: ClassManifest](convert: W => T) = {
    val wClass = classManifest[W].erasure.asInstanceOf[Class[W]]
    new WritableConverter[T](_ => wClass, x => convert(x.asInstanceOf[W]))
  }

  implicit def intWritableConverter() = simpleWritableConverter[Int, IntWritable](_.get)

  implicit def longWritableConverter() = simpleWritableConverter[Long, LongWritable](_.get)

  implicit def doubleWritableConverter() = simpleWritableConverter[Double, DoubleWritable](_.get)

  implicit def floatWritableConverter() = simpleWritableConverter[Float, FloatWritable](_.get)

  implicit def booleanWritableConverter() = simpleWritableConverter[Boolean, BooleanWritable](_.get)

  implicit def bytesWritableConverter() = simpleWritableConverter[Array[Byte], BytesWritable](_.getBytes)

  implicit def stringWritableConverter() = simpleWritableConverter[String, Text](_.toString)

  implicit def writableWritableConverter[T <: Writable]() =
    new WritableConverter[T](_.erasure.asInstanceOf[Class[T]], _.asInstanceOf[T])

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext
   */
  def jarOfClass(cls: Class[_]): Seq[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class", so pull out the /path/foo.jar
        List(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        Nil
      }
    } else {
      Nil
    }
  }

  /** Find the JAR that contains the class of a particular object */
  def jarOfObject(obj: AnyRef): Seq[String] = jarOfClass(obj.getClass)
}


/**
 * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
 * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
 * The getter for the writable class takes a ClassManifest[T] in case this is a generic object
 * that doesn't know the type of T when it is created. This sounds strange but is necessary to
 * support converting subclasses of Writable to themselves (writableWritableConverter).
 */
private[spark] class WritableConverter[T](
    val writableClass: ClassManifest[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable
