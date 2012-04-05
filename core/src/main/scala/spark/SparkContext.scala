package spark

import java.io._
import java.util.concurrent.atomic.AtomicInteger

import scala.actors.remote.RemoteActor
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
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

import org.apache.mesos.MesosNativeLibrary

import spark.broadcast._

class SparkContext(
    master: String,
    frameworkName: String,
    val sparkHome: String = null,
    val jars: Seq[String] = Nil)
  extends Logging {
  
  // Ensure logging is initialized before we spawn any threads
  initLogging()

  // Set Spark master host and port system properties
  if (System.getProperty("spark.master.host") == null) {
    System.setProperty("spark.master.host", Utils.localIpAddress)
  }
  if (System.getProperty("spark.master.port") == null) {
    System.setProperty("spark.master.port", "7077")
  }

  // Make sure a proper class loader is set for remote actors (unless user set one)
  if (RemoteActor.classLoader == null) {
    RemoteActor.classLoader = getClass.getClassLoader
  }
  
  // Create the Spark execution environment (cache, map output tracker, etc)
  val env = SparkEnv.createFromSystemProperties(true)
  SparkEnv.set(env)
  Broadcast.initialize(true)

  // Create and start the scheduler
  private var scheduler: Scheduler = {
    // Regular expression used for local[N] master format
    val LOCAL_N_REGEX = """local\[([0-9]+)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+),([0-9]+)\]""".r
    master match {
      case "local" => 
        new LocalScheduler(1, 0)
      case LOCAL_N_REGEX(threads) => 
        new LocalScheduler(threads.toInt, 0)
      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        new LocalScheduler(threads.toInt, maxFailures.toInt)
      case _ =>
        MesosNativeLibrary.load()
        new MesosScheduler(this, master, frameworkName)
    }
  }
  scheduler.start()

  private val isLocal = scheduler.isInstanceOf[LocalScheduler]

  // Methods for creating RDDs

  def parallelize[T: ClassManifest](seq: Seq[T], numSlices: Int = defaultParallelism ): RDD[T] = {
    new ParallelCollection[T](this, seq, numSlices)
  }
    
  def makeRDD[T: ClassManifest](seq: Seq[T], numSlices: Int = defaultParallelism ): RDD[T] = {
    parallelize(seq, numSlices)
  }

  def textFile(path: String, minSplits: Int = defaultMinSplits): RDD[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], minSplits)
      .map(pair => pair._2.toString)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadooop JobConf giving its InputFormat and any
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
   * values and the InputFormat so that users don't need to pass them directly.
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

  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F]): RDD[(K, V)] =
    hadoopFile[K, V, F](path, defaultMinSplits)

  /** Get an RDD for a Hadoop file with an arbitrary new API InputFormat. */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](path: String)
      (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F]): RDD[(K, V)] = {
    val job = new NewHadoopJob
    NewFileInputFormat.addInputPath(job, new Path(path))
    val conf = job.getConfiguration
    newAPIHadoopFile(
        path,
        fm.erasure.asInstanceOf[Class[F]],
        km.erasure.asInstanceOf[Class[K]],
        vm.erasure.asInstanceOf[Class[V]],
        conf)
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
      conf: Configuration
      ): RDD[(K, V)] = new NewHadoopRDD(this, fClass, kClass, vClass, conf)

  /** Get an RDD for a Hadoop SequenceFile with given key and value types */
  def sequenceFile[K, V](path: String,
      keyClass: Class[K],
      valueClass: Class[V],
      minSplits: Int
      ): RDD[(K, V)] = {
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass, minSplits)
  }

  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]): RDD[(K, V)] =
    sequenceFile(path, keyClass, valueClass, defaultMinSplits)

  /**
   * Version of sequenceFile() for types implicitly convertible to Writables through a 
   * WritableConverter.
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
  def union[T: ClassManifest](rdds: RDD[T]*): RDD[T] = new UnionRDD(this, rdds)

  // Methods for creating shared variables

  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  // Keep around a weak hash map of values to Cached versions?
  def broadcast[T](value: T) = Broadcast.getBroadcastFactory.newBroadcast[T] (value, isLocal)

  // Stop the SparkContext
  def stop() {
    scheduler.stop()
    scheduler = null
    // TODO: Broadcast.stop(), Cache.stop()?
    env.mapOutputTracker.stop()
    env.cacheTracker.stop()
    env.shuffleFetcher.stop()
    env.shuffleManager.stop()
    SparkEnv.set(null)
  }

  // Wait for the scheduler to be registered
  def waitForRegister() {
    scheduler.waitForRegister()
  }

  // Get Spark's home location from either a value set through the constructor,
  // or the spark.home Java property, or the SPARK_HOME environment variable
  // (in that order of preference). If neither of these is set, return None.
  def getSparkHome(): Option[String] = {
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
    logInfo("Starting job...")
    val start = System.nanoTime
    val result = scheduler.runJob(rdd, func, partitions, allowLocal)
    logInfo("Job finished in " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

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

  def runJob[T, U: ClassManifest](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.splits.size, false)
  }

  // Clean a closure to make it ready to serialized and send to tasks
  // (removes unreferenced variables in $outer's, updates REPL variables)
  private[spark] def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }

  // Default level of parallelism to use when not given by user (e.g. for reduce tasks)
  def defaultParallelism: Int = scheduler.defaultParallelism

  // Default min number of splits for Hadoop RDDs when not given by user
  def defaultMinSplits: Int = math.min(defaultParallelism, 2)

  private var nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = {
    nextShuffleId.getAndIncrement()
  }
  
  private var nextRddId = new AtomicInteger(0)

  // Register a new RDD, returning its RDD ID
  private[spark] def newRddId(): Int = {
    nextRddId.getAndIncrement()
  }
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

  implicit def rddToSequenceFileRDDFunctions[K <% Writable: ClassManifest, V <% Writable: ClassManifest](rdd: RDD[(K, V)]) =
    new SequenceFileRDDFunctions(rdd)

  implicit def rddToOrderedRDDFunctions[K <% Ordered[K]: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)]) =
    new OrderedRDDFunctions(rdd)

  // Implicit conversions to common Writable types, for saveAsSequenceFile

  implicit def intToIntWritable(i: Int) = new IntWritable(i)

  implicit def longToLongWritable(l: Long) = new LongWritable(l)

  implicit def floatToFloatWritable(f: Float) = new FloatWritable(f)
  
  implicit def doubleToDoubleWritable(d: Double) = new DoubleWritable(d)

  implicit def boolToBoolWritable (b: Boolean) = new BooleanWritable(b)

  implicit def bytesToBytesWritable (aob: Array[Byte]) = new BytesWritable(aob)

  implicit def stringToText(s: String) = new Text(s)

  private implicit def arrayToArrayWritable[T <% Writable: ClassManifest](arr: Traversable[T]): ArrayWritable = {
    def getWritableClass[T <% Writable: ClassManifest](): Class[_ <: Writable] = {
      val c = {
       if (classOf[Writable].isAssignableFrom(classManifest[T].erasure)) {
         classManifest[T].erasure
       } else {
         implicitly[T => Writable].getClass.getMethods()(0).getReturnType
       }
       // TODO: use something like WritableConverter to avoid reflection
      }
      c.asInstanceOf[Class[ _ <: Writable]]
    }

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

  // Find the JAR from which a given class was loaded, to make it easy for users to pass
  // their JARs to SparkContext
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
 
  // Find the JAR that contains the class of a particular object
  def jarOfObject(obj: AnyRef): Seq[String] = jarOfClass(obj.getClass)
}


/**
 * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
 * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
 * The getter for the writable class takes a ClassManifest[T] in case this is a generic object
 * that doesn't know the type of T when it is created. This sounds strange but is necessary to
 * support converting subclasses of Writable to themselves (writableWritableConverter).
 */
class WritableConverter[T](
    val writableClass: ClassManifest[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable
