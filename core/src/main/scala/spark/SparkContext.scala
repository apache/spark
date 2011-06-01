package spark

import java.io._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.SequenceFileInputFormat


class SparkContext(
  master: String,
  frameworkName: String,
  val sparkHome: String = null,
  val jars: Seq[String] = Nil)
extends Logging {
  // Ensure logging is initialized before we spawn any threads
  initLogging()

  // Set Spark master host and port system properties
  if (System.getProperty("spark.master.host") == null)
    System.setProperty("spark.master.host", Utils.localHostName)
  if (System.getProperty("spark.master.port") == null)
    System.setProperty("spark.master.port", "50501")
  
  // Create the Spark execution environment (cache, map output tracker, etc)
  val env = SparkEnv.createFromSystemProperties(true)
  SparkEnv.set(env)
  Broadcast.initialize(true)
    
  // Create and start the scheduler
  private var scheduler: Scheduler = {
    // Regular expression used for local[N] master format
    val LOCAL_N_REGEX = """local\[([0-9]+)\]""".r
    master match {
      case "local" =>
        new LocalScheduler(1)
      case LOCAL_N_REGEX(threads) =>
        new LocalScheduler(threads.toInt)
      case _ =>
        System.loadLibrary("mesos")
        new MesosScheduler(this, master, frameworkName)
    }
  }
  scheduler.start()

  private val isLocal = scheduler.isInstanceOf[LocalScheduler]

  // Methods for creating RDDs

  def parallelize[T: ClassManifest](seq: Seq[T], numSlices: Int): RDD[T] =
    new ParallelArray[T](this, seq, numSlices)

  def parallelize[T: ClassManifest](seq: Seq[T]): RDD[T] =
    parallelize(seq, numCores)
    
  def makeRDD[T: ClassManifest](seq: Seq[T], numSlices: Int): RDD[T] =
    parallelize(seq, numSlices)

  def makeRDD[T: ClassManifest](seq: Seq[T]): RDD[T] =
    parallelize(seq, numCores)

  def textFile(path: String): RDD[String] =
    new HadoopTextFile(this, path)

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat */
  def hadoopFile[K, V](path: String,
                       inputFormatClass: Class[_ <: InputFormat[K, V]],
                       keyClass: Class[K],
                       valueClass: Class[V])
      : RDD[(K, V)] = {
    new HadoopFile(this, path, inputFormatClass, keyClass, valueClass)
  }

  /**
   * Smarter version of hadoopFile() that uses class manifests to figure out
   * the classes of keys, values and the InputFormat so that users don't need
   * to pass them directly.
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassManifest[K], vm: ClassManifest[V], fm: ClassManifest[F])
      : RDD[(K, V)] = {
    hadoopFile(path,
               fm.erasure.asInstanceOf[Class[F]],
               km.erasure.asInstanceOf[Class[K]],
               vm.erasure.asInstanceOf[Class[V]])
  }

  /** Get an RDD for a Hadoop SequenceFile with given key and value types */
  def sequenceFile[K, V](path: String,
                         keyClass: Class[K],
                         valueClass: Class[V]): RDD[(K, V)] = {
    val inputFormatClass = classOf[SequenceFileInputFormat[K, V]]
    hadoopFile(path, inputFormatClass, keyClass, valueClass)
  }

  /**
   * Smarter version of sequenceFile() that obtains the key and value classes
   * from ClassManifests instead of requiring the user to pass them directly.
   */
  def sequenceFile[K, V](path: String)
      (implicit km: ClassManifest[K], vm: ClassManifest[V]): RDD[(K, V)] = {
    sequenceFile(path,
                 km.erasure.asInstanceOf[Class[K]],
                 vm.erasure.asInstanceOf[Class[V]])
  }

  /** Build the union of a list of RDDs. */
  //def union[T: ClassManifest](rdds: RDD[T]*): RDD[T] =
  //  new UnionRDD(this, rdds)

  // Methods for creating shared variables

  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  // Keep around a weak hash map of values to Cached versions?
  def broadcast[T](value: T) = 
    Broadcast.getBroadcastFactory.newBroadcast[T] (value, isLocal)

  // Stop the SparkContext
  def stop() {
     scheduler.stop()
     scheduler = null
     // TODO: Broadcast.stop(), Cache.stop()?
     env.mapOutputTracker.stop()
     env.cacheTracker.stop()
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
    if (sparkHome != null)
      Some(sparkHome)
    else if (System.getProperty("spark.home") != null)
      Some(System.getProperty("spark.home"))
    else if (System.getenv("SPARK_HOME") != null)
      Some(System.getenv("SPARK_HOME"))
    else
      None
  }

  private[spark] def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int])
                                 (implicit m: ClassManifest[U])
      : Array[U] = {
    logInfo("Starting job...")
    val start = System.nanoTime
    val result = scheduler.runJob(rdd, func, partitions)
    logInfo("Job finished in " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  private[spark] def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U)
                                 (implicit m: ClassManifest[U])
      : Array[U] = {
    runJob(rdd, func, 0 until rdd.splits.size)
  }

  // Clean a closure to make it ready to serialized and send to tasks
  // (removes unreferenced variables in $outer's, updates REPL variables)
  private[spark] def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }

  // Get the number of cores available to run tasks (as reported by Scheduler)
  def numCores = scheduler.numCores

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
 * The SparkContext object contains a number of implicit conversions and
 * parameters for use with various Spark features.
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

  implicit def rddToPairRDDExtras[K, V](rdd: RDD[(K, V)]) =
    new PairRDDExtras(rdd)
}
