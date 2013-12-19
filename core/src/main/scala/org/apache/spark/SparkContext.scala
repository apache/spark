/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._
import java.net.URI
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.Map
import scala.collection.generic.Growable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.reflect.{ClassTag, classTag}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileInputFormat
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.{Job => NewHadoopJob}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat => NewFileInputFormat}

import org.apache.mesos.MesosNativeLibrary

import org.apache.spark.deploy.{LocalSparkCluster, SparkHadoopUtil}
import org.apache.spark.partial.{ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend,
  SparkDeploySchedulerBackend, ClusterScheduler, SimrSchedulerBackend}
import org.apache.spark.scheduler.cluster.mesos.{CoarseMesosSchedulerBackend, MesosSchedulerBackend}
import org.apache.spark.scheduler.local.LocalScheduler
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.storage.{BlockManagerSource, RDDInfo, StorageStatus, StorageUtils}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.{ClosureCleaner, MetadataCleaner, MetadataCleanerType,
  TimeStampedHashMap, Utils}

/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
 * @param appName A name for your application, to display on the cluster web UI.
 * @param sparkHome Location where Spark is installed on cluster nodes.
 * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
 *             system or HDFS, HTTP, HTTPS, or FTP URLs.
 * @param environment Environment variables to set on worker nodes.
 */
class SparkContext(
    val master: String,
    val appName: String,
    val sparkHome: String = null,
    val jars: Seq[String] = Nil,
    val environment: Map[String, String] = Map(),
    // This is used only by YARN for now, but should be relevant to other cluster types (Mesos, etc)
    // too. This is typically generated from InputFormatInfo.computePreferredLocations .. host, set
    // of data-local splits on host
    val preferredNodeLocationData: scala.collection.Map[String, scala.collection.Set[SplitInfo]] =
      scala.collection.immutable.Map())
  extends Logging {

  // Ensure logging is initialized before we spawn any threads
  initLogging()

  // Set Spark driver host and port system properties
  if (System.getProperty("spark.driver.host") == null) {
    System.setProperty("spark.driver.host", Utils.localHostName())
  }
  if (System.getProperty("spark.driver.port") == null) {
    System.setProperty("spark.driver.port", "0")
  }

  val isLocal = (master == "local" || master.startsWith("local["))

  // Create the Spark execution environment (cache, map output tracker, etc)
  private[spark] val env = SparkEnv.createFromSystemProperties(
    "<driver>",
    System.getProperty("spark.driver.host"),
    System.getProperty("spark.driver.port").toInt,
    true,
    isLocal)
  SparkEnv.set(env)

  // Used to store a URL for each static file/jar together with the file's local timestamp
  private[spark] val addedFiles = HashMap[String, Long]()
  private[spark] val addedJars = HashMap[String, Long]()

  // Keeps track of all persisted RDDs
  private[spark] val persistentRdds = new TimeStampedHashMap[Int, RDD[_]]
  private[spark] val metadataCleaner = new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup)

  // Initialize the Spark UI
  private[spark] val ui = new SparkUI(this)
  ui.bind()

  val startTime = System.currentTimeMillis()

  // Add each JAR given through the constructor
  if (jars != null) {
    jars.foreach { addJar(_) }
  }

  // Environment variables to pass to our executors
  private[spark] val executorEnvs = HashMap[String, String]()
  // Note: SPARK_MEM is included for Mesos, but overwritten for standalone mode in ExecutorRunner
  for (key <- Seq("SPARK_CLASSPATH", "SPARK_LIBRARY_PATH", "SPARK_JAVA_OPTS", "SPARK_TESTING")) {
    val value = System.getenv(key)
    if (value != null) {
      executorEnvs(key) = value
    }
  }
  // Since memory can be set with a system property too, use that
  executorEnvs("SPARK_MEM") = SparkContext.executorMemoryRequested + "m"
  if (environment != null) {
    executorEnvs ++= environment
  }

  // Set SPARK_USER for user who is running SparkContext.
  val sparkUser = Option {
    Option(System.getProperty("user.name")).getOrElse(System.getenv("SPARK_USER"))
  }.getOrElse {
    SparkContext.SPARK_UNKNOWN_USER
  }
  executorEnvs("SPARK_USER") = sparkUser

  // Create and start the scheduler
  private[spark] var taskScheduler = SparkContext.createTaskScheduler(this, master, appName)
  taskScheduler.start()

  @volatile private[spark] var dagScheduler = new DAGScheduler(taskScheduler)
  dagScheduler.start()

  ui.start()

  /** A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse. */
  val hadoopConfiguration = {
    val env = SparkEnv.get
    val conf = SparkHadoopUtil.get.newConfiguration()
    // Explicitly check for S3 environment variables
    if (System.getenv("AWS_ACCESS_KEY_ID") != null &&
        System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
      conf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
      conf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
      conf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
      conf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    }
    // Copy any "spark.hadoop.foo=bar" system properties into conf as "foo=bar"
    Utils.getSystemProperties.foreach { case (key, value) =>
      if (key.startsWith("spark.hadoop.")) {
        conf.set(key.substring("spark.hadoop.".length), value)
      }
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    conf.set("io.file.buffer.size", bufferSize)
    conf
  }

  private[spark] var checkpointDir: Option[String] = None

  // Thread Local variable that can be used by users to pass information down the stack
  private val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = new Properties(parent)
  }

  private[spark] def getLocalProperties(): Properties = localProperties.get()

  private[spark] def setLocalProperties(props: Properties) {
    localProperties.set(props)
  }

  def initLocalProperties() {
    localProperties.set(new Properties())
  }

  def setLocalProperty(key: String, value: String) {
    if (localProperties.get() == null) {
      localProperties.set(new Properties())
    }
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).getOrElse(null)

  /** Set a human readable description of the current job. */
  @deprecated("use setJobGroup", "0.8.1")
  def setJobDescription(value: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, value)
  }

  /**
   * Assigns a group id to all the jobs started by this thread until the group id is set to a
   * different value or cleared.
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use [[org.apache.spark.SparkContext.cancelJobGroup]] to cancel all
   * running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description")
   * sc.parallelize(1 to 10000, 2).map { i => Thread.sleep(10); i }.count()
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel")
   * }}}
   */
  def setJobGroup(groupId: String, description: String) {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, description)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, groupId)
  }

  /** Clear the job group id and its description. */
  def clearJobGroup() {
    setLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION, null)
    setLocalProperty(SparkContext.SPARK_JOB_GROUP_ID, null)
  }

  // Post init
  taskScheduler.postStartHook()

  private val dagSchedulerSource = new DAGSchedulerSource(this.dagScheduler, this)
  private val blockManagerSource = new BlockManagerSource(SparkEnv.get.blockManager, this)

  def initDriverMetrics() {
    SparkEnv.get.metricsSystem.registerSource(dagSchedulerSource)
    SparkEnv.get.metricsSystem.registerSource(blockManagerSource)
  }

  initDriverMetrics()

  // Methods for creating RDDs

  /** Distribute a local Scala collection to form an RDD. */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
  }

  /** Distribute a local Scala collection to form an RDD. */
  def makeRDD[T: ClassTag](seq: Seq[T], numSlices: Int = defaultParallelism): RDD[T] = {
    parallelize(seq, numSlices)
  }

  /** Distribute a local Scala collection to form an RDD, with one or more
    * location preferences (hostnames of Spark nodes) for each object.
    * Create a new partition for each collection item. */
   def makeRDD[T: ClassTag](seq: Seq[(T, Seq[String])]): RDD[T] = {
    val indexToPrefs = seq.zipWithIndex.map(t => (t._2, t._1._2)).toMap
    new ParallelCollectionRDD[T](this, seq.map(_._1), seq.size, indexToPrefs)
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
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and any
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
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)
    new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minSplits)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minSplits: Int = defaultMinSplits
      ): RDD[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableWritable(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minSplits)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path, minSplits)
   * }}}
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String, minSplits: Int)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F])
      : RDD[(K, V)] = {
    hadoopFile(path,
        fm.runtimeClass.asInstanceOf[Class[F]],
        km.runtimeClass.asInstanceOf[Class[K]],
        vm.runtimeClass.asInstanceOf[Class[V]],
        minSplits)
  }

  /**
   * Smarter version of hadoopFile() that uses class tags to figure out the classes of keys,
   * values and the InputFormat so that users don't need to pass them directly. Instead, callers
   * can just write, for example,
   * {{{
   * val file = sparkContext.hadoopFile[LongWritable, Text, TextInputFormat](path)
   * }}}
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] =
    hadoopFile[K, V, F](path, defaultMinSplits)

  /** Get an RDD for a Hadoop file with an arbitrary new API InputFormat. */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](path: String)
      (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F]): RDD[(K, V)] = {
    newAPIHadoopFile(
        path,
        fm.runtimeClass.asInstanceOf[Class[F]],
        km.runtimeClass.asInstanceOf[Class[K]],
        vm.runtimeClass.asInstanceOf[Class[V]])
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
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {
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
      conf: Configuration = hadoopConfiguration,
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
   * for the appropriate type. In addition, we pass the converter a ClassTag of its type to
   * allow it to figure out the Writable class to use in the subclass case.
   */
   def sequenceFile[K, V](path: String, minSplits: Int = defaultMinSplits)
      (implicit km: ClassTag[K], vm: ClassTag[V],
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
  def objectFile[T: ClassTag](
      path: String,
      minSplits: Int = defaultMinSplits
      ): RDD[T] = {
    sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minSplits)
      .flatMap(x => Utils.deserialize[Array[T]](x._2.getBytes))
  }


  protected[spark] def checkpointFile[T: ClassTag](
      path: String
    ): RDD[T] = {
    new CheckpointRDD[T](this, path)
  }

  /** Build the union of a list of RDDs. */
  def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = new UnionRDD(this, rdds)

  /** Build the union of a list of RDDs passed as variable-length arguments. */
  def union[T: ClassTag](first: RDD[T], rest: RDD[T]*): RDD[T] =
    new UnionRDD(this, Seq(first) ++ rest)

  // Methods for creating shared variables

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add" values
   * to using the `+=` method. Only the driver can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T)(implicit param: AccumulatorParam[T]) =
    new Accumulator(initialValue, param)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable, to which tasks can add values with `+=`.
   * Only the driver can access the accumuable's `value`.
   * @tparam T accumulator type
   * @tparam R type that can be added to the accumulator
   */
  def accumulable[T, R](initialValue: T)(implicit param: AccumulableParam[T, R]) =
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
   * Broadcast a read-only variable to the cluster, returning a [[org.apache.spark.broadcast.Broadcast]] object for
   * reading it in distributed functions. The variable will be sent to each cluster only once.
   */
  def broadcast[T](value: T) = env.broadcastManager.newBroadcast[T](value, isLocal)

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(path)` to find its download location.
   */
  def addFile(path: String) {
    val uri = new URI(path)
    val key = uri.getScheme match {
      case null | "file" => env.httpFileServer.addFile(new File(uri.getPath))
      case "local"       => "file:" + uri.getPath
      case _             => path
    }
    addedFiles(key) = System.currentTimeMillis

    // Fetch the file locally in case a job is executed locally.
    // Jobs that run through LocalScheduler will already fetch the required dependencies,
    // but jobs run in DAGScheduler.runLocally() will not so we must fetch the files here.
    Utils.fetchFile(path, new File(SparkFiles.getRootDirectory))

    logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
  }

  def addSparkListener(listener: SparkListener) {
    dagScheduler.addSparkListener(listener)
  }

  /**
   * Return a map from the slave to the max memory available for caching and the remaining
   * memory available for caching.
   */
  def getExecutorMemoryStatus: Map[String, (Long, Long)] = {
    env.blockManager.master.getMemoryStatus.map { case(blockManagerId, mem) =>
      (blockManagerId.host + ":" + blockManagerId.port, mem)
    }
  }

  /**
   * Return information about what RDDs are cached, if they are in mem or on disk, how much space
   * they take, etc.
   */
  def getRDDStorageInfo: Array[RDDInfo] = {
    StorageUtils.rddInfoFromStorageStatus(getExecutorStorageStatus, this)
  }

  /**
   * Returns an immutable map of RDDs that have marked themselves as persistent via cache() call.
   * Note that this does not necessarily mean the caching or computation was successful.
   */
  def getPersistentRDDs: Map[Int, RDD[_]] = persistentRdds.toMap

  def getStageInfo: Map[Stage,StageInfo] = {
    dagScheduler.stageToInfos
  }

  /**
   * Return information about blocks stored in all of the slaves
   */
  def getExecutorStorageStatus: Array[StorageStatus] = {
    env.blockManager.master.getStorageStatus
  }

  /**
   *  Return pools for fair scheduler
   *  TODO(xiajunluan): We should take nested pools into account
   */
  def getAllPools: ArrayBuffer[Schedulable] = {
    taskScheduler.rootPool.schedulableQueue
  }

  /**
   * Return the pool associated with the given name, if one exists
   */
  def getPoolForName(pool: String): Option[Schedulable] = {
    taskScheduler.rootPool.schedulableNameToSchedulable.get(pool)
  }

  /**
   *  Return current scheduling mode
   */
  def getSchedulingMode: SchedulingMode.SchedulingMode = {
    taskScheduler.schedulingMode
  }

  /**
   * Clear the job's list of files added by `addFile` so that they do not get downloaded to
   * any new nodes.
   */
  def clearFiles() {
    addedFiles.clear()
  }

  /**
   * Gets the locality information associated with the partition in a particular rdd
   * @param rdd of interest
   * @param partition to be looked up for locality
   * @return list of preferred locations for the partition
   */
  private [spark] def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    dagScheduler.getPreferredLocs(rdd, partition)
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   */
  def addJar(path: String) {
    if (path == null) {
      logWarning("null specified as parameter to addJar")
    } else {
      var key = ""
      if (path.contains("\\")) {
        // For local paths with backslashes on Windows, URI throws an exception
        key = env.httpFileServer.addJar(new File(path))
      } else {
        val uri = new URI(path)
        key = uri.getScheme match {
          // A JAR file which exists only on the driver node
          case null | "file" =>
            if (SparkHadoopUtil.get.isYarnMode()) {
              // In order for this to work on yarn the user must specify the --addjars option to
              // the client to upload the file into the distributed cache to make it show up in the
              // current working directory.
              val fileName = new Path(uri.getPath).getName()
              try {
                env.httpFileServer.addJar(new File(fileName))
              } catch {
                case e: Exception => {
                  logError("Error adding jar (" + e + "), was the --addJars option used?")
                  throw e
                }
              }
            } else {
              env.httpFileServer.addJar(new File(uri.getPath))
            }
          // A JAR file which exists locally on every worker node
          case "local" =>
            "file:" + uri.getPath
          case _ =>
            path
        }
      }
      addedJars(key) = System.currentTimeMillis
      logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
    }
  }

  /**
   * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
   * any new nodes.
   */
  def clearJars() {
    addedJars.clear()
  }

  /** Shut down the SparkContext. */
  def stop() {
    ui.stop()
    // Do this only if not stopped already - best case effort.
    // prevent NPE if stopped more than once.
    val dagSchedulerCopy = dagScheduler
    dagScheduler = null
    if (dagSchedulerCopy != null) {
      metadataCleaner.cancel()
      dagSchedulerCopy.stop()
      taskScheduler = null
      // TODO: Cache.stop()?
      env.stop()
      // Clean up locally linked files
      clearFiles()
      clearJars()
      SparkEnv.set(null)
      ShuffleMapTask.clearCache()
      ResultTask.clearCache()
      logInfo("Successfully stopped SparkContext")
    } else {
      logInfo("SparkContext already stopped")
    }
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
   * Run a function on a given set of partitions in an RDD and pass the results to the given
   * handler function. This is the main entry point for all actions in Spark. The allowLocal
   * flag specifies whether the scheduler can run the computation on the driver rather than
   * shipping it out to the cluster, for short actions like first().
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit) {
    val callSite = Utils.formatSparkCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite)
    val start = System.nanoTime
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
      resultHandler, localProperties.get)
    logInfo("Job finished: " + callSite + ", took " + (System.nanoTime - start) / 1e9 + " s")
    rdd.doCheckpoint()
  }

  /**
   * Run a function on a given set of partitions in an RDD and return the results as an array. The
   * allowLocal flag specifies whether the scheduler can run the computation on the driver rather
   * than shipping it out to the cluster, for short actions like first().
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean
      ): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, allowLocal, (index, res) => results(index) = res)
    results
  }

  /**
   * Run a job on a given set of partitions of an RDD, but take a function of type
   * `Iterator[T] => U` instead of `(TaskContext, Iterator[T]) => U`.
   */
  def runJob[T, U: ClassTag](
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
  def runJob[T, U: ClassTag](rdd: RDD[T], func: (TaskContext, Iterator[T]) => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    runJob(rdd, func, 0 until rdd.partitions.size, false)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
   */
  def runJob[T, U: ClassTag](
    rdd: RDD[T],
    processPartition: (TaskContext, Iterator[T]) => U,
    resultHandler: (Int, U) => Unit)
  {
    runJob[T, U](rdd, processPartition, 0 until rdd.partitions.size, false, resultHandler)
  }

  /**
   * Run a job on all partitions in an RDD and pass the results to a handler function.
   */
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.size, false, resultHandler)
  }

  /**
   * Run a job that can return approximate results.
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      timeout: Long): PartialResult[R] = {
    val callSite = Utils.formatSparkCallSite
    logInfo("Starting job: " + callSite)
    val start = System.nanoTime
    val result = dagScheduler.runApproximateJob(rdd, func, evaluator, callSite, timeout,
      localProperties.get)
    logInfo("Job finished: " + callSite + ", took " + (System.nanoTime - start) / 1e9 + " s")
    result
  }

  /**
   * Submit a job for execution and return a FutureJob holding the result.
   */
  def submitJob[T, U, R](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit,
      resultFunc: => R): SimpleFutureAction[R] =
  {
    val cleanF = clean(processPartition)
    val callSite = Utils.formatSparkCallSite
    val waiter = dagScheduler.submitJob(
      rdd,
      (context: TaskContext, iter: Iterator[T]) => cleanF(iter),
      partitions,
      callSite,
      allowLocal = false,
      resultHandler,
      localProperties.get)
    new SimpleFutureAction(waiter, resultFunc)
  }

  /**
   * Cancel active jobs for the specified group. See [[org.apache.spark.SparkContext.setJobGroup]]
   * for more information.
   */
  def cancelJobGroup(groupId: String) {
    dagScheduler.cancelJobGroup(groupId)
  }

  /** Cancel all jobs that have been scheduled or are running.  */
  def cancelAllJobs() {
    dagScheduler.cancelAllJobs()
  }

  /**
   * Clean a closure to make it ready to serialized and send to tasks
   * (removes unreferenced variables in $outer's, updates REPL variables)
   */
  private[spark] def clean[F <: AnyRef](f: F): F = {
    ClosureCleaner.clean(f)
    return f
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster. If the directory does not exist, it will
   * be created. If the directory exists and useExisting is set to true, then the
   * exisiting directory will be used. Otherwise an exception will be thrown to
   * prevent accidental overriding of checkpoint files in the existing directory.
   */
  def setCheckpointDir(dir: String, useExisting: Boolean = false) {
    val path = new Path(dir)
    val fs = path.getFileSystem(SparkHadoopUtil.get.newConfiguration())
    if (!useExisting) {
      if (fs.exists(path)) {
        throw new Exception("Checkpoint directory '" + path + "' already exists.")
      } else {
        fs.mkdirs(path)
      }
    }
    checkpointDir = Some(dir)
  }

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
  def defaultParallelism: Int = taskScheduler.defaultParallelism

  /** Default min number of partitions for Hadoop RDDs when not given by user */
  def defaultMinSplits: Int = math.min(defaultParallelism, 2)

  private val nextShuffleId = new AtomicInteger(0)

  private[spark] def newShuffleId(): Int = nextShuffleId.getAndIncrement()

  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /** Called by MetadataCleaner to clean up the persistentRdds map periodically */
  private[spark] def cleanup(cleanupTime: Long) {
    persistentRdds.clearOldValues(cleanupTime)
  }
}

/**
 * The SparkContext object contains a number of implicit conversions and parameters for use with
 * various Spark features.
 */
object SparkContext {

  private[spark] val SPARK_JOB_DESCRIPTION = "spark.job.description"

  private[spark] val SPARK_JOB_GROUP_ID = "spark.jobGroup.id"

  private[spark] val SPARK_UNKNOWN_USER = "<unknown>"

  implicit object DoubleAccumulatorParam extends AccumulatorParam[Double] {
    def addInPlace(t1: Double, t2: Double): Double = t1 + t2
    def zero(initialValue: Double) = 0.0
  }

  implicit object IntAccumulatorParam extends AccumulatorParam[Int] {
    def addInPlace(t1: Int, t2: Int): Int = t1 + t2
    def zero(initialValue: Int) = 0
  }

  implicit object LongAccumulatorParam extends AccumulatorParam[Long] {
    def addInPlace(t1: Long, t2: Long) = t1 + t2
    def zero(initialValue: Long) = 0l
  }

  implicit object FloatAccumulatorParam extends AccumulatorParam[Float] {
    def addInPlace(t1: Float, t2: Float) = t1 + t2
    def zero(initialValue: Float) = 0f
  }

  // TODO: Add AccumulatorParams for other types, e.g. lists and strings

  implicit def rddToPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) =
    new PairRDDFunctions(rdd)

  implicit def rddToAsyncRDDActions[T: ClassTag](rdd: RDD[T]) = new AsyncRDDActions(rdd)

  implicit def rddToSequenceFileRDDFunctions[K <% Writable: ClassTag, V <% Writable: ClassTag](
      rdd: RDD[(K, V)]) =
    new SequenceFileRDDFunctions(rdd)

  implicit def rddToOrderedRDDFunctions[K <% Ordered[K]: ClassTag, V: ClassTag](
      rdd: RDD[(K, V)]) =
    new OrderedRDDFunctions[K, V, (K, V)](rdd)

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

  private implicit def arrayToArrayWritable[T <% Writable: ClassTag](arr: Traversable[T]): ArrayWritable = {
    def anyToWritable[U <% Writable](u: U): Writable = u

    new ArrayWritable(classTag[T].runtimeClass.asInstanceOf[Class[Writable]],
        arr.map(x => anyToWritable(x)).toArray)
  }

  // Helper objects for converting common types to Writable
  private def simpleWritableConverter[T, W <: Writable: ClassTag](convert: W => T) = {
    val wClass = classTag[W].runtimeClass.asInstanceOf[Class[W]]
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
    new WritableConverter[T](_.runtimeClass.asInstanceOf[Class[T]], _.asInstanceOf[T])

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

  /** Get the amount of memory per executor requested through system properties or SPARK_MEM */
  private[spark] val executorMemoryRequested = {
    // TODO: Might need to add some extra memory for the non-heap parts of the JVM
    Option(System.getProperty("spark.executor.memory"))
      .orElse(Option(System.getenv("SPARK_MEM")))
      .map(Utils.memoryStringToMb)
      .getOrElse(512)
  }

  // Creates a task scheduler based on a given master URL. Extracted for testing.
  private
  def createTaskScheduler(sc: SparkContext, master: String, appName: String): TaskScheduler = {
    // Regular expression used for local[N] master format
    val LOCAL_N_REGEX = """local\[([0-9]+)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """spark://(.*)""".r
    // Regular expression for connection to Mesos cluster by mesos:// or zk:// url
    val MESOS_REGEX = """(mesos|zk)://.*""".r
    // Regular expression for connection to Simr cluster
    val SIMR_REGEX = """simr://(.*)""".r

    master match {
      case "local" =>
        new LocalScheduler(1, 0, sc)

      case LOCAL_N_REGEX(threads) =>
        new LocalScheduler(threads.toInt, 0, sc)

      case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
        new LocalScheduler(threads.toInt, maxFailures.toInt, sc)

      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new ClusterScheduler(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls, appName)
        scheduler.initialize(backend)
        scheduler

      case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
        // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
        val memoryPerSlaveInt = memoryPerSlave.toInt
        if (SparkContext.executorMemoryRequested > memoryPerSlaveInt) {
          throw new SparkException(
            "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
              memoryPerSlaveInt, SparkContext.executorMemoryRequested))
        }

        val scheduler = new ClusterScheduler(sc)
        val localCluster = new LocalSparkCluster(
          numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt)
        val masterUrls = localCluster.start()
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls, appName)
        scheduler.initialize(backend)
        backend.shutdownCallback = (backend: SparkDeploySchedulerBackend) => {
          localCluster.stop()
        }
        scheduler

      case "yarn-standalone" =>
        val scheduler = try {
          val clazz = Class.forName("org.apache.spark.scheduler.cluster.YarnClusterScheduler")
          val cons = clazz.getConstructor(classOf[SparkContext])
          cons.newInstance(sc).asInstanceOf[ClusterScheduler]
        } catch {
          // TODO: Enumerate the exact reasons why it can fail
          // But irrespective of it, it means we cannot proceed !
          case th: Throwable => {
            throw new SparkException("YARN mode not available ?", th)
          }
        }
        val backend = new CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
        scheduler.initialize(backend)
        scheduler

      case "yarn-client" =>
        val scheduler = try {
          val clazz = Class.forName("org.apache.spark.scheduler.cluster.YarnClientClusterScheduler")
          val cons = clazz.getConstructor(classOf[SparkContext])
          cons.newInstance(sc).asInstanceOf[ClusterScheduler]

        } catch {
          case th: Throwable => {
            throw new SparkException("YARN mode not available ?", th)
          }
        }

        val backend = try {
          val clazz = Class.forName("org.apache.spark.scheduler.cluster.YarnClientSchedulerBackend")
          val cons = clazz.getConstructor(classOf[ClusterScheduler], classOf[SparkContext])
          cons.newInstance(scheduler, sc).asInstanceOf[CoarseGrainedSchedulerBackend]
        } catch {
          case th: Throwable => {
            throw new SparkException("YARN mode not available ?", th)
          }
        }

        scheduler.initialize(backend)
        scheduler

      case mesosUrl @ MESOS_REGEX(_) =>
        MesosNativeLibrary.load()
        val scheduler = new ClusterScheduler(sc)
        val coarseGrained = System.getProperty("spark.mesos.coarse", "false").toBoolean
        val url = mesosUrl.stripPrefix("mesos://") // strip scheme from raw Mesos URLs
        val backend = if (coarseGrained) {
          new CoarseMesosSchedulerBackend(scheduler, sc, url, appName)
        } else {
          new MesosSchedulerBackend(scheduler, sc, url, appName)
        }
        scheduler.initialize(backend)
        scheduler

      case SIMR_REGEX(simrUrl) =>
        val scheduler = new ClusterScheduler(sc)
        val backend = new SimrSchedulerBackend(scheduler, sc, simrUrl)
        scheduler.initialize(backend)
        scheduler

      case _ =>
        throw new SparkException("Could not parse Master URL: '" + master + "'")
    }
  }
}

/**
 * A class encapsulating how to convert some type T to Writable. It stores both the Writable class
 * corresponding to T (e.g. IntWritable for Int) and a function for doing the conversion.
 * The getter for the writable class takes a ClassTag[T] in case this is a generic object
 * that doesn't know the type of T when it is created. This sounds strange but is necessary to
 * support converting subclasses of Writable to themselves (writableWritableConverter).
 */
private[spark] class WritableConverter[T](
    val writableClass: ClassTag[T] => Class[_ <: Writable],
    val convert: Writable => T)
  extends Serializable

