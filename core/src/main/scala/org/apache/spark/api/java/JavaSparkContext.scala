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

package org.apache.spark.api.java

import java.io.Closeable
import java.util
import java.util.{Map => JMap}

import scala.annotation.varargs
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark._
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, NewHadoopRDD}
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util.ArrayImplicits._

/**
 * A Java-friendly version of [[org.apache.spark.SparkContext]] that returns
 * [[org.apache.spark.api.java.JavaRDD]]s and works with Java collections instead of Scala ones.
 *
 * @note Only one `SparkContext` should be active per JVM. You must `stop()` the
 *   active `SparkContext` before creating a new one.
 */
class JavaSparkContext(val sc: SparkContext) extends Closeable {

  /**
   * Create a JavaSparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
   */
  def this() = this(new SparkContext())

  /**
   * @param conf a [[org.apache.spark.SparkConf]] object specifying Spark parameters
   */
  def this(conf: SparkConf) = this(new SparkContext(conf))

  /**
   * @param master Cluster URL to connect to (e.g. spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   */
  def this(master: String, appName: String) = this(new SparkContext(master, appName))

  /**
   * @param master Cluster URL to connect to (e.g. spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) =
    this(conf.setMaster(master).setAppName(appName))

  /**
   * @param master Cluster URL to connect to (e.g. spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the worker nodes
   * @param jarFile JAR file to send to the cluster. This can be a path on the local file system
   *                or an HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(master: String, appName: String, sparkHome: String, jarFile: String) =
    this(new SparkContext(master, appName, sparkHome, Seq(jarFile)))

  /**
   * @param master Cluster URL to connect to (e.g. spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the worker nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toImmutableArraySeq))

  /**
   * @param master Cluster URL to connect to (e.g. spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the worker nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String],
      environment: JMap[String, String]) =
    this(
      new SparkContext(master, appName, sparkHome, jars.toImmutableArraySeq, environment.asScala))

  private[spark] val env = sc.env

  def statusTracker: JavaSparkStatusTracker = new JavaSparkStatusTracker(sc)

  def isLocal: java.lang.Boolean = sc.isLocal

  def sparkUser: String = sc.sparkUser

  def master: String = sc.master

  def appName: String = sc.appName

  def resources: JMap[String, ResourceInformation] = sc.resources.asJava

  def jars: util.List[String] = sc.jars.asJava

  def startTime: java.lang.Long = sc.startTime

  /** The version of Spark on which this application is running. */
  def version: String = sc.version

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
  def defaultParallelism: java.lang.Integer = sc.defaultParallelism

  /** Default min number of partitions for Hadoop RDDs when not given by user */
  def defaultMinPartitions: java.lang.Integer = sc.defaultMinPartitions

  /** Distribute a local Scala collection to form an RDD. */
  def parallelize[T](list: java.util.List[T], numSlices: Int): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.parallelize(list.asScala.toSeq, numSlices)
  }

  /** Get an RDD that has no partitions or elements. */
  def emptyRDD[T]: JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    JavaRDD.fromRDD(new EmptyRDD[T](sc))
  }


  /** Distribute a local Scala collection to form an RDD. */
  def parallelize[T](list: java.util.List[T]): JavaRDD[T] =
    parallelize(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]], numSlices: Int)
  : JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = fakeClassTag
    implicit val ctagV: ClassTag[V] = fakeClassTag
    JavaPairRDD.fromRDD(sc.parallelize(list.asScala.toSeq, numSlices))
  }

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]]): JavaPairRDD[K, V] =
    parallelizePairs(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizeDoubles(list: java.util.List[java.lang.Double], numSlices: Int): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(sc.parallelize(list.asScala.map(_.doubleValue()).toSeq, numSlices))

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizeDoubles(list: java.util.List[java.lang.Double]): JavaDoubleRDD =
    parallelizeDoubles(list, sc.defaultParallelism)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * The text files must be encoded as UTF-8.
   */
  def textFile(path: String): JavaRDD[String] = sc.textFile(path)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * The text files must be encoded as UTF-8.
   */
  def textFile(path: String, minPartitions: Int): JavaRDD[String] =
    sc.textFile(path, minPartitions)



  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
   * The text files must be encoded as UTF-8.
   *
   * <p> For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * {{{
   *   JavaPairRDD<String, String> rdd = sparkContext.wholeTextFiles("hdfs://a-hdfs-path")
   * }}}
   *
   * <p> then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred, large file is also allowable, but may cause bad performance.
   *
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   */
  def wholeTextFiles(path: String, minPartitions: Int): JavaPairRDD[String, String] =
    new JavaPairRDD(sc.wholeTextFiles(path, minPartitions))

  /**
   * Read a directory of text files from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI. Each file is read as a single record and returned in a
   * key-value pair, where the key is the path of each file, the value is the content of each file.
   * The text files must be encoded as UTF-8.
   *
   * @see `wholeTextFiles(path: String, minPartitions: Int)`.
   */
  def wholeTextFiles(path: String): JavaPairRDD[String, String] =
    new JavaPairRDD(sc.wholeTextFiles(path))

  /**
   * Read a directory of binary files from HDFS, a local file system (available on all nodes),
   * or any Hadoop-supported file system URI as a byte array. Each file is read as a single
   * record and returned in a key-value pair, where the key is the path of each file,
   * the value is the content of each file.
   *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * {{{
   *   JavaPairRDD<String, byte[]> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")
   * }}}
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files but may cause bad performance.
   *
   * @param minPartitions A suggestion value of the minimal splitting number for input data.
   */
  def binaryFiles(path: String, minPartitions: Int): JavaPairRDD[String, PortableDataStream] =
    new JavaPairRDD(sc.binaryFiles(path, minPartitions))

  /**
   * Read a directory of binary files from HDFS, a local file system (available on all nodes),
   * or any Hadoop-supported file system URI as a byte array. Each file is read as a single
   * record and returned in a key-value pair, where the key is the path of each file,
   * the value is the content of each file.
   *
   * For example, if you have the following files:
   * {{{
   *   hdfs://a-hdfs-path/part-00000
   *   hdfs://a-hdfs-path/part-00001
   *   ...
   *   hdfs://a-hdfs-path/part-nnnnn
   * }}}
   *
   * Do
   * {{{
   *   JavaPairRDD<String, byte[]> rdd = sparkContext.dataStreamFiles("hdfs://a-hdfs-path")
   * }}},
   *
   * then `rdd` contains
   * {{{
   *   (a-hdfs-path/part-00000, its content)
   *   (a-hdfs-path/part-00001, its content)
   *   ...
   *   (a-hdfs-path/part-nnnnn, its content)
   * }}}
   *
   * @note Small files are preferred; very large files but may cause bad performance.
   */
  def binaryFiles(path: String): JavaPairRDD[String, PortableDataStream] =
    new JavaPairRDD(sc.binaryFiles(path, defaultMinPartitions))

  /**
   * Load data from a flat binary file, assuming the length of each record is constant.
   *
   * @param path Directory to the input data files
   * @return An RDD of data with values, represented as byte arrays
   */
  def binaryRecords(path: String, recordLength: Int): JavaRDD[Array[Byte]] = {
    new JavaRDD(sc.binaryRecords(path, recordLength))
  }

  /**
   * Get an RDD for a Hadoop SequenceFile with given key and value types.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def sequenceFile[K, V](path: String,
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass, minPartitions))
  }

  /**
   * Get an RDD for a Hadoop SequenceFile.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]):
  JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass))
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
   */
  def objectFile[T](path: String, minPartitions: Int): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.objectFile(path, minPartitions)(ctag)
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
   */
  def objectFile[T](path: String): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    sc.objectFile(path)(ctag)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   * etc).
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   * @param minPartitions Minimum number of Hadoop Splits to generate.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass, minPartitions)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   *
   * @param conf JobConf for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop file with an arbitrary InputFormat.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopFile(path, inputFormatClass, keyClass, valueClass, minPartitions)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a Hadoop file with an arbitrary InputFormat
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(keyClass)
    implicit val ctagV: ClassTag[V] = ClassTag(valueClass)
    val rdd = sc.hadoopFile(path, inputFormatClass, keyClass, valueClass)
    new JavaHadoopRDD(rdd.asInstanceOf[HadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)
    val rdd = sc.newAPIHadoopFile(path, fClass, kClass, vClass, conf)
    new JavaNewHadoopRDD(rdd.asInstanceOf[NewHadoopRDD[K, V]])
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   *
   * @param conf Configuration for setting up the dataset. Note: This will be put into a Broadcast.
   *             Therefore if you plan to reuse this conf to create multiple RDDs, you need to make
   *             sure you won't modify the conf. A safe approach is always creating a new conf for
   *             a new RDD.
   * @param fClass Class of the InputFormat
   * @param kClass Class of the keys
   * @param vClass Class of the values
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    conf: Configuration,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V]): JavaPairRDD[K, V] = {
    implicit val ctagK: ClassTag[K] = ClassTag(kClass)
    implicit val ctagV: ClassTag[V] = ClassTag(vClass)
    val rdd = sc.newAPIHadoopRDD(conf, fClass, kClass, vClass)
    new JavaNewHadoopRDD(rdd.asInstanceOf[NewHadoopRDD[K, V]])
  }

  /** Build the union of JavaRDDs. */
  @varargs
  def union[T](rdds: JavaRDD[T]*): JavaRDD[T] = {
    require(rdds.nonEmpty, "Union called on no RDDs")
    implicit val ctag: ClassTag[T] = rdds.head.classTag
    sc.union(rdds.map(_.rdd))
  }

  /** Build the union of JavaPairRDDs. */
  @varargs
  def union[K, V](rdds: JavaPairRDD[K, V]*): JavaPairRDD[K, V] = {
    require(rdds.nonEmpty, "Union called on no RDDs")
    implicit val ctag: ClassTag[(K, V)] = rdds.head.classTag
    implicit val ctagK: ClassTag[K] = rdds.head.kClassTag
    implicit val ctagV: ClassTag[V] = rdds.head.vClassTag
    new JavaPairRDD(sc.union(rdds.map(_.rdd)))
  }

  /** Build the union of JavaDoubleRDDs. */
  @varargs
  def union(rdds: JavaDoubleRDD*): JavaDoubleRDD = {
    require(rdds.nonEmpty, "Union called on no RDDs")
    new JavaDoubleRDD(sc.union(rdds.map(_.srdd)))
  }

  /**
   * Broadcast a read-only variable to the cluster, returning a
   * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
   * The variable will be sent to each cluster only once.
   */
  def broadcast[T](value: T): Broadcast[T] = sc.broadcast(value)(fakeClassTag)

  /** Shut down the SparkContext. */
  def stop(): Unit = {
    sc.stop()
  }

  override def close(): Unit = stop()

  /**
   * Get Spark's home location from either a value set through the constructor,
   * or the spark.home Java property, or the SPARK_HOME environment variable
   * (in that order of preference). If neither of these is set, return None.
   */
  def getSparkHome(): Optional[String] = JavaUtils.optionToOptional(sc.getSparkHome())

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   *
   * @note A path can be added only once. Subsequent additions of the same path are ignored.
   */
  def addFile(path: String): Unit = {
    sc.addFile(path)
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(fileName)` to find its download location.
   *
   * A directory can be given if the recursive option is set to true. Currently directories are only
   * supported for Hadoop-supported filesystems.
   *
   * @note A path can be added only once. Subsequent additions of the same path are ignored.
   */
  def addFile(path: String, recursive: Boolean): Unit = {
    sc.addFile(path, recursive)
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.
   *
   * @note A path can be added only once. Subsequent additions of the same path are ignored.
   */
  def addJar(path: String): Unit = {
    sc.addJar(path)
  }

  /**
   * Returns the Hadoop configuration used for the Hadoop code (e.g. file systems) we reuse.
   *
   * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
   * plan to set some global configurations for all Hadoop RDDs.
   */
  def hadoopConfiguration(): Configuration = {
    sc.hadoopConfiguration
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be an HDFS path if running on a cluster.
   */
  def setCheckpointDir(dir: String): Unit = {
    sc.setCheckpointDir(dir)
  }

  def getCheckpointDir: Optional[String] = JavaUtils.optionToOptional(sc.getCheckpointDir)

  protected def checkpointFile[T](path: String): JavaRDD[T] = {
    implicit val ctag: ClassTag[T] = fakeClassTag
    new JavaRDD(sc.checkpointFile(path))
  }

  /**
   * Return a copy of this JavaSparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
   */
  def getConf: SparkConf = sc.getConf

  /** Return a read-only version of the spark conf. */
  def getReadOnlyConf: ReadOnlySparkConf = sc.getReadOnlyConf

  /**
   * Pass-through to SparkContext.setCallSite.  For API support only.
   */
  def setCallSite(site: String): Unit = {
    sc.setCallSite(site)
  }

  /**
   * Pass-through to SparkContext.setCallSite.  For API support only.
   */
  def clearCallSite(): Unit = {
    sc.clearCallSite()
  }

  /**
   * Set a local property that affects jobs submitted from this thread, and all child
   * threads, such as the Spark fair scheduler pool.
   *
   * These properties are inherited by child threads spawned from this thread. This
   * may have unexpected consequences when working with thread pools. The standard java
   * implementation of thread pools have worker threads spawn other worker threads.
   * As a result, local properties may propagate unpredictably.
   */
  def setLocalProperty(key: String, value: String): Unit = sc.setLocalProperty(key, value)

  /**
   * Get a local property set in this thread, or null if it is missing. See
   * `org.apache.spark.api.java.JavaSparkContext.setLocalProperty`.
   */
  def getLocalProperty(key: String): String = sc.getLocalProperty(key)

  /**
   *  Set a human readable description of the current job.
   *  @since 2.3.0
   */
  def setJobDescription(value: String): Unit = sc.setJobDescription(value)

  /** Control our logLevel. This overrides any user-defined log settings.
   * @param logLevel The desired log level as a string.
   * Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
   */
  def setLogLevel(logLevel: String): Unit = {
    sc.setLogLevel(logLevel)
  }

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   *
   * Often, a unit of execution in an application consists of multiple Spark actions or jobs.
   * Application programmers can use this method to group all those jobs together and give a
   * group description. Once set, the Spark web UI will associate such jobs with this group.
   *
   * The application can also use `org.apache.spark.api.java.JavaSparkContext.cancelJobGroup`
   * to cancel all running jobs in this group. For example,
   * {{{
   * // In the main thread:
   * sc.setJobGroup("some_job_to_cancel", "some job description");
   * rdd.map(...).count();
   *
   * // In a separate thread:
   * sc.cancelJobGroup("some_job_to_cancel");
   * }}}
   *
   * If interruptOnCancel is set to true for the job group, then job cancellation will result
   * in Thread.interrupt() being called on the job's executor threads. This is useful to help ensure
   * that the tasks are actually stopped in a timely manner, but is off by default due to HDFS-1208,
   * where HDFS may respond to Thread.interrupt() by marking nodes as dead.
   */
  def setJobGroup(groupId: String, description: String, interruptOnCancel: Boolean): Unit =
    sc.setJobGroup(groupId, description, interruptOnCancel)

  /**
   * Assigns a group ID to all the jobs started by this thread until the group ID is set to a
   * different value or cleared.
   *
   * @see `setJobGroup(groupId: String, description: String, interruptThread: Boolean)`.
   *      This method sets interruptOnCancel to false.
   */
  def setJobGroup(groupId: String, description: String): Unit = sc.setJobGroup(groupId, description)

  /** Clear the current thread's job group ID and its description. */
  def clearJobGroup(): Unit = sc.clearJobGroup()

  /**
   * Set the behavior of job cancellation from jobs started in this thread.
   *
   * @param interruptOnCancel If true, then job cancellation will result in `Thread.interrupt()`
   * being called on the job's executor threads. This is useful to help ensure that the tasks
   * are actually stopped in a timely manner, but is off by default due to HDFS-1208, where HDFS
   * may respond to Thread.interrupt() by marking nodes as dead.
   *
   * @since 3.5.0
   */
  def setInterruptOnCancel(interruptOnCancel: Boolean): Unit =
    sc.setInterruptOnCancel(interruptOnCancel)

  /**
   * Add a tag to be assigned to all the jobs started by this thread.
   *
   * @param tag The tag to be added. Cannot contain ',' (comma) character.
   *
   * @since 3.5.0
   */
  def addJobTag(tag: String): Unit = sc.addJobTag(tag)

  /**
   * Remove a tag previously added to be assigned to all the jobs started by this thread.
   * Noop if such a tag was not added earlier.
   *
   * @param tag The tag to be removed. Cannot contain ',' (comma) character.
   *
   * @since 3.5.0
   */
  def removeJobTag(tag: String): Unit = sc.removeJobTag(tag)

  /**
   * Get the tags that are currently set to be assigned to all the jobs started by this thread.
   *
   * @since 3.5.0
   */
  def getJobTags(): util.Set[String] = sc.getJobTags().asJava

  /**
   * Clear the current thread's job tags.
   *
   * @since 3.5.0
   */
  def clearJobTags(): Unit = sc.clearJobTags()

  /**
   * Cancel active jobs for the specified group. See
   * `org.apache.spark.api.java.JavaSparkContext.setJobGroup` for more information.
   *
   * @param groupId the group ID to cancel
   * @param reason reason for cancellation
   *
   * @since 4.0.0
   */
  def cancelJobGroup(groupId: String, reason: String): Unit = sc.cancelJobGroup(groupId, reason)

  /**
   * Cancel active jobs for the specified group. See
   * `org.apache.spark.api.java.JavaSparkContext.setJobGroup` for more information.
   *
   * @param groupId the group ID to cancel
   */
  def cancelJobGroup(groupId: String): Unit = sc.cancelJobGroup(groupId)

  /**
   * Cancel active jobs that have the specified tag. See `org.apache.spark.SparkContext.addJobTag`.
   *
   * @param tag The tag to be cancelled. Cannot contain ',' (comma) character.
   * @param reason reason for cancellation
   *
   * @since 4.0.0
   */
  def cancelJobsWithTag(tag: String, reason: String): Unit = sc.cancelJobsWithTag(tag, reason)

  /**
   * Cancel active jobs that have the specified tag. See `org.apache.spark.SparkContext.addJobTag`.
   *
   * @param tag The tag to be cancelled. Cannot contain ',' (comma) character.
   *
   * @since 3.5.0
   */
  def cancelJobsWithTag(tag: String): Unit = sc.cancelJobsWithTag(tag)

  /** Cancel all jobs that have been scheduled or are running. */
  def cancelAllJobs(): Unit = sc.cancelAllJobs()

  /**
   * Returns a Java map of JavaRDDs that have marked themselves as persistent via cache() call.
   *
   * @note This does not necessarily mean the caching or computation was successful.
   */
  def getPersistentRDDs: JMap[java.lang.Integer, JavaRDD[_]] = {
    sc.getPersistentRDDs.toMap.transform((_, s) => JavaRDD.fromRDD(s))
      .asJava.asInstanceOf[JMap[java.lang.Integer, JavaRDD[_]]]
  }

}

object JavaSparkContext {
  implicit def fromSparkContext(sc: SparkContext): JavaSparkContext = new JavaSparkContext(sc)

  implicit def toSparkContext(jsc: JavaSparkContext): SparkContext = jsc.sc

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
   */
  def jarOfClass(cls: Class[_]): Array[String] = SparkContext.jarOfClass(cls).toArray

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
   */
  def jarOfObject(obj: AnyRef): Array[String] = SparkContext.jarOfObject(obj).toArray

  /**
   * Produces a ClassTag[T], which is actually just a casted ClassTag[AnyRef].
   *
   * This method is used to keep ClassTags out of the external Java API, as the Java compiler
   * cannot produce them automatically. While this ClassTag-faking does please the compiler,
   * it can cause problems at runtime if the Scala API relies on ClassTags for correctness.
   *
   * Often, though, a ClassTag[AnyRef] will not lead to incorrect behavior, just worse performance
   * or security issues. For instance, an Array[AnyRef] can hold any type T, but may lose primitive
   * specialization.
   */
  private[spark]
  def fakeClassTag[T]: ClassTag[T] = ClassTag.AnyRef.asInstanceOf[ClassTag[T]]
}
