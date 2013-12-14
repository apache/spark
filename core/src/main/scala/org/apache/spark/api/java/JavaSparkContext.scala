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

import java.util.{Map => JMap}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import com.google.common.base.Optional

import org.apache.spark.{Accumulable, AccumulableParam, Accumulator, AccumulatorParam, SparkContext}
import org.apache.spark.SparkContext.IntAccumulatorParam
import org.apache.spark.SparkContext.DoubleAccumulatorParam
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * A Java-friendly version of [[org.apache.spark.SparkContext]] that returns [[org.apache.spark.api.java.JavaRDD]]s and
 * works with Java collections instead of Scala ones.
 */
class JavaSparkContext(val sc: SparkContext) extends JavaSparkContextVarargsWorkaround {

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   */
  def this(master: String, appName: String) = this(new SparkContext(master, appName))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jarFile JAR file to send to the cluster. This can be a path on the local file system
   *                or an HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(master: String, appName: String, sparkHome: String, jarFile: String) =
    this(new SparkContext(master, appName, sparkHome, Seq(jarFile)))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(master: String, appName: String, sparkHome: String, jars: Array[String],
      environment: JMap[String, String]) =
    this(new SparkContext(master, appName, sparkHome, jars.toSeq, environment))

  private[spark] val env = sc.env

  /** Distribute a local Scala collection to form an RDD. */
  def parallelize[T](list: java.util.List[T], numSlices: Int): JavaRDD[T] = {
    implicit val cm: ClassTag[T] = implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    sc.parallelize(JavaConversions.asScalaBuffer(list), numSlices)
  }

  /** Distribute a local Scala collection to form an RDD. */
  def parallelize[T](list: java.util.List[T]): JavaRDD[T] =
    parallelize(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]], numSlices: Int)
  : JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[K]]
    implicit val vcm: ClassTag[V] = implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    JavaPairRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(list), numSlices))
  }

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]]): JavaPairRDD[K, V] =
    parallelizePairs(list, sc.defaultParallelism)

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizeDoubles(list: java.util.List[java.lang.Double], numSlices: Int): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(list).map(_.doubleValue()),
      numSlices))

  /** Distribute a local Scala collection to form an RDD. */
  def parallelizeDoubles(list: java.util.List[java.lang.Double]): JavaDoubleRDD =
    parallelizeDoubles(list, sc.defaultParallelism)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(path: String): JavaRDD[String] = sc.textFile(path)

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(path: String, minSplits: Int): JavaRDD[String] = sc.textFile(path, minSplits)

  /**Get an RDD for a Hadoop SequenceFile with given key and value types. */
  def sequenceFile[K, V](path: String,
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int
    ): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(keyClass)
    implicit val vcm: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass, minSplits))
  }

  /**Get an RDD for a Hadoop SequenceFile. */
  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]):
  JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(keyClass)
    implicit val vcm: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass))
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
   */
  def objectFile[T](path: String, minSplits: Int): JavaRDD[T] = {
    implicit val cm: ClassTag[T] = implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    sc.objectFile(path, minSplits)(cm)
  }

  /**
   * Load an RDD saved as a SequenceFile containing serialized objects, with NullWritable keys and
   * BytesWritable values that contain a serialized partition. This is still an experimental storage
   * format and may not be supported exactly as is in future Spark releases. It will also be pretty
   * slow if you use the default serializer (Java serialization), though the nice thing about it is
   * that there's very little effort required to save arbitrary objects.
   */
  def objectFile[T](path: String): JavaRDD[T] = {
    implicit val cm: ClassTag[T] = implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    sc.objectFile(path)(cm)
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadooop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   * etc).
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int
    ): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(keyClass)
    implicit val vcm: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass, minSplits))
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadooop JobConf giving its InputFormat and any
   * other necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable,
   * etc).
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(keyClass)
    implicit val vcm: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass))
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int
    ): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(keyClass)
    implicit val vcm: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.hadoopFile(path, inputFormatClass, keyClass, valueClass, minSplits))
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(keyClass)
    implicit val vcm: ClassTag[V] = ClassTag(valueClass)
    new JavaPairRDD(sc.hadoopFile(path,
      inputFormatClass, keyClass, valueClass))
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
    conf: Configuration): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(kClass)
    implicit val vcm: ClassTag[V] = ClassTag(vClass)
    new JavaPairRDD(sc.newAPIHadoopFile(path, fClass, kClass, vClass, conf))
  }

  /**
   * Get an RDD for a given Hadoop file with an arbitrary new API InputFormat
   * and extra configuration options to pass to the input format.
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
    conf: Configuration,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V]): JavaPairRDD[K, V] = {
    implicit val kcm: ClassTag[K] = ClassTag(kClass)
    implicit val vcm: ClassTag[V] = ClassTag(vClass)
    new JavaPairRDD(sc.newAPIHadoopRDD(conf, fClass, kClass, vClass))
  }

  /** Build the union of two or more RDDs. */
  override def union[T](first: JavaRDD[T], rest: java.util.List[JavaRDD[T]]): JavaRDD[T] = {
    val rdds: Seq[RDD[T]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.rdd)
    implicit val cm: ClassTag[T] = first.classTag
    sc.union(rdds)(cm)
  }

  /** Build the union of two or more RDDs. */
  override def union[K, V](first: JavaPairRDD[K, V], rest: java.util.List[JavaPairRDD[K, V]])
      : JavaPairRDD[K, V] = {
    val rdds: Seq[RDD[(K, V)]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.rdd)
    implicit val cm: ClassTag[(K, V)] = first.classTag
    implicit val kcm: ClassTag[K] = first.kClassTag
    implicit val vcm: ClassTag[V] = first.vClassTag
    new JavaPairRDD(sc.union(rdds)(cm))(kcm, vcm)
  }

  /** Build the union of two or more RDDs. */
  override def union(first: JavaDoubleRDD, rest: java.util.List[JavaDoubleRDD]): JavaDoubleRDD = {
    val rdds: Seq[RDD[Double]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.srdd)
    new JavaDoubleRDD(sc.union(rdds))
  }

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def intAccumulator(initialValue: Int): Accumulator[java.lang.Integer] =
    sc.accumulator(initialValue)(IntAccumulatorParam).asInstanceOf[Accumulator[java.lang.Integer]]

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def doubleAccumulator(initialValue: Double): Accumulator[java.lang.Double] =
    sc.accumulator(initialValue)(DoubleAccumulatorParam).asInstanceOf[Accumulator[java.lang.Double]]

  /**
   * Create an [[org.apache.spark.Accumulator]] integer variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def accumulator(initialValue: Int): Accumulator[java.lang.Integer] = intAccumulator(initialValue)

  /**
   * Create an [[org.apache.spark.Accumulator]] double variable, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def accumulator(initialValue: Double): Accumulator[java.lang.Double] =
    doubleAccumulator(initialValue)

  /**
   * Create an [[org.apache.spark.Accumulator]] variable of a given type, which tasks can "add" values
   * to using the `add` method. Only the master can access the accumulator's `value`.
   */
  def accumulator[T](initialValue: T, accumulatorParam: AccumulatorParam[T]): Accumulator[T] =
    sc.accumulator(initialValue)(accumulatorParam)

  /**
   * Create an [[org.apache.spark.Accumulable]] shared variable of the given type, to which tasks can
   * "add" values with `add`. Only the master can access the accumuable's `value`.
   */
  def accumulable[T, R](initialValue: T, param: AccumulableParam[T, R]): Accumulable[T, R] =
    sc.accumulable(initialValue)(param)

  /**
   * Broadcast a read-only variable to the cluster, returning a [[org.apache.spark.Broadcast]] object for
   * reading it in distributed functions. The variable will be sent to each cluster only once.
   */
  def broadcast[T](value: T): Broadcast[T] = sc.broadcast(value)

  /** Shut down the SparkContext. */
  def stop() {
    sc.stop()
  }

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
   * use `SparkFiles.get(path)` to find its download location.
   */
  def addFile(path: String) {
    sc.addFile(path)
  }

  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.
   */
  def addJar(path: String) {
    sc.addJar(path)
  }

  /**
   * Clear the job's list of JARs added by `addJar` so that they do not get downloaded to
   * any new nodes.
   */
  def clearJars() {
    sc.clearJars()
  }

  /**
   * Clear the job's list of files added by `addFile` so that they do not get downloaded to
   * any new nodes.
   */
  def clearFiles() {
    sc.clearFiles()
  }

  /**
   * Returns the Hadoop configuration used for the Hadoop code (e.g. file systems) we reuse.
   */
  def hadoopConfiguration(): Configuration = {
    sc.hadoopConfiguration
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster. If the directory does not exist, it will
   * be created. If the directory exists and useExisting is set to true, then the
   * exisiting directory will be used. Otherwise an exception will be thrown to
   * prevent accidental overriding of checkpoint files in the existing directory.
   */
  def setCheckpointDir(dir: String, useExisting: Boolean) {
    sc.setCheckpointDir(dir, useExisting)
  }

  /**
   * Set the directory under which RDDs are going to be checkpointed. The directory must
   * be a HDFS path if running on a cluster. If the directory does not exist, it will
   * be created. If the directory exists, an exception will be thrown to prevent accidental
   * overriding of checkpoint files.
   */
  def setCheckpointDir(dir: String) {
    sc.setCheckpointDir(dir)
  }

  protected def checkpointFile[T](path: String): JavaRDD[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    new JavaRDD(sc.checkpointFile(path))
  }
}

object JavaSparkContext {
  implicit def fromSparkContext(sc: SparkContext): JavaSparkContext = new JavaSparkContext(sc)

  implicit def toSparkContext(jsc: JavaSparkContext): SparkContext = jsc.sc
}
