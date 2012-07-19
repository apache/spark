package spark.api.java

import spark.{AccumulatorParam, RDD, SparkContext}
import spark.SparkContext.IntAccumulatorParam
import spark.SparkContext.DoubleAccumulatorParam

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}


import scala.collection.JavaConversions

class JavaSparkContext(val sc: SparkContext) extends JavaSparkContextVarargsWorkaround {

  def this(master: String, frameworkName: String) = this(new SparkContext(master, frameworkName))

  val env = sc.env

  def parallelize[T](list: java.util.List[T], numSlices: Int): JavaRDD[T] = {
    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    sc.parallelize(JavaConversions.asScalaBuffer(list), numSlices)
  }

  def parallelize[T](list: java.util.List[T]): JavaRDD[T] =
    parallelize(list, sc.defaultParallelism)


  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]], numSlices: Int)
  : JavaPairRDD[K, V] = {
    implicit val kcm: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val vcm: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    JavaPairRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(list), numSlices))
  }

  def parallelizePairs[K, V](list: java.util.List[Tuple2[K, V]]): JavaPairRDD[K, V] =
    parallelizePairs(list, sc.defaultParallelism)

  def parallelizeDoubles(list: java.util.List[java.lang.Double], numSlices: Int): JavaDoubleRDD =
    JavaDoubleRDD.fromRDD(sc.parallelize(JavaConversions.asScalaBuffer(list).map(_.doubleValue()),
      numSlices))

  def parallelizeDoubles(list: java.util.List[java.lang.Double]): JavaDoubleRDD =
    parallelizeDoubles(list, sc.defaultParallelism)

  def textFile(path: String): JavaRDD[String] = sc.textFile(path)

  def textFile(path: String, minSplits: Int): JavaRDD[String] = sc.textFile(path, minSplits)

  /**Get an RDD for a Hadoop SequenceFile with given key and value types */
  def sequenceFile[K, V](path: String,
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int
    ): JavaPairRDD[K, V] = {
    implicit val kcm = ClassManifest.fromClass(keyClass)
    implicit val vcm = ClassManifest.fromClass(valueClass)
    new JavaPairRDD(sc.sequenceFile(path, keyClass, valueClass, minSplits))
  }

  def sequenceFile[K, V](path: String, keyClass: Class[K], valueClass: Class[V]):
  JavaPairRDD[K, V] = {
    implicit val kcm = ClassManifest.fromClass(keyClass)
    implicit val vcm = ClassManifest.fromClass(valueClass)
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
    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    sc.objectFile(path, minSplits)(cm)
  }

  def objectFile[T](path: String): JavaRDD[T] = {
    implicit val cm: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
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
    implicit val kcm = ClassManifest.fromClass(keyClass)
    implicit val vcm = ClassManifest.fromClass(valueClass)
    new JavaPairRDD(sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass, minSplits))
  }

  def hadoopRDD[K, V, F <: InputFormat[K, V]](
    conf: JobConf,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val kcm = ClassManifest.fromClass(keyClass)
    implicit val vcm = ClassManifest.fromClass(valueClass)
    new JavaPairRDD(sc.hadoopRDD(conf, inputFormatClass, keyClass, valueClass))
  }

  /**Get an RDD for a Hadoop file with an arbitrary InputFormat */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int
    ): JavaPairRDD[K, V] = {
    implicit val kcm = ClassManifest.fromClass(keyClass)
    implicit val vcm = ClassManifest.fromClass(valueClass)
    new JavaPairRDD(sc.hadoopFile(path, inputFormatClass, keyClass, valueClass, minSplits))
  }

  def hadoopFile[K, V, F <: InputFormat[K, V]](
    path: String,
    inputFormatClass: Class[F],
    keyClass: Class[K],
    valueClass: Class[V]
    ): JavaPairRDD[K, V] = {
    implicit val kcm = ClassManifest.fromClass(keyClass)
    implicit val vcm = ClassManifest.fromClass(valueClass)
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
    implicit val kcm = ClassManifest.fromClass(kClass)
    implicit val vcm = ClassManifest.fromClass(vClass)
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
    implicit val kcm = ClassManifest.fromClass(kClass)
    implicit val vcm = ClassManifest.fromClass(vClass)
    new JavaPairRDD(sc.newAPIHadoopRDD(conf, fClass, kClass, vClass))
  }

  @Override
  def union[T](jrdds: java.util.List[JavaRDD[T]]): JavaRDD[T] = {
    val rdds: Seq[RDD[T]] = asScalaBuffer(jrdds).map(_.rdd)
    implicit val cm: ClassManifest[T] = jrdds.head.classManifest
    sc.union(rdds: _*)(cm)
  }

  @Override
  def union[K, V](jrdds: java.util.List[JavaPairRDD[K, V]]): JavaPairRDD[K, V] = {
    val rdds: Seq[RDD[(K, V)]] = asScalaBuffer(jrdds).map(_.rdd)
    implicit val cm: ClassManifest[(K, V)] = jrdds.head.classManifest
    implicit val kcm: ClassManifest[K] = jrdds.head.kManifest
    implicit val vcm: ClassManifest[V] = jrdds.head.vManifest
    new JavaPairRDD(sc.union(rdds: _*)(cm))(kcm, vcm)
  }

  @Override
  def union(jrdds: java.util.List[JavaDoubleRDD]): JavaDoubleRDD = {
    val rdds: Seq[RDD[Double]] = asScalaBuffer(jrdds).map(_.srdd)
    new JavaDoubleRDD(sc.union(rdds: _*))
  }

  def intAccumulator(initialValue: Int) = sc.accumulator(initialValue)(IntAccumulatorParam)

  def doubleAccumulator(initialValue: Double) =
    sc.accumulator(initialValue)(DoubleAccumulatorParam)

  def accumulator[T](initialValue: T, accumulatorParam: AccumulatorParam[T]) =
    sc.accumulator(initialValue)(accumulatorParam)

  def broadcast[T](value: T) = sc.broadcast(value)

  def stop() {
    sc.stop()
  }

  def getSparkHome() = sc.getSparkHome()
}

object JavaSparkContext {
  implicit def fromSparkContext(sc: SparkContext): JavaSparkContext = new JavaSparkContext(sc)

  implicit def toSparkContext(jsc: JavaSparkContext): SparkContext = jsc.sc
}