package spark.api.java

import java.util.{List => JList}
import java.util.Comparator

import scala.Tuple2
import scala.collection.JavaConversions._

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.conf.Configuration

import spark.api.java.function.{Function2 => JFunction2}
import spark.api.java.function.{Function => JFunction}
import spark.partial.BoundedDouble
import spark.partial.PartialResult
import spark.OrderedRDDFunctions
import spark.storage.StorageLevel
import spark.HashPartitioner
import spark.Partitioner
import spark.RDD
import spark.SparkContext.rddToPairRDDFunctions

class JavaPairRDD[K, V](val rdd: RDD[(K, V)])(implicit val kManifest: ClassManifest[K],
  implicit val vManifest: ClassManifest[V]) extends JavaRDDLike[(K, V), JavaPairRDD[K, V]] {

  override def wrapRDD(rdd: RDD[(K, V)]): JavaPairRDD[K, V] = JavaPairRDD.fromRDD(rdd)

  override val classManifest: ClassManifest[(K, V)] =
    implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]

  import JavaPairRDD._

  // Common RDD functions

  /** Persist this RDD with the default storage level (`MEMORY_ONLY`). */
  def cache(): JavaPairRDD[K, V] = new JavaPairRDD[K, V](rdd.cache())

  /** 
   * Set this RDD's storage level to persist its values across operations after the first time
   * it is computed. Can only be called once on each RDD.
   */
  def persist(newLevel: StorageLevel): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.persist(newLevel))

  // Transformations (return a new RDD)

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): JavaPairRDD[K, V] = new JavaPairRDD[K, V](rdd.distinct())

  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numSplits: Int): JavaPairRDD[K, V] = new JavaPairRDD[K, V](rdd.distinct(numSplits))

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
  def filter(f: Function[(K, V), java.lang.Boolean]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.filter(x => f(x).booleanValue()))

  /**
   * Return a sampled subset of this RDD.
   */
  def sample(withReplacement: Boolean, fraction: Double, seed: Int): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.sample(withReplacement, fraction, seed))

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: JavaPairRDD[K, V]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd.union(other.rdd))

  // first() has to be overridden here so that the generated method has the signature
  // 'public scala.Tuple2 first()'; if the trait's definition is used,
  // then the method has the signature 'public java.lang.Object first()',
  // causing NoSuchMethodErrors at runtime.
  override def first(): (K, V) = rdd.first()

  // Pair RDD functions
 
  /**
   * Generic function to combine the elements for each key using a custom set of aggregation 
   * functions. Turns a JavaPairRDD[(K, V)] into a result of type JavaPairRDD[(K, C)], for a 
   * "combined type" C * Note that V and C can be different -- for example, one might group an 
   * RDD of type (Int, Int) into an RDD of type (Int, List[Int]). Users provide three 
   * functions:
   * 
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  def combineByKey[C](createCombiner: Function[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C],
    partitioner: Partitioner): JavaPairRDD[K, C] = {
    implicit val cm: ClassManifest[C] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[C]]
    fromRDD(rdd.combineByKey(
      createCombiner,
      mergeValue,
      mergeCombiners,
      partitioner
    ))
  }

  /**
   * Simplified version of combineByKey that hash-partitions the output RDD.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C],
    numSplits: Int): JavaPairRDD[K, C] =
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numSplits))

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def reduceByKey(partitioner: Partitioner, func: JFunction2[V, V, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.reduceByKey(partitioner, func))

  /**
   * Merge the values for each key using an associative reduce function, but return the results
   * immediately to the master as a Map. This will also perform the merging locally on each mapper
   * before sending results to a reducer, similarly to a "combiner" in MapReduce.
   */
  def reduceByKeyLocally(func: JFunction2[V, V, V]): java.util.Map[K, V] =
    mapAsJavaMap(rdd.reduceByKeyLocally(func))

  /** Count the number of elements for each key, and return the result to the master as a Map. */
  def countByKey(): java.util.Map[K, Long] = mapAsJavaMap(rdd.countByKey())

  /** 
   * (Experimental) Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  def countByKeyApprox(timeout: Long): PartialResult[java.util.Map[K, BoundedDouble]] =
    rdd.countByKeyApprox(timeout).map(mapAsJavaMap)

  /** 
   * (Experimental) Approximate version of countByKey that can return a partial result if it does
   * not finish within a timeout.
   */
  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
  : PartialResult[java.util.Map[K, BoundedDouble]] =
    rdd.countByKeyApprox(timeout, confidence).map(mapAsJavaMap)

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numSplits splits.
   */
  def reduceByKey(func: JFunction2[V, V, V], numSplits: Int): JavaPairRDD[K, V] =
    fromRDD(rdd.reduceByKey(func, numSplits))

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   */
  def groupByKey(partitioner: Partitioner): JavaPairRDD[K, JList[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey(partitioner)))

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numSplits` partitions.
   */
  def groupByKey(numSplits: Int): JavaPairRDD[K, JList[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey(numSplits)))

  /**
   * Return a copy of the RDD partitioned using the specified partitioner. If `mapSideCombine`
   * is true, Spark will group values of the same key together on the map side before the
   * repartitioning, to only send each key over the network once. If a large number of
   * duplicated keys are expected, and the size of the keys are large, `mapSideCombine` should
   * be set to true.
   */
  def partitionBy(partitioner: Partitioner): JavaPairRDD[K, V] =
    fromRDD(rdd.partitionBy(partitioner))

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  def join[W](other: JavaPairRDD[K, W], partitioner: Partitioner): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other, partitioner))

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def leftOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (V, Option[W])] =
    fromRDD(rdd.leftOuterJoin(other, partitioner))

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Uses the given Partitioner to
   * partition the output RDD.
   */
  def rightOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (Option[V], W)] =
    fromRDD(rdd.rightOuterJoin(other, partitioner))

  /** 
   * Simplified version of combineByKey that hash-partitions the resulting RDD using the default
   * parallelism level.
   */
  def combineByKey[C](createCombiner: JFunction[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C]): JavaPairRDD[K, C] = {
    implicit val cm: ClassManifest[C] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[C]]
    fromRDD(combineByKey(createCombiner, mergeValue, mergeCombiners))
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the default parallelism level.
   */
  def reduceByKey(func: JFunction2[V, V, V]): JavaPairRDD[K, V] = {
    val partitioner = rdd.defaultPartitioner(rdd)
    fromRDD(reduceByKey(partitioner, func))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the default parallelism level.
   */
  def groupByKey(): JavaPairRDD[K, JList[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey()))

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other))

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  def join[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other, numSplits))

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * using the default level of parallelism.
   */
  def leftOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, Option[W])] =
    fromRDD(rdd.leftOuterJoin(other))

  /**
   * Perform a left outer join of `this` and `other`. For each element (k, v) in `this`, the
   * resulting RDD will either contain all pairs (k, (v, Some(w))) for w in `other`, or the
   * pair (k, (v, None)) if no elements in `other` have key k. Hash-partitions the output
   * into `numSplits` partitions.
   */
  def leftOuterJoin[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (V, Option[W])] =
    fromRDD(rdd.leftOuterJoin(other, numSplits))

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD using the default parallelism level.
   */
  def rightOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (Option[V], W)] =
    fromRDD(rdd.rightOuterJoin(other))

  /**
   * Perform a right outer join of `this` and `other`. For each element (k, w) in `other`, the
   * resulting RDD will either contain all pairs (k, (Some(v), w)) for v in `this`, or the
   * pair (k, (None, w)) if no elements in `this` have key k. Hash-partitions the resulting
   * RDD into the given number of partitions.
   */
  def rightOuterJoin[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (Option[V], W)] =
    fromRDD(rdd.rightOuterJoin(other, numSplits))

  /**
   * Return the key-value pairs in this RDD to the master as a Map.
   */
  def collectAsMap(): java.util.Map[K, V] = mapAsJavaMap(rdd.collectAsMap())

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  def mapValues[U](f: Function[V, U]): JavaPairRDD[K, U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    fromRDD(rdd.mapValues(f))
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  def flatMapValues[U](f: JFunction[V, java.lang.Iterable[U]]): JavaPairRDD[K, U] = {
    import scala.collection.JavaConverters._
    def fn = (x: V) => f.apply(x).asScala
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    fromRDD(rdd.flatMapValues(fn))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (JList[V], JList[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other, partitioner)))

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2], partitioner: Partitioner)
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2, partitioner)))

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JList[V], JList[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other)))

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2])
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2)))

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  def cogroup[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (JList[V], JList[W])]
  = fromRDD(cogroupResultToJava(rdd.cogroup(other, numSplits)))

  /**
   * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
   * tuple with the list of values for that key in `this`, `other1` and `other2`.
   */
  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2], numSplits: Int)
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2, numSplits)))

  /** Alias for cogroup. */
  def groupWith[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JList[V], JList[W])] =
    fromRDD(cogroupResultToJava(rdd.groupWith(other)))

  /** Alias for cogroup. */
  def groupWith[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2])
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.groupWith(other1, other2)))

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   */
  def lookup(key: K): JList[V] = seqAsJavaList(rdd.lookup(key))

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsHadoopFile[F <: OutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F],
    conf: JobConf) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsHadoopFile[F <: OutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F]) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass)
  }

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F],
    conf: Configuration) {
    rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  /** Output the RDD to any Hadoop-supported file system. */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F]) {
    rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   */
  def saveAsHadoopDataset(conf: JobConf) {
    rdd.saveAsHadoopDataset(conf)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements in
   * ascending order. Calling `collect` or `save` on the resulting RDD will return or output an
   * ordered list of records (in the `save` case, they will be written to multiple `part-X` files
   * in the filesystem, in order of the keys).
   */
  def sortByKey(): JavaPairRDD[K, V] = sortByKey(true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(ascending: Boolean): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKey(comp, true)
  }

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(comp: Comparator[K]): JavaPairRDD[K, V] = sortByKey(comp, true)

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  def sortByKey(comp: Comparator[K], ascending: Boolean): JavaPairRDD[K, V] = {
    class KeyOrdering(val a: K) extends Ordered[K] {
      override def compare(b: K) = comp.compare(a, b)
    }
    implicit def toOrdered(x: K): Ordered[K] = new KeyOrdering(x)
    fromRDD(new OrderedRDDFunctions(rdd).sortByKey(ascending))
  }
}

object JavaPairRDD {
  def groupByResultToJava[K, T](rdd: RDD[(K, Seq[T])])(implicit kcm: ClassManifest[K],
    vcm: ClassManifest[T]): RDD[(K, JList[T])] =
    rddToPairRDDFunctions(rdd).mapValues(seqAsJavaList _)

  def cogroupResultToJava[W, K, V](rdd: RDD[(K, (Seq[V], Seq[W]))])(implicit kcm: ClassManifest[K],
    vcm: ClassManifest[V]): RDD[(K, (JList[V], JList[W]))] = rddToPairRDDFunctions(rdd).mapValues((x: (Seq[V],
    Seq[W])) => (seqAsJavaList(x._1), seqAsJavaList(x._2)))

  def cogroupResult2ToJava[W1, W2, K, V](rdd: RDD[(K, (Seq[V], Seq[W1],
    Seq[W2]))])(implicit kcm: ClassManifest[K]) : RDD[(K, (JList[V], JList[W1],
    JList[W2]))] = rddToPairRDDFunctions(rdd).mapValues(
    (x: (Seq[V], Seq[W1], Seq[W2])) => (seqAsJavaList(x._1),
      seqAsJavaList(x._2),
      seqAsJavaList(x._3)))

  def fromRDD[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd)

  implicit def toRDD[K, V](rdd: JavaPairRDD[K, V]): RDD[(K, V)] = rdd.rdd
}
