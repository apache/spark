package spark.api.java

import spark.SparkContext.rddToPairRDDFunctions
import spark.api.java.function.{Function2 => JFunction2}
import spark.api.java.function.{Function => JFunction}
import spark.partial.BoundedDouble
import spark.partial.PartialResult
import spark._

import java.util.{List => JList}
import java.util.Comparator

import scala.Tuple2
import scala.collection.Map
import scala.collection.JavaConversions._

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputFormat
import org.apache.hadoop.mapreduce.{OutputFormat => NewOutputFormat}
import org.apache.hadoop.conf.Configuration

class JavaPairRDD[K, V](val rdd: RDD[(K, V)])(implicit val kManifest: ClassManifest[K],
  implicit val vManifest: ClassManifest[V]) extends JavaRDDLike[(K, V), JavaPairRDD[K, V]] {

  def wrapRDD = JavaPairRDD.fromRDD _

  def classManifest = implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]

  import JavaPairRDD._

  // Common RDD functions

  def cache() = new JavaPairRDD[K, V](rdd.cache())

  // Transformations (return a new RDD)

  def distinct() = new JavaPairRDD[K, V](rdd.distinct())

  def filter(f: Function[(K, V), java.lang.Boolean]) =
    new JavaPairRDD[K, V](rdd.filter(x => f(x).booleanValue()))

  def sample(withReplacement: Boolean, fraction: Double, seed: Int) =
    new JavaPairRDD[K, V](rdd.sample(withReplacement, fraction, seed))

  def union(other: JavaPairRDD[K, V]) = new JavaPairRDD[K, V](rdd.union(other.rdd))

  // first() has to be overridden here so that the generated method has the signature
  // 'public scala.Tuple2 first()'; if the trait's definition is used,
  // then the method has the signature 'public java.lang.Object first()',
  // causing NoSuchMethodErrors at runtime.
  override def first(): (K, V) = rdd.first()

  // Pair RDD functions

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

  def combineByKey[C](createCombiner: JFunction[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C],
    numSplits: Int): JavaPairRDD[K, C] =
    combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(numSplits))

  def reduceByKey(partitioner: Partitioner, func: JFunction2[V, V, V]): JavaPairRDD[K, V] =
    fromRDD(rdd.reduceByKey(partitioner, func))

  def reduceByKeyLocally(func: JFunction2[V, V, V]) = {
    rdd.reduceByKeyLocally(func)
  }

  def countByKey() = rdd.countByKey()

  def countByKeyApprox(timeout: Long): PartialResult[java.util.Map[K, BoundedDouble]] =
    rdd.countByKeyApprox(timeout).map(mapAsJavaMap)

  def countByKeyApprox(timeout: Long, confidence: Double = 0.95)
  : PartialResult[java.util.Map[K, BoundedDouble]] =
    rdd.countByKeyApprox(timeout, confidence).map(mapAsJavaMap)

  def reduceByKey(func: JFunction2[V, V, V], numSplits: Int): JavaPairRDD[K, V] =
    fromRDD(rdd.reduceByKey(func, numSplits))

  def groupByKey(partitioner: Partitioner): JavaPairRDD[K, JList[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey(partitioner)))

  def groupByKey(numSplits: Int): JavaPairRDD[K, JList[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey(numSplits)))

  def partitionBy(partitioner: Partitioner): JavaPairRDD[K, V] =
    fromRDD(rdd.partitionBy(partitioner))

  def join[W](other: JavaPairRDD[K, W], partitioner: Partitioner): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other, partitioner))

  def leftOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (V, Option[W])] =
    fromRDD(rdd.leftOuterJoin(other, partitioner))

  def rightOuterJoin[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (Option[V], W)] =
    fromRDD(rdd.rightOuterJoin(other, partitioner))

  def combineByKey[C](createCombiner: JFunction[V, C],
    mergeValue: JFunction2[C, V, C],
    mergeCombiners: JFunction2[C, C, C]): JavaPairRDD[K, C] = {
    implicit val cm: ClassManifest[C] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[C]]
    fromRDD(combineByKey(createCombiner, mergeValue, mergeCombiners))
  }

  def reduceByKey(func: JFunction2[V, V, V]): JavaPairRDD[K, V] = {
    val partitioner = rdd.defaultPartitioner(rdd)
    fromRDD(reduceByKey(partitioner, func))
  }

  def groupByKey(): JavaPairRDD[K, JList[V]] =
    fromRDD(groupByResultToJava(rdd.groupByKey()))

  def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other))

  def join[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (V, W)] =
    fromRDD(rdd.join(other, numSplits))

  def leftOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, Option[W])] =
    fromRDD(rdd.leftOuterJoin(other))

  def leftOuterJoin[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (V, Option[W])] =
    fromRDD(rdd.leftOuterJoin(other, numSplits))

  def rightOuterJoin[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (Option[V], W)] =
    fromRDD(rdd.rightOuterJoin(other))

  def rightOuterJoin[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (Option[V], W)] =
    fromRDD(rdd.rightOuterJoin(other, numSplits))

  def collectAsMap(): Map[K, V] = rdd.collectAsMap()

  def mapValues[U](f: Function[V, U]): JavaPairRDD[K, U] = {
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    fromRDD(rdd.mapValues(f))
  }

  def flatMapValues[U](f: JFunction[V, java.lang.Iterable[U]]): JavaPairRDD[K, U] = {
    import scala.collection.JavaConverters._
    def fn = (x: V) => f.apply(x).asScala
    implicit val cm: ClassManifest[U] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[U]]
    fromRDD(rdd.flatMapValues(fn))
  }

  def cogroup[W](other: JavaPairRDD[K, W], partitioner: Partitioner)
  : JavaPairRDD[K, (JList[V], JList[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other, partitioner)))

  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2], partitioner: Partitioner)
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2, partitioner)))

  def cogroup[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JList[V], JList[W])] =
    fromRDD(cogroupResultToJava(rdd.cogroup(other)))

  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2])
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2)))

  def cogroup[W](other: JavaPairRDD[K, W], numSplits: Int): JavaPairRDD[K, (JList[V], JList[W])]
  = fromRDD(cogroupResultToJava(rdd.cogroup(other, numSplits)))

  def cogroup[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2], numSplits: Int)
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.cogroup(other1, other2, numSplits)))

  def groupWith[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JList[V], JList[W])] =
    fromRDD(cogroupResultToJava(rdd.groupWith(other)))

  def groupWith[W1, W2](other1: JavaPairRDD[K, W1], other2: JavaPairRDD[K, W2])
  : JavaPairRDD[K, (JList[V], JList[W1], JList[W2])] =
    fromRDD(cogroupResult2ToJava(rdd.groupWith(other1, other2)))

  def lookup(key: K): JList[V] = seqAsJavaList(rdd.lookup(key))

  def saveAsHadoopFile[F <: OutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F],
    conf: JobConf) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  def saveAsHadoopFile[F <: OutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F]) {
    rdd.saveAsHadoopFile(path, keyClass, valueClass, outputFormatClass)
  }

  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F],
    conf: Configuration) {
    rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass, conf)
  }

  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
    path: String,
    keyClass: Class[_],
    valueClass: Class[_],
    outputFormatClass: Class[F]) {
    rdd.saveAsNewAPIHadoopFile(path, keyClass, valueClass, outputFormatClass)
  }

  def saveAsHadoopDataset(conf: JobConf) {
    rdd.saveAsHadoopDataset(conf)
  }


  // Ordered RDD Functions
  def sortByKey(): JavaPairRDD[K, V] = sortByKey(true)

  def sortByKey(ascending: Boolean): JavaPairRDD[K, V] = {
    val comp = com.google.common.collect.Ordering.natural().asInstanceOf[Comparator[K]]
    sortByKey(comp, true)
  }

  def sortByKey(comp: Comparator[K]): JavaPairRDD[K, V] = sortByKey(comp, true)

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
    new PartitioningPreservingMappedRDD(rdd, (x: (K, Seq[T])) => (x._1, seqAsJavaList(x._2)))

  def cogroupResultToJava[W, K, V](rdd: RDD[(K, (Seq[V], Seq[W]))])
  : RDD[(K, (JList[V], JList[W]))] = new PartitioningPreservingMappedRDD(rdd,
    (x: (K, (Seq[V], Seq[W]))) => (x._1, (seqAsJavaList(x._2._1), seqAsJavaList(x._2._2))))

  def cogroupResult2ToJava[W1, W2, K, V](rdd: RDD[(K, (Seq[V], Seq[W1], Seq[W2]))])
  : RDD[(K, (JList[V], JList[W1], JList[W2]))] = new PartitioningPreservingMappedRDD(rdd,
    (x: (K, (Seq[V], Seq[W1], Seq[W2]))) => (x._1, (seqAsJavaList(x._2._1),
      seqAsJavaList(x._2._2),
      seqAsJavaList(x._2._3))))

  def fromRDD[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)]): JavaPairRDD[K, V] =
    new JavaPairRDD[K, V](rdd)

  implicit def toRDD[K, V](rdd: JavaPairRDD[K, V]): RDD[(K, V)] = rdd.rdd
}