package spark.api.java

import spark.{Split, RDD}
import spark.api.java.JavaPairRDD._
import spark.api.java.function.{Function2 => JFunction2, Function => JFunction, _}
import spark.partial.{PartialResult, BoundedDouble}

import java.util.{List => JList}

import scala.collection.JavaConversions._
import java.lang
import scala.Tuple2

trait JavaRDDLike[T, This <: JavaRDDLike[T, This]] extends Serializable {
  def wrapRDD: (RDD[T] => This)

  implicit def classManifest: ClassManifest[T]

  def rdd: RDD[T]

  def context = rdd.context

  def id = rdd.id

  def getStorageLevel = rdd.getStorageLevel

  def iterator(split: Split): java.util.Iterator[T] = asJavaIterator(rdd.iterator(split))

  // Transformations (return a new RDD)

  def map[R](f: JFunction[T, R]): JavaRDD[R] =
    new JavaRDD(rdd.map(f)(f.returnType()))(f.returnType())

  def map[R](f: DoubleFunction[T]): JavaDoubleRDD =
    new JavaDoubleRDD(rdd.map(x => f(x).doubleValue()))

  def map[K2, V2](f: PairFunction[T, K2, V2]): JavaPairRDD[K2, V2] = {
    def cm = implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K2, V2]]]
    new JavaPairRDD(rdd.map(f)(cm))(f.keyType(), f.valueType())
  }

  def flatMap[U](f: FlatMapFunction[T, U]): JavaRDD[U] = {
    import scala.collection.JavaConverters._
    def fn = (x: T) => f.apply(x).asScala
    JavaRDD.fromRDD(rdd.flatMap(fn)(f.elementType()))(f.elementType())
  }

  def flatMap(f: DoubleFlatMapFunction[T]): JavaDoubleRDD = {
    import scala.collection.JavaConverters._
    def fn = (x: T) => f.apply(x).asScala
    new JavaDoubleRDD(rdd.flatMap(fn).map((x: java.lang.Double) => x.doubleValue()))
  }

  def flatMap[K, V](f: PairFlatMapFunction[T, K, V]): JavaPairRDD[K, V] = {
    import scala.collection.JavaConverters._
    def fn = (x: T) => f.apply(x).asScala
    def cm = implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[Tuple2[K, V]]]
    new JavaPairRDD(rdd.flatMap(fn)(cm))(f.keyType(), f.valueType())
  }

  def cartesian[U](other: JavaRDDLike[U, _]): JavaPairRDD[T, U] =
    JavaPairRDD.fromRDD(rdd.cartesian(other.rdd)(other.classManifest))(classManifest,
      other.classManifest)

  def groupBy[K](f: JFunction[T, K]): JavaPairRDD[K, JList[T]] = {
    implicit val kcm: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val vcm: ClassManifest[JList[T]] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[JList[T]]]
    JavaPairRDD.fromRDD(groupByResultToJava(rdd.groupBy(f)(f.returnType)))(kcm, vcm)
  }

  def groupBy[K](f: JFunction[T, K], numSplits: Int): JavaPairRDD[K, JList[T]] = {
    implicit val kcm: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val vcm: ClassManifest[JList[T]] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[JList[T]]]
    JavaPairRDD.fromRDD(groupByResultToJava(rdd.groupBy(f, numSplits)(f.returnType)))(kcm, vcm)
  }

  def pipe(command: String): JavaRDD[String] = rdd.pipe(command)

  def pipe(command: JList[String]): JavaRDD[String] =
    rdd.pipe(asScalaBuffer(command))

  def pipe(command: JList[String], env: java.util.Map[String, String]): JavaRDD[String] =
    rdd.pipe(asScalaBuffer(command), mapAsScalaMap(env))

  // Actions (launch a job to return a value to the user program)

  def foreach(f: VoidFunction[T]) {
    val cleanF = rdd.context.clean(f)
    rdd.foreach(cleanF)
  }

  def collect(): JList[T] = {
    import scala.collection.JavaConversions._
    val arr: java.util.Collection[T] = rdd.collect().toSeq
    new java.util.ArrayList(arr)
  }

  def reduce(f: JFunction2[T, T, T]): T = rdd.reduce(f)

  def fold(zeroValue: T)(f: JFunction2[T, T, T]): T =
    rdd.fold(zeroValue)(f)

  def aggregate[U](zeroValue: U)(seqOp: JFunction2[U, T, U],
    combOp: JFunction2[U, U, U]): U =
    rdd.aggregate(zeroValue)(seqOp, combOp)(seqOp.returnType)

  def count() = rdd.count()

  def countApprox(timeout: Long, confidence: Double): PartialResult[BoundedDouble] =
    rdd.countApprox(timeout, confidence)

  def countApprox(timeout: Long): PartialResult[BoundedDouble] =
    rdd.countApprox(timeout)

  def countByValue(): java.util.Map[T, java.lang.Long] =
    mapAsJavaMap(rdd.countByValue().map((x => (x._1, new lang.Long(x._2)))))

  def countByValueApprox(
    timeout: Long,
    confidence: Double
    ): PartialResult[java.util.Map[T, BoundedDouble]] =
    rdd.countByValueApprox(timeout, confidence).map(mapAsJavaMap)

  def countByValueApprox(timeout: Long): PartialResult[java.util.Map[T, BoundedDouble]] =
    rdd.countByValueApprox(timeout).map(mapAsJavaMap)

  def take(num: Int): JList[T] = {
    import scala.collection.JavaConversions._
    val arr: java.util.Collection[T] = rdd.take(num).toSeq
    new java.util.ArrayList(arr)
  }

  def takeSample(withReplacement: Boolean, num: Int, seed: Int): JList[T] = {
    import scala.collection.JavaConversions._
    val arr: java.util.Collection[T] = rdd.takeSample(withReplacement, num, seed).toSeq
    new java.util.ArrayList(arr)
  }

  def first(): T = rdd.first()

  def saveAsTextFile(path: String) = rdd.saveAsTextFile(path)

  def saveAsObjectFile(path: String) = rdd.saveAsObjectFile(path)
}
