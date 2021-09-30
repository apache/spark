
package org.apache.spark.sql

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.types._

/**
 * :: Experimental(实验性的) ::
 * Used to convert a JVM object of type `T` to and from the internal Spark SQL representation.
 * 用于将类型为“T”的 JVM 对象与内部 Spark SQL 表示形式相互转换。
 * == Scala ==
 * Encoders are generally created automatically through implicits from a `SparkSession`, or can be
 * explicitly created by calling static methods on [[Encoders]].
 * 编码器通常通过来自 `SparkSession` 的隐式自动创建，或者可以通过调用 [[Encoders]] 上的静态方法显式创建
 * {{{
 *   import spark.implicits._
 *
 *   val ds = Seq(1, 2, 3).toDS() // implicitly provided (spark.implicits.newIntEncoder)
 * }}}
 *
 * == Java ==
 * Encoders are specified by calling static methods on [[Encoders]].
 *
 * {{{
 *   List<String> data = Arrays.asList("abc", "abc", "xyz");
 *   Dataset<String> ds = context.createDataset(data, Encoders.STRING());
 * }}}
 *
 * Encoders can be composed into tuples:
 *
 * {{{
 *   Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.STRING());
 *   List<Tuple2<Integer, String>> data2 = Arrays.asList(new scala.Tuple2(1, "a");
 *   Dataset<Tuple2<Integer, String>> ds2 = context.createDataset(data2, encoder2);
 * }}}
 *
 * Or constructed from Java Beans:
 *
 * {{{
 *   Encoders.bean(MyClass.class);
 * }}}
 *
 * == Implementation ==
 *  - Encoders are not required to be thread-safe and thus they do not need to use locks to guard
 *    against concurrent access if they reuse internal buffers to improve performance.
 * 编码器不需要是线程安全的，因此如果它们重用内部缓冲区来提高性能，则不需要使用锁来防止并发访问
 * @since 1.6.0
 */
@Experimental
@Evolving
@implicitNotFound("Unable to find encoder for type ${T}. An implicit Encoder[${T}] is needed to " +
  "store ${T} instances in a Dataset. Primitive types (Int, String, etc) and Product types (case " +
  "classes) are supported by importing spark.implicits._  Support for serializing other types " +
  "will be added in future releases.")
trait Encoder[T] extends Serializable {

  /** Returns the schema of encoding this type of object as a Row. */
  def schema: StructType

  /**
   * A ClassTag that can be used to construct an Array to contain a collection of `T`.
   */
  def clsTag: ClassTag[T]
}
