package org.apache.spark.api.python

import org.msgpack.ScalaMessagePack
import scala.util.Try
import org.apache.spark.rdd.RDD
import java.io.Serializable
import org.apache.spark.{SparkContext, Logging}
import org.apache.hadoop.io._
import scala.util.Success
import scala.util.Failure

/**
 * Utilities for serialization / deserialization between Python and Java, using MsgPack.
 * Also contains utilities for converting [[org.apache.hadoop.io.Writable]] -> Scala objects and primitives
 */
private[python] object SerDeUtil extends Logging {

  def register[T](clazz: Class[T], msgpack: ScalaMessagePack) {
    Try {
      log.info("%s".format(clazz))
      clazz match {
        case c if c.isPrimitive =>
        case c if c.isInstanceOf[java.lang.String] =>
        case _ => msgpack.register(clazz)
      }
    }.getOrElse(log.warn("Failed to register class (%s) with MsgPack. ".format(clazz.getName) +
      "Falling back to default MsgPack serialization, or 'toString' as last resort"))
  }

  // serialize and RDD[(K, V)] -> RDD[Array[Byte]] using MsgPack
  def serMsgPack[K, V](rdd: RDD[(K, V)]) = {
    import org.msgpack.ScalaMessagePack._
    val msgpack = new ScalaMessagePack with Serializable
    val first = rdd.first()
    val kc = ClassManifest.fromClass(first._1.getClass).asInstanceOf[ClassManifest[K]].erasure.asInstanceOf[Class[K]]
    val vc = ClassManifest.fromClass(first._2.getClass).asInstanceOf[ClassManifest[V]].erasure.asInstanceOf[Class[V]]
    register(kc, msgpack)
    register(vc, msgpack)
    rdd.map{ pair =>
      Try {
        msgpack.write(pair)
      } match {
        case Failure(err) =>
          Try {
            write((pair._1.toString, pair._2.toString))
          } match {
            case Success(result) => result
            case Failure(e) => throw e
          }
        case Success(result) => result
      }
    }
  }

  def convertRDD[K, V](rdd: RDD[(K, V)]) = {
    rdd.map{
      case (k: Writable, v: Writable) => (convert(k).asInstanceOf[K], convert(v).asInstanceOf[V])
      case (k: Writable, v) => (convert(k).asInstanceOf[K], v.asInstanceOf[V])
      case (k, v: Writable) => (k.asInstanceOf[K], convert(v).asInstanceOf[V])
      case (k, v) => (k.asInstanceOf[K], v.asInstanceOf[V])
    }
  }

  def convert(writable: Writable): Any = {
    import collection.JavaConversions._
    writable match {
      case iw: IntWritable => SparkContext.intWritableConverter().convert(iw)
      case dw: DoubleWritable => SparkContext.doubleWritableConverter().convert(dw)
      case lw: LongWritable => SparkContext.longWritableConverter().convert(lw)
      case fw: FloatWritable => SparkContext.floatWritableConverter().convert(fw)
      case t: Text => SparkContext.stringWritableConverter().convert(t)
      case bw: BooleanWritable => SparkContext.booleanWritableConverter().convert(bw)
      case byw: BytesWritable => SparkContext.bytesWritableConverter().convert(byw)
      case n: NullWritable => None
      case aw: ArrayWritable => aw.get().map(convert(_))
      case mw: MapWritable => mw.map{ case (k, v) => (convert(k), convert(v)) }.toMap
      case other => other
    }
  }

}
