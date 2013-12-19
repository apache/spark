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

  /** Attempts to register a class with MsgPack, only if it is not a primitive or a String */
  def register[T](clazz: Class[T], msgpack: ScalaMessagePack) {
    //implicit val kcm = ClassManifest.fromClass(clazz)
    //val kc = kcm.erasure
    Try {
      //if (kc.isInstance("") || kc.isPrimitive) {
      //  log.info("Class: %s doesn't need to be registered".format(kc.getName))
      //} else {
      msgpack.register(clazz)
      log.info("Registered key/value class with MsgPack: %s".format(clazz))
      //}

    } match {
      case Failure(err)  =>
        log.warn("Failed to register class (%s) with MsgPack. ".format(clazz.getName) +
          "Falling back to default MsgPack serialization, or 'toString' as last resort. " +
          "Error: %s".format(err.getMessage))
      case Success(result) =>
    }
  }

  /** Serializes an RDD[(K, V)] -> RDD[Array[Byte]] using MsgPack */
  def serMsgPack[K, V](rdd: RDD[(K, V)]) = {
    import org.msgpack.ScalaMessagePack._
    rdd.mapPartitions{ pairs =>
      val mp = new ScalaMessagePack
      var triedReg = false
      pairs.map{ pair =>
        Try {
          if (!triedReg) {
            register(pair._1.getClass, mp)
            register(pair._2.getClass, mp)
            triedReg = true
          }
          mp.write(pair)
        } match {
          case Failure(err) =>
            log.debug("Failed to write", err)
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
  }

  /**
   * Converts an RDD of (K, V) pairs, where K and/or V could be instances of [[org.apache.hadoop.io.Writable]],
   * into an RDD[(K, V)]
   */
  def convertRDD[K, V](rdd: RDD[(K, V)]) = {
    rdd.map{
      case (k: Writable, v: Writable) => (convert(k).asInstanceOf[K], convert(v).asInstanceOf[V])
      case (k: Writable, v) => (convert(k).asInstanceOf[K], v.asInstanceOf[V])
      case (k, v: Writable) => (k.asInstanceOf[K], convert(v).asInstanceOf[V])
      case (k, v) => (k.asInstanceOf[K], v.asInstanceOf[V])
    }
  }

  /** Converts a [[org.apache.hadoop.io.Writable]] to the underlying primitive, String or object representation */
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
