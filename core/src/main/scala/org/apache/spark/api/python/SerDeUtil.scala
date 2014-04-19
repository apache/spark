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

package org.apache.spark.api.python

import org.msgpack.ScalaMessagePack
import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging}
import org.apache.hadoop.io._
import scala.util.Success
import scala.util.Failure

/**
 * Utilities for serialization / deserialization between Python and Java, using MsgPack.
 * Also contains utilities for converting [[org.apache.hadoop.io.Writable]] -> Scala objects and primitives
 */
private[python] object SerDeUtil extends Logging {

  /**
   * Checks whether a Scala object needs to be registered with MsgPack. String, primitives and the standard collections
   * don't need to be registered as MsgPack takes care of serializing them and registering them throws scary looking
   * errors (but still works).
   */
  def needsToBeRegistered[T](t: T) = {
    t match {
      case d: Double => false
      case f: Float => false
      case i: Int => false
      case l: Long => false
      case b: Byte => false
      case c: Char => false
      case bool: Boolean => false
      case s: String => false
      case m: Map[_, _] => false
      case a: Seq[_] => false
      case o: Option[_] => false
      case _ => true
    }
  }

  /** Attempts to register a class with MsgPack */
  def register[T](t: T, msgpack: ScalaMessagePack) {
    if (!needsToBeRegistered(t)) {
      return
    }
    val clazz = t.getClass
    Try {
      msgpack.register(clazz)
      log.info(s"Registered key/value class with MsgPack: $clazz")
    } match {
      case Failure(err)  =>
        log.warn(s"""Failed to register class ($clazz) with MsgPack.
        Falling back to default MsgPack serialization, or 'toString' as last resort.
        Error: ${err.getMessage}""")
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
            register(pair._1, mp)
            register(pair._2, mp)
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
   * Converts an RDD of key-value pairs, where key and/or value could be instances of [[org.apache.hadoop.io.Writable]],
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
