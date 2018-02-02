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

package org.apache.spark.sql.catalyst

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.Utils

/*
 * Trait for user-defined Encoder to be used within ExpressionEncoder.
 * e.g. Define a avro field (AvroEncoder) in scala case class (ExpressionEncoder) in Dataset.
 *
 * User needs to configure the user-defined type and Encoder(which extends this trait)
 * through conf spark.expressionencoder:
 * e.g. spark.expressionencoder.org.apache.avro.specific.SpecificRecord
 *      = com.databricks.spark.avro.AvroEncoder$
 * This enables Dataset of case class can have SpecificRecord typed field to use AvroEncoder to
 * ser/de this field within the case class.
 *
 * Encoder class extending this trait needs to have a default no-arg constructor, Encoder
 * singleton object extending this trait needs to have class name suffix $ in the conf.
 */
trait EncoderWithinExpressionEncoder {
  /**
   * get the SQL data type for given class type T.
   * This is required in ExpressionEncoder's schemaFor method to get SQL schema for
   * user-defined field.
   *
   * @param inputClass class of T
   * @return DataType spark sql schema of T
   */
  def schemaFor[T](inputClass: Class[T]): DataType

  /**
   * get the serializer used to serializer object type T into internal row.
   * This is required in ExpressionEncoder's serializerFor method to get serializer for
   * user-defined field.
   *
   * @param inputObject input object T as Expression
   * @param inputClass class of T
   * @return serializer as Expression
   */
  def serializerFor[T](inputObject: Expression, inputClass: Class[T]): Expression

  /**
   * get the deserializer used to deserializer internal row into object type T.
   * This is required in ExpressionEncoder's deserializerFor method to get deserializer for
   * user-defined field.
   *
   * @param path input path as Expression
   * @param inputClass class of T
   * @return deserializer as Expression
   */
  def deserializerFor[T](path: Option[Expression], inputClass: Class[T]): Expression
}

/*
 * Util class for using user-defined type and Encoder within ExpressionEncoder.
 *
 * The user-defined Encoder must extend EncoderWithinExpressionEncoder trait.
 *
 * Encoder extending this trait needs to have a default no-arg constructor, singleton
 * Encoder object extending this trait needs to have class name suffix $ in the conf
 */
object ExpressionEncoderUtils {

  /**
   * Get the spark conf from SparkEnv
   */
  lazy val conf: SparkConf = {
    if (SparkEnv.get != null) {
      SparkEnv.get.conf
    } else {
      // Fall back if SparkEnv not initialized. e.g. unit test
      new SparkConf
    }
  }

  /**
   * Get the array of user-defined types and the encoders that can be used
   * inside ExpressionEncoder.
   */
  lazy private val typeClassToEncoderClass: Array[(Class[_], Class[_])] =
    (conf)
      .getAllWithPrefix("spark.expressionencoder.")
      .filter{case(k, v) => Utils.classIsLoadable(k) && Utils.classIsLoadable(v)}
      .map{case(k, v) => (Utils.classForName(k), Utils.classForName(v))}

  /**
   * Encoder instance cache for the user-defined types
   * to avoid duplicate Encoder instantiation.
   */
  lazy private val encoderCache: MutableMap[Class[_], EncoderWithinExpressionEncoder] =
    MutableMap[Class[_], EncoderWithinExpressionEncoder]()

  /**
   * Check if given user-defined type has Encoder configured.
   */
  def hasEncoderForClass(clz: Class[_]): Boolean =
    encoderCache.contains(clz) ||
    typeClassToEncoderClass.filter(_._1 isAssignableFrom clz).size > 0

  /**
   * Return Encoder for user-defined type.
   */
  def getEncoderForClass(clz: Class[_]): EncoderWithinExpressionEncoder = {
    encoderCache.getOrElseUpdate(clz, findEncoderForClass(clz))
  }

  /**
   * Instantiate and return Encoder instance for the user-defined type.
   * Check only one Encoder is present for the type and the Encoder class must
   * implement trait EncoderWithinExpressionEncoder
   */
  private def findEncoderForClass(clz: Class[_]): EncoderWithinExpressionEncoder = {
    val encoders = typeClassToEncoderClass.filter(_._1 isAssignableFrom clz)
      .map(_._2)
    assert(encoders.size == 1,
      s"More than one encoder in spark.expressionencoder exists "
      + s"for class: $clz.getName")

    val encoder = encoders.head
    assert(classOf[EncoderWithinExpressionEncoder] isAssignableFrom encoder,
      s"${encoder} does not extend trait EncoderWithinExpressionEncoder")

    // If encoder is a singleton object(end with $), return the singleton object
    if (encoder.getName.endsWith("$")) {
      encoder.getField("MODULE$").get(null)
        .asInstanceOf[EncoderWithinExpressionEncoder]
    } else {
      // The encoder should be a class that has no-arg constructor
      encoder.newInstance.asInstanceOf[EncoderWithinExpressionEncoder]
    }
  }
}
