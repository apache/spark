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
package org.apache.spark.sql.catalyst.encoders

import java.lang.invoke.{MethodHandle, MethodHandles, MethodType}

import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.util.{SparkClassUtils, SparkSerDeUtils}

/**
 * Codec for doing conversions between two representations.
 *
 * @tparam I
 *   input type (typically the external representation of the data.
 * @tparam O
 *   output type (typically the internal representation of the data.
 */
trait Codec[I, O] {
  def encode(in: I): O
  def decode(out: O): I
}

/**
 * A codec that uses Java Serialization as its output format.
 */
class JavaSerializationCodec[I] extends Codec[I, Array[Byte]] {
  override def encode(in: I): Array[Byte] = SparkSerDeUtils.serialize(in)
  override def decode(out: Array[Byte]): I = SparkSerDeUtils.deserialize(out)
}

object JavaSerializationCodec extends (() => Codec[Any, Array[Byte]]) {
  override def apply(): Codec[Any, Array[Byte]] = new JavaSerializationCodec[Any]
}

/**
 * A codec that uses Kryo to (de)serialize arbitrary objects to and from a byte array.
 *
 * Please note that this is currently only supported for Classic Spark applications. The reason
 * for this is that Connect applications can have a significantly different classpath than the
 * driver or executor. This makes having a the same Kryo configuration on both the client and
 * server (driver & executors) very tricky. As a workaround a user can define their own Codec
 * which internalizes the Kryo configuration.
 */
object KryoSerializationCodec extends (() => Codec[Any, Array[Byte]]) {
  private lazy val kryoCodecConstructor: MethodHandle = {
    val cls = SparkClassUtils.classForName(
      "org.apache.spark.sql.catalyst.encoders.KryoSerializationCodecImpl")
    MethodHandles.lookup().findConstructor(cls, MethodType.methodType(classOf[Unit]))
  }

  override def apply(): Codec[Any, Array[Byte]] = {
    try {
      kryoCodecConstructor.invoke().asInstanceOf[Codec[Any, Array[Byte]]]
    } catch {
      case _: ClassNotFoundException =>
        throw ExecutionErrors.cannotUseKryoSerialization()
    }
  }
}
