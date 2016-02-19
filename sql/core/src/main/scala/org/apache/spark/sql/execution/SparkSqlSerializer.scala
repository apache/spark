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

package org.apache.spark.sql.execution

import java.nio.ByteBuffer
import java.util.{HashMap => JavaHashMap}

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.twitter.chill.ResourcePool

import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.util.MutablePair
import org.apache.spark.{SparkConf, SparkEnv}


private[sql] class SparkSqlSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = super.newKryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_, _]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericMutableRow])
    kryo.register(classOf[java.math.BigDecimal], new JavaBigDecimalSerializer)
    kryo.register(classOf[BigDecimal], new ScalaBigDecimalSerializer)

    kryo.register(classOf[Decimal])
    kryo.register(classOf[JavaHashMap[_, _]])

    kryo.setReferences(false)
    kryo
  }
}

private[execution] class KryoResourcePool(size: Int)
  extends ResourcePool[SerializerInstance](size) {

  val ser: SparkSqlSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SparkSqlSerializer(sparkConf)
  }

  def newInstance(): SerializerInstance = ser.newInstance()
}

private[sql] object SparkSqlSerializer {
  @transient lazy val resourcePool = new KryoResourcePool(30)

  private[this] def acquireRelease[O](fn: SerializerInstance => O): O = {
    val kryo = resourcePool.borrow
    try {
      fn(kryo)
    } finally {
      resourcePool.release(kryo)
    }
  }

  def serialize[T: ClassTag](o: T): Array[Byte] =
    acquireRelease { k =>
      k.serialize(o).array()
    }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T =
    acquireRelease { k =>
      k.deserialize[T](ByteBuffer.wrap(bytes))
    }
}

private[sql] class JavaBigDecimalSerializer extends Serializer[java.math.BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: java.math.BigDecimal) {
    // TODO: There are probably more efficient representations than strings...
    output.writeString(bd.toString)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[java.math.BigDecimal]): java.math.BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}

private[sql] class ScalaBigDecimalSerializer extends Serializer[BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: BigDecimal) {
    // TODO: There are probably more efficient representations than strings...
    output.writeString(bd.toString)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[BigDecimal]): BigDecimal = {
    new java.math.BigDecimal(input.readString())
  }
}
