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

import scala.reflect.ClassTag

import com.clearspring.analytics.stream.cardinality.HyperLogLog
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Serializer, Kryo}
import com.twitter.chill.{AllScalaRegistrar, ResourcePool}

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.serializer.{SerializerInstance, KryoSerializer}
import org.apache.spark.util.MutablePair
import org.apache.spark.util.Utils

private[sql] class SparkSqlSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = new Kryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_, _]])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericMutableRow])
    kryo.register(classOf[com.clearspring.analytics.stream.cardinality.HyperLogLog],
                  new HyperLogLogSerializer)
    kryo.register(classOf[scala.math.BigDecimal], new BigDecimalSerializer)
    kryo.setReferences(false)
    kryo.setClassLoader(Utils.getSparkClassLoader)
    new AllScalaRegistrar().apply(kryo)
    kryo
  }
}

private[execution] class KryoResourcePool(size: Int)
    extends ResourcePool[SerializerInstance](size) {

  val ser: KryoSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    // TODO (lian) Using KryoSerializer here is workaround, needs further investigation
    // Using SparkSqlSerializer here makes BasicQuerySuite to fail because of Kryo serialization
    // related error.
    new KryoSerializer(sparkConf)
  }

  def newInstance() = ser.newInstance()
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

private[sql] class BigDecimalSerializer extends Serializer[BigDecimal] {
  def write(kryo: Kryo, output: Output, bd: math.BigDecimal) {
    // TODO: There are probably more efficient representations than strings...
    output.writeString(bd.toString())
  }

  def read(kryo: Kryo, input: Input, tpe: Class[BigDecimal]): BigDecimal = {
    BigDecimal(input.readString())
  }
}

private[sql] class HyperLogLogSerializer extends Serializer[HyperLogLog] {
  def write(kryo: Kryo, output: Output, hyperLogLog: HyperLogLog) {
    val bytes = hyperLogLog.getBytes()
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[HyperLogLog]): HyperLogLog = {
    val length = input.readInt()
    val bytes = input.readBytes(length)
    HyperLogLog.Builder.build(bytes)
  }
}
