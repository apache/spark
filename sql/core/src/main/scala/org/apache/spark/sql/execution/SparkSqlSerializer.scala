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

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Serializer, Kryo}

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.MutablePair
import org.apache.spark.util.Utils

private[sql] class SparkSqlSerializer(conf: SparkConf) extends KryoSerializer(conf) {
  override def newKryo(): Kryo = {
    val kryo = new Kryo()
    kryo.setRegistrationRequired(false)
    kryo.register(classOf[MutablePair[_, _]])
    kryo.register(classOf[Array[Any]])
    // This is kinda hacky...
    kryo.register(classOf[scala.collection.immutable.Map$Map1], new MapSerializer)
    kryo.register(classOf[scala.collection.immutable.Map$Map2], new MapSerializer)
    kryo.register(classOf[scala.collection.immutable.Map$Map3], new MapSerializer)
    kryo.register(classOf[scala.collection.immutable.Map$Map4], new MapSerializer)
    kryo.register(classOf[scala.collection.immutable.Map[_,_]], new MapSerializer)
    kryo.register(classOf[scala.collection.Map[_,_]], new MapSerializer)
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericRow])
    kryo.register(classOf[org.apache.spark.sql.catalyst.expressions.GenericMutableRow])
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.math.BigDecimal], new BigDecimalSerializer)
    kryo.setReferences(false)
    kryo.setClassLoader(Utils.getSparkClassLoader)
    kryo
  }
}

private[sql] object SparkSqlSerializer {
  // TODO (lian) Using KryoSerializer here is workaround, needs further investigation
  // Using SparkSqlSerializer here makes BasicQuerySuite to fail because of Kryo serialization
  // related error.
  @transient lazy val ser: KryoSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new KryoSerializer(sparkConf)
  }

  def serialize[T](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
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

/**
 * Maps do not have a no arg constructor and so cannot be serialized by default. So, we serialize
 * them as `Array[(k,v)]`.
 */
private[sql] class MapSerializer extends Serializer[Map[_,_]] {
  def write(kryo: Kryo, output: Output, map: Map[_,_]) {
    kryo.writeObject(output, map.flatMap(e => Seq(e._1, e._2)).toArray)
  }

  def read(kryo: Kryo, input: Input, tpe: Class[Map[_,_]]): Map[_,_] = {
    kryo.readObject(input, classOf[Array[Any]])
      .sliding(2,2)
      .map { case Array(k,v) => (k,v) }
      .toMap
  }
}
