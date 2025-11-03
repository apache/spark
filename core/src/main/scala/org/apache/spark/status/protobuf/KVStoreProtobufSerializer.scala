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

package org.apache.spark.status.protobuf

import java.lang.reflect.ParameterizedType
import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.apache.spark.status.KVUtils.KVStoreScalaSerializer

private[spark] class KVStoreProtobufSerializer extends KVStoreScalaSerializer {
  override def serialize(o: Object): Array[Byte] =
    KVStoreProtobufSerializer.getSerializer(o.getClass) match {
      case Some(serializer) => serializer.serialize(o)
      case _ => super.serialize(o)
    }

  override def deserialize[T](data: Array[Byte], klass: Class[T]): T =
    KVStoreProtobufSerializer.getSerializer(klass) match {
      case Some(serializer) =>
        serializer.deserialize(data).asInstanceOf[T]
      case _ => super.deserialize(data, klass)
    }
}

private[spark] object KVStoreProtobufSerializer {

  private[this] lazy val serializerMap: Map[Class[_], ProtobufSerDe[Any]] = {
    def getGenericsType(klass: Class[_]): Class[_] = {
      klass.getGenericInterfaces.head.asInstanceOf[ParameterizedType]
        .getActualTypeArguments.head.asInstanceOf[Class[_]]
    }
    ServiceLoader.load(classOf[ProtobufSerDe[Any]]).asScala.map { serDe =>
      getGenericsType(serDe.getClass) -> serDe
    }.toMap
  }

  def getSerializer(klass: Class[_]): Option[ProtobufSerDe[Any]] =
    serializerMap.get(klass)
}
