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

import org.apache.spark.status.{ApplicationEnvironmentInfoWrapper, JobDataWrapper, TaskDataWrapper}
import org.apache.spark.status.KVUtils.KVStoreScalaSerializer

private[spark] class KVStoreProtobufSerializer extends KVStoreScalaSerializer {
  override def serialize(o: Object): Array[Byte] = o match {
    case j: JobDataWrapper => JobDataWrapperSerializer.serialize(j)
    case t: TaskDataWrapper => TaskDataWrapperSerializer.serialize(t)
    case a: ApplicationEnvironmentInfoWrapper =>
      ApplicationEnvironmentInfoWrapperSerializer.serialize(a)
    case other => super.serialize(other)
  }

  override def deserialize[T](data: Array[Byte], klass: Class[T]): T = klass match {
    case _ if classOf[JobDataWrapper].isAssignableFrom(klass) =>
      JobDataWrapperSerializer.deserialize(data).asInstanceOf[T]
    case _ if classOf[TaskDataWrapper].isAssignableFrom(klass) =>
      TaskDataWrapperSerializer.deserialize(data).asInstanceOf[T]
    case _ if classOf[ApplicationEnvironmentInfoWrapper].isAssignableFrom(klass) =>
      ApplicationEnvironmentInfoWrapperSerializer.deserialize(data).asInstanceOf[T]
    case other => super.deserialize(data, klass)
  }
}
