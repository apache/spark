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

import org.apache.spark.annotation.{DeveloperApi, Unstable}

/**
 * :: DeveloperApi ::
 * `ProtobufSerDe` used to represent the API for serialize and deserialize of
 * Protobuf data related to UI. The subclass should implement this trait and
 * register itself to `org.apache.spark.status.protobuf.ProtobufSerDe` so that
 * `KVStoreProtobufSerializer` can use `ServiceLoader` to load and use them.
 *
 * TODO: SPARK-41644 How to define `ProtobufSerDe` as `ProtobufSerDe[T]`
 *
 * @since 3.4.0
 */
@DeveloperApi
@Unstable
trait ProtobufSerDe {

  /**
   * Specify the data types supported by the current `ProtobufSerDe`
   */
  val supportClass: Class[_]

  /**
   * Serialize the input data of the type corresponding to `supportClass`
   * to `Array[Byte]`, since the current input parameter type is `Any`,
   * the input type needs to be guaranteed from the code level.
   */
  def serialize(input: Any): Array[Byte]

  /**
   * Deserialize the input `Array[Byte]` to an object of the
   * type corresponding to `supportClass`.
   */
  def deserialize(bytes: Array[Byte]): Any
}
