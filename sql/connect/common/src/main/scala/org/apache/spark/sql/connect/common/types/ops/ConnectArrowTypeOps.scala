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

package org.apache.spark.sql.connect.common.types.ops

import org.apache.arrow.vector.FieldVector

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.connect.client.arrow.{ArrowDeserializers, ArrowSerializer, ArrowVectorReader}
import org.apache.spark.sql.connect.client.arrow.types.ops.TimeTypeConnectOps
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Optional type operations for Spark Connect Arrow serialization/deserialization.
 *
 * Handles Arrow-based data exchange on the Connect client side for framework-managed types. Used
 * by ArrowSerializer, ArrowDeserializer, and ArrowVectorReader.
 *
 * NOTE: No feature flag check - the Connect client must handle whatever types the server sends.
 * The feature flag controls server-side engine behavior; the client always needs to handle types
 * that exist in the encoder.
 *
 * @since 4.2.0
 */
trait ConnectArrowTypeOps extends Serializable {

  def encoder: AgnosticEncoder[_]

  /** Creates an Arrow serializer for writing values to a vector. */
  def createArrowSerializer(vector: AnyRef): ArrowSerializer.Serializer

  /** Creates an Arrow deserializer for reading values from a vector. */
  def createArrowDeserializer(
      enc: AgnosticEncoder[_],
      vector: FieldVector,
      timeZoneId: String): ArrowDeserializers.Deserializer[Any]

  /** Creates an ArrowVectorReader for this type's vector. */
  def createArrowVectorReader(vector: FieldVector): ArrowVectorReader
}

/**
 * Factory object for ConnectArrowTypeOps lookup.
 *
 * No feature flag check - the Connect client always handles registered types.
 */
object ConnectArrowTypeOps {

  /** Encoder-keyed dispatch (for ArrowSerializer, ArrowDeserializer). */
  def apply(enc: AgnosticEncoder[_]): Option[ConnectArrowTypeOps] =
    enc match {
      case LocalTimeEncoder => Some(new TimeTypeConnectOps(TimeType()))
      // Add new framework encoders here
      case _ => None
    }

  /** DataType-keyed dispatch (for ArrowVectorReader which doesn't have an encoder). */
  def apply(dt: DataType): Option[ConnectArrowTypeOps] =
    dt match {
      case tt: TimeType => Some(new TimeTypeConnectOps(tt))
      // Add new framework types here
      case _ => None
    }
}
