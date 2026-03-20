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

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.LocalTimeEncoder
import org.apache.spark.sql.connect.client.arrow.TimeTypeConnectOps
import org.apache.spark.sql.types.{DataType, TimeType}

/**
 * Optional type operations for Spark Connect Arrow serialization/deserialization.
 *
 * Handles Arrow-based data exchange on the Connect client side for framework-managed types. Used
 * by ArrowSerializer, ArrowDeserializer, and ArrowVectorReader.
 *
 * NOTE: No feature flag check -the Connect client must handle whatever types the server sends.
 * The feature flag controls server-side engine behavior; the client always needs to handle types
 * that exist in the encoder.
 *
 * Methods return Any to avoid referencing arrow-private types from the types.ops package. Call
 * sites in the arrow package cast to the expected types.
 *
 * @since 4.2.0
 */
trait ConnectArrowTypeOps extends Serializable {

  def encoder: AgnosticEncoder[_]

  /** Creates an Arrow serializer for writing values to a vector. Returns a Serializer. */
  def createArrowSerializer(vector: Any): Any

  /** Creates an Arrow deserializer for reading values from a vector. Returns a Deserializer. */
  def createArrowDeserializer(enc: AgnosticEncoder[_], vector: Any, timeZoneId: String): Any

  /** Creates an ArrowVectorReader for this type's vector. Returns an ArrowVectorReader. */
  def createArrowVectorReader(vector: Any): Any
}

/**
 * Factory object for ConnectArrowTypeOps lookup.
 *
 * No feature flag check -the Connect client always handles registered types.
 */
object ConnectArrowTypeOps {

  /** Encoder-keyed dispatch (for ArrowSerializer, ArrowDeserializer). */
  def apply(enc: AgnosticEncoder[_]): Option[ConnectArrowTypeOps] =
    enc match {
      case LocalTimeEncoder => Some(new TimeTypeConnectOps(TimeType()))
      case _ => None
    }

  /** DataType-keyed dispatch (for ArrowVectorReader which doesn't have an encoder). */
  def apply(dt: DataType): Option[ConnectArrowTypeOps] =
    dt match {
      case tt: TimeType => Some(new TimeTypeConnectOps(tt))
      case _ => None
    }
}
