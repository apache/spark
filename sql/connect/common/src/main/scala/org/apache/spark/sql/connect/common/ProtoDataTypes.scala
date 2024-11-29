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

package org.apache.spark.sql.connect.common

import org.apache.spark.connect.proto

private[sql] object ProtoDataTypes {

  val NullType: proto.DataType = proto.DataType
    .newBuilder()
    .setNull(proto.DataType.NULL.getDefaultInstance)
    .build()

  val BooleanType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setBoolean(proto.DataType.Boolean.getDefaultInstance)
      .build()

  val BinaryType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setBinary(proto.DataType.Binary.getDefaultInstance)
      .build()

  val ByteType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setByte(proto.DataType.Byte.getDefaultInstance)
      .build()

  val ShortType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setShort(proto.DataType.Short.getDefaultInstance)
      .build()

  val IntegerType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setInteger(proto.DataType.Integer.getDefaultInstance)
      .build()

  val LongType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setLong(proto.DataType.Long.getDefaultInstance)
      .build()

  val FloatType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setFloat(proto.DataType.Float.getDefaultInstance)
      .build()

  val DoubleType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setDouble(proto.DataType.Double.getDefaultInstance)
      .build()

  val StringType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setString(proto.DataType.String.getDefaultInstance)
      .build()

  val DateType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setDate(proto.DataType.Date.getDefaultInstance)
      .build()

  val TimestampType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setTimestamp(proto.DataType.Timestamp.getDefaultInstance)
      .build()

  val TimestampNTZType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setTimestampNtz(proto.DataType.TimestampNTZ.getDefaultInstance)
      .build()

  val CalendarIntervalType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setCalendarInterval(proto.DataType.CalendarInterval.getDefaultInstance)
      .build()

  val VariantType: proto.DataType =
    proto.DataType
      .newBuilder()
      .setVariant(proto.DataType.Variant.getDefaultInstance)
      .build()
}
