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

package org.apache.spark.sql.connect.planner

import org.apache.spark.connect.proto
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
 * This object offers methods to convert to/from connect proto to catalyst types.
 */
object DataTypeProtoConverter {
  def toCatalystType(t: proto.DataType): DataType = {
    t.getKindCase match {
      case proto.DataType.KindCase.I32 => IntegerType
      case proto.DataType.KindCase.STRING => StringType
      case _ =>
        throw InvalidPlanInput(s"Does not support convert ${t.getKindCase} to catalyst types.")
    }
  }

  def toConnectProtoType(t: DataType): proto.DataType = {
    t match {
      case IntegerType =>
        proto.DataType.newBuilder().setI32(proto.DataType.I32.getDefaultInstance).build()
      case StringType =>
        proto.DataType.newBuilder().setString(proto.DataType.String.getDefaultInstance).build()
      case _ =>
        throw InvalidPlanInput(s"Does not support convert ${t.typeName} to connect proto types.")
    }
  }
}
