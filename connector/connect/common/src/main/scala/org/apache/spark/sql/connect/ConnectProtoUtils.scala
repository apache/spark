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

package org.apache.spark.sql.connect

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.common.ProtoUtils

/**
 * Utility functions for parsing Spark Connect protocol buffers with a recursion limit. This is
 * intended to be used by plugins, as they cannot use `ProtoUtils.parseWithRecursionLimit` due to
 * the shading of the `com.google.protobuf` package.
 */
object ConnectProtoUtils {
  @DeveloperApi
  def parsePlanWithRecursionLimit(bytes: Array[Byte], recursionLimit: Int): proto.Plan = {
    ProtoUtils.parseWithRecursionLimit(bytes, proto.Plan.parser(), recursionLimit)
  }

  @DeveloperApi
  def parseRelationWithRecursionLimit(bytes: Array[Byte], recursionLimit: Int): proto.Relation = {
    ProtoUtils.parseWithRecursionLimit(bytes, proto.Relation.parser(), recursionLimit)
  }

  @DeveloperApi
  def parseCommandWithRecursionLimit(bytes: Array[Byte], recursionLimit: Int): proto.Command = {
    ProtoUtils.parseWithRecursionLimit(bytes, proto.Command.parser(), recursionLimit)
  }

  @DeveloperApi
  def parseExpressionWithRecursionLimit(
      bytes: Array[Byte],
      recursionLimit: Int): proto.Expression = {
    ProtoUtils.parseWithRecursionLimit(bytes, proto.Expression.parser(), recursionLimit)
  }

  @DeveloperApi
  def parseDataTypeWithRecursionLimit(bytes: Array[Byte], recursionLimit: Int): proto.DataType = {
    ProtoUtils.parseWithRecursionLimit(bytes, proto.DataType.parser(), recursionLimit)
  }
}
