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
package org.apache.spark.sql.proto

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column

// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name

  /**
   * Converts a binary column of Proto format into its corresponding catalyst value.
   * The specified schema must match actual schema of the read data, otherwise the behavior
   * is undefined: it may fail or return arbitrary result.
   * To deserialize the data with a compatible and evolved schema, the expected Proto schema can be
   * set via the option protoSchema.
   *
   * @param data         the binary column.
   * @param descFilePath the proto schema in Message GeneratedMessageV3 format.
   * @param messageName  the proto MessageName to look for in descriptorFile.
   * @since 3.4.0
   */
  @Experimental
  def from_proto(data: Column, descFilePath: String, messageName: String,
                 options: java.util.Map[String, String]): Column = {
    new Column(ProtoDataToCatalyst(data.expr, descFilePath, messageName, options.asScala.toMap))
  }

  /**
   * Converts a binary column of Proto format into its corresponding catalyst value.
   * The specified schema must match actual schema of the read data, otherwise the behavior
   * is undefined: it may fail or return arbitrary result.
   * To deserialize the data with a compatible and evolved schema, the expected Proto schema can be
   * set via the option protoSchema.
   *
   * @param data         the binary column.
   * @param descFilePath the proto schema in Message GeneratedMessageV3 format.
   * @param messageName  the proto MessageName to look for in descriptorFile.
   * @since 3.4.0
   */
  @Experimental
  def from_proto(data: Column, descFilePath: String, messageName: String): Column = {
    new Column(ProtoDataToCatalyst(data.expr, descFilePath, messageName, Map.empty))
  }

  /**
   * Converts a column into binary of proto format.
   *
   * @param data        the data column.
   * @since 3.4.0
   */
  @Experimental
  def to_proto(data: Column): Column = {
    new Column(CatalystDataToProto(data.expr, None, None))
  }

  /**
   * Converts a column into binary of proto format.
   *
   * @param data         the data column.
   * @param descFilePath the proto schema in Message GeneratedMessageV3 format.
   * @param messageName  the proto MessageName to look for in descriptorFile.
   * @since 3.4.0
   */
  @Experimental
  def to_proto(data: Column, descFilePath: String, messageName: String): Column = {
    new Column(CatalystDataToProto(data.expr, Some(descFilePath), Some(messageName)))
  }
}
