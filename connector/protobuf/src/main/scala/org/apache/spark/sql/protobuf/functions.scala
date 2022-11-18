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
package org.apache.spark.sql.protobuf

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column

// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value. The
   * specified schema must match actual schema of the read data, otherwise the behavior is
   * undefined: it may fail or return arbitrary result. To deserialize the data with a compatible
   * and evolved schema, the expected Protobuf schema can be set via the option protoSchema.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf message name to look for in descriptorFile.
   * @param descFilePath
   *   the protobuf descriptor in Message GeneratedMessageV3 format.
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: java.util.Map[String, String]): Column = {
    new Column(
      ProtobufDataToCatalyst(data.expr, messageName, Some(descFilePath), options.asScala.toMap)
    )
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value. The
   * specified schema must match actual schema of the read data, otherwise the behavior is
   * undefined: it may fail or return arbitrary result. To deserialize the data with a compatible
   * and evolved schema, the expected Protobuf schema can be set via the option protoSchema.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf MessageName to look for in descriptorFile.
   * @param descFilePath
   *   the protobuf descriptor in Message GeneratedMessageV3 format.
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(data: Column, messageName: String, descFilePath: String): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, messageName, descFilePath = Some(descFilePath)))
    // TODO: Add an option for user to provide descriptor file content as a buffer. This
    //       gives flexibility in how the content is fetched.
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value. The
   * specified Protobuf class must match the data, otherwise the behavior is
   * undefined: it may fail or return arbitrary result. The jar containing Java class should be
   * shaded. Specifically, `com.google.protobuf.*` should be shaded to
   * `org.sparkproject.spark-protobuf.protobuf.*`.
   *
   * @param data
   *   the binary column.
   * @param shadedMessageClassName
   *   The Protobuf class name. E.g. <code>org.spark.examples.protobuf.ExampleEvent</code>.
   *   The jar with these classes needs to be shaded as described above.
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(data: Column, shadedMessageClassName: String): Column = {
    new Column(ProtobufDataToCatalyst(data.expr, shadedMessageClassName))
  }

  /**
   * Converts a column into binary of protobuf format.
   *
   * @param data
   *   the data column.
   * @param messageName
   *   the protobuf MessageName to look for in descriptorFile.
   * @param descFilePath
   *   the protobuf descriptor in Message GeneratedMessageV3 format.
   * @since 3.4.0
   */
  @Experimental
  def to_protobuf(data: Column, messageName: String, descFilePath: String): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageName, Some(descFilePath)))
  }

  /**
   * Converts a column into binary of protobuf format.
   *
   * @param data
   *   the data column.
   * @param messageClassName
   *   The Protobuf class name. E.g. <code>org.spark.examples.protobuf.ExampleEvent</code>.
   * @since 3.4.0
   */
  @Experimental
  def to_protobuf(data: Column, messageClassName: String): Column = {
    new Column(CatalystDataToProtobuf(data.expr, messageClassName))
  }
}
