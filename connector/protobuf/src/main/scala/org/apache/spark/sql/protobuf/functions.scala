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

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.protobuf.utils.ProtobufUtils

// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value. The
   * Protobuf definition is provided through Protobuf <i>descriptor file</i>.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf message name to look for in descriptor file.
   * @param descFilePath
   *   The Protobuf descriptor file. This file is usually created using `protoc` with
   *   `--descriptor_set_out` and `--include_imports` options.
   * @param options
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: java.util.Map[String, String]): Column = {
    val descriptorFileContent = ProtobufUtils.readDescriptorFileContent(descFilePath)
    from_protobuf(data, messageName, descriptorFileContent, options)
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value.The
   * Protobuf definition is provided through Protobuf `FileDescriptorSet`.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf MessageName to look for in the descriptor set.
   * @param binaryFileDescriptorSet
   *   Serialized Protobuf descriptor (`FileDescriptorSet`). Typically contents of file created
   *   using `protoc` with `--descriptor_set_out` and `--include_imports` options.
   *   @param options
   * @since 3.5.0
   */
  @Experimental
  def from_protobuf(
      data: Column,
      messageName: String,
      binaryFileDescriptorSet: Array[Byte],
      options: java.util.Map[String, String]): Column = {
    Column.internalFnWithOptions(
      "from_protobuf",
      options.asScala.iterator,
      data,
      lit(messageName),
      lit(binaryFileDescriptorSet)
    )
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value. The
   * Protobuf definition is provided through Protobuf <i>descriptor file</i>.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf MessageName to look for in descriptor file.
   * @param descFilePath
   *   The Protobuf descriptor file. This file is usually created using `protoc` with
   *   `--descriptor_set_out` and `--include_imports` options.
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(data: Column, messageName: String, descFilePath: String): Column = {
    val fileContent = ProtobufUtils.readDescriptorFileContent(descFilePath)
    from_protobuf(data, messageName, fileContent)
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value.The
   * Protobuf definition is provided through Protobuf `FileDescriptorSet`.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf MessageName to look for in the descriptor set.
   * @param binaryFileDescriptorSet
   *   Serialized Protobuf descriptor (`FileDescriptorSet`). Typically contents of file created
   *   using `protoc` with `--descriptor_set_out` and `--include_imports` options.
   * @since 3.5.0
   */
  @Experimental
  def from_protobuf(data: Column, messageName: String, binaryFileDescriptorSet: Array[Byte])
  : Column = {
    Column.internalFn(
      "from_protobuf",
      data,
      lit(messageName),
      lit(binaryFileDescriptorSet)
    )
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value.
   * `messageClassName` points to Protobuf Java class. The jar containing Java class should be
   * shaded. Specifically, `com.google.protobuf.*` should be shaded to
   * `org.sparkproject.spark_protobuf.protobuf.*`.
   * https://github.com/rangadi/shaded-protobuf-classes is useful to create shaded jar from
   * Protobuf files.
   *
   * @param data
   *   the binary column.
   * @param messageClassName
   *   The full name for Protobuf Java class. E.g. <code>com.example.protos.ExampleEvent</code>.
   *   The jar with these classes needs to be shaded as described above.
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(data: Column, messageClassName: String): Column = {
    Column.internalFn(
      "from_protobuf",
      data,
      lit(messageClassName)
    )
  }

  /**
   * Converts a binary column of Protobuf format into its corresponding catalyst value.
   * `messageClassName` points to Protobuf Java class. The jar containing Java class should be
   * shaded. Specifically, `com.google.protobuf.*` should be shaded to
   * `org.sparkproject.spark_protobuf.protobuf.*`.
   * https://github.com/rangadi/shaded-protobuf-classes is useful to create shaded jar from
   * Protobuf files.
   *
   * @param data
   *   the binary column.
   * @param messageClassName
   *   The full name for Protobuf Java class. E.g. <code>com.example.protos.ExampleEvent</code>.
   *   The jar with these classes needs to be shaded as described above.
   * @param options
   * @since 3.4.0
   */
  @Experimental
  def from_protobuf(
    data: Column,
    messageClassName: String,
    options: java.util.Map[String, String]): Column = {
    from_protobuf(data, messageClassName, "", options)
  }

  /**
   * Converts a column into binary of protobuf format. The Protobuf definition is provided
   * through Protobuf <i>descriptor file</i>.
   *
   * @param data
   *   the data column.
   * @param messageName
   *   the protobuf MessageName to look for in descriptor file.
   * @param descFilePath
   *   The Protobuf descriptor file. This file is usually created using `protoc` with
   *   `--descriptor_set_out` and `--include_imports` options.
   * @since 3.4.0
   */
  @Experimental
  def to_protobuf(data: Column, messageName: String, descFilePath: String): Column = {
    to_protobuf(data, messageName, descFilePath, Map.empty[String, String].asJava)
  }

  /**
   * Converts a column into binary of protobuf format.The Protobuf definition is provided
   * through Protobuf `FileDescriptorSet`.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf MessageName to look for in the descriptor set.
   * @param binaryFileDescriptorSet
   *   Serialized Protobuf descriptor (`FileDescriptorSet`). Typically contents of file created
   *   using `protoc` with `--descriptor_set_out` and `--include_imports` options.
   *
   * @since 3.5.0
   */
  @Experimental
  def to_protobuf(data: Column, messageName: String, binaryFileDescriptorSet: Array[Byte])
  : Column = {
    Column.internalFn(
      "to_protobuf",
      data,
      lit(messageName),
      lit(binaryFileDescriptorSet)
    )
  }
  /**
   * Converts a column into binary of protobuf format. The Protobuf definition is provided
   * through Protobuf <i>descriptor file</i>.
   *
   * @param data
   *   the data column.
   * @param messageName
   *   the protobuf MessageName to look for in descriptor file.
   * @param descFilePath
   *   the protobuf descriptor file.
   * @param options
   * @since 3.4.0
   */
  @Experimental
  def to_protobuf(
    data: Column,
    messageName: String,
    descFilePath: String,
    options: java.util.Map[String, String]): Column = {
    val fileContent = ProtobufUtils.readDescriptorFileContent(descFilePath)
    to_protobuf(data, messageName, fileContent, options)
  }

  /**
   * Converts a column into binary of protobuf format.The Protobuf definition is provided
   * through Protobuf `FileDescriptorSet`.
   *
   * @param data
   *   the binary column.
   * @param messageName
   *   the protobuf MessageName to look for in the descriptor set.
   * @param binaryFileDescriptorSet
   *   Serialized Protobuf descriptor (`FileDescriptorSet`). Typically contents of file created
   *   using `protoc` with `--descriptor_set_out` and `--include_imports` options.
   * @param options
   * @since 3.5.0
   */
  @Experimental
  def to_protobuf(
    data: Column,
    messageName: String,
    binaryFileDescriptorSet: Array[Byte],
    options: java.util.Map[String, String]
  ): Column = {
    Column.internalFnWithOptions(
      "to_protobuf",
      options.asScala.iterator,
      data,
      lit(messageName),
      lit(binaryFileDescriptorSet)
    )
  }

  /**
   * Converts a column into binary of protobuf format.
   * `messageClassName` points to Protobuf Java class. The jar containing Java class should be
   * shaded. Specifically, `com.google.protobuf.*` should be shaded to
   * `org.sparkproject.spark_protobuf.protobuf.*`.
   * https://github.com/rangadi/shaded-protobuf-classes is useful to create shaded jar from
   * Protobuf files.
   *
   * @param data
   *   the data column.
   * @param messageClassName
   *   The full name for Protobuf Java class. E.g. <code>com.example.protos.ExampleEvent</code>.
   *   The jar with these classes needs to be shaded as described above.
   * @since 3.4.0
   */
  @Experimental
  def to_protobuf(data: Column, messageClassName: String): Column = {
    Column.internalFn(
      "to_protobuf",
      data,
      lit(messageClassName)
    )
  }

  /**
   * Converts a column into binary of protobuf format.
   * `messageClassName` points to Protobuf Java class. The jar containing Java class should be
   * shaded. Specifically, `com.google.protobuf.*` should be shaded to
   * `org.sparkproject.spark_protobuf.protobuf.*`.
   * https://github.com/rangadi/shaded-protobuf-classes is useful to create shaded jar from
   * Protobuf files.
   *
   * @param data
   *   the data column.
   * @param messageClassName
   *   The full name for Protobuf Java class. E.g. <code>com.example.protos.ExampleEvent</code>.
   *   The jar with these classes needs to be shaded as described above.
   * @param options
   * @since 3.4.0
   */
  @Experimental
  def to_protobuf(data: Column, messageClassName: String, options: java.util.Map[String, String])
  : Column = {
    to_protobuf(data, messageClassName, "", options)
  }
}
