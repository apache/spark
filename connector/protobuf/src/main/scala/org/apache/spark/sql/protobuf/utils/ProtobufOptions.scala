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
package org.apache.spark.sql.protobuf.utils

import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailFastMode, ParseMode}

/**
 * Options for Protobuf Reader and Writer stored in case insensitive manner.
 */
private[sql] class ProtobufOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    @transient val conf: Configuration)
    extends FileSourceOptions(parameters)
    with Logging {

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(FailFastMode)

  /**
   * Adds support for recursive fields. If this option is is not specified, recursive fields are
   * not permitted. Setting it to 0 drops the recursive fields, 1 allows it to be recursed once,
   * and 2 allows it to be recursed twice and so on, up to 10. Values larger than 10 are not
   * allowed in order avoid inadvertently creating very large schemas. If a Protobuf message
   * has depth beyond this limit, the Spark struct returned is truncated after the recursion limit.
   *
   * Examples. Consider a Protobuf with a recursive field:
   *   `message Person { string name = 1; Person friend = 2; }`
   * The following lists the schema with different values for this setting.
   *  1:  struct<name: string>
   *  2:  struct<name string, friend: struct<name: string>>
   *  3:  struct<name string, friend: struct<name string, friend: struct<name: string>>>
   * and so on.
   */
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

  /**
   * This option enables converting Protobuf 'Any' fields to JSON. At runtime such 'Any' fields
   * can contain arbitrary Protobuf message serialized as binary data.
   *
   * By default when this option is not enabled, such field behaves like normal Protobuf message
   * with two fields (`STRUCT<type_url: STRING, value: BINARY>`). The binary `value` field is not
   * interpreted. This might not be convenient in practice.
   *
   * One option is to deserialize it into actual Protobuf message and convert it to Spark STRUCT.
   * But this is not feasible since the schema for `from_protobuf()` is needed at query compile
   * time and can not change at run time. As a result this is not feasible.
   *
   * Another option is parse the binary and deserialize the Protobuf message into JSON string.
   * This this lot more readable than the binary data. This configuration option enables
   * converting Any fields to JSON. The example blow clarifies further.
   *
   *  Consider two Protobuf types defined as follows:
   *    message ProtoWithAny {
   *       string event_name = 1;
   *       google.protobuf.Any details = 2;
   *    }
   *
   *    message Person {
   *      string name = 1;
   *      int32 id = 2;
   *   }
   *
   * With this option enabled, schema for `from_protobuf("col", messageName = "ProtoWithAny")`
   * would be : `STRUCT<event_name: STRING, details: STRING>`.
   * At run time, if `details` field contains `Person` Protobuf message, the returned value looks
   * like this with JSON string for `details`:
   *   ('click', '{"@type":"type.googleapis.com/...ProtoWithAny","name":"Mario","id":100}')
   *
   * Requirements:
   *  - The definitions for all the possible Protobuf types that are used in Any fields should be
   *    available in the Protobuf descriptor file passed to `from_protobuf()`. If any Protobuf
   *    is not found, it will result in error for that record.
   *  - This feature is supported with Java classes as well. But only the Protobuf types defined
   *    in the same `proto` file as the primary Java class might be visible.
   *    E.g. if `ProtoWithAny` and `Person` in above example are in different proto files,
   *    definition for `Person` may not be found.
   */
  val convertAnyFieldsToJson: Boolean =
    parameters.getOrElse("convert.any.fields.to.json", "false").toBoolean
}

private[sql] object ProtobufOptions {
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    val hadoopConf = SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new ProtobufOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }
}
