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

  // Setting the `recursive.fields.max.depth` to 1 allows it to be recurse once,
  // and 2 allows it to be recursed twice and so on. A value of `recursive.fields.max.depth`
  // greater than 10 is not permitted. If it is not  specified, the default value is -1;
  // A value of 0 or below disallows any recursive fields. If a protobuf
  // record has more depth than the allowed value for recursive fields, it will be truncated
  // and corresponding fields are ignored (dropped).
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

  // Whether to render fields with zero values when deserializing Protobufs to a Spark struct.
  // When a field is empty in the serialized Protobuf, this library will deserialize them as
  // null by default. However, this flag can control whether to render the type-specific zero value.
  // This operates similarly to `includingDefaultValues` in protobuf-java-util's JsonFormat, or
  // `emitDefaults` in golang/protobuf's jsonpb.
  //
  // As an example:
  // ```
  // syntax = "proto3";
  // message Person {
  //   string name = 1;
  //   int64 age = 2;
  //   optional string middle_name = 3;
  //   optional int64 salary = 4;
  // }
  // ```
  //
  // And we have a proto constructed like:
  // `Person(age=0, middle_name="")
  //
  // The result after calling from_protobuf without this flag set would be:
  // `{"name": null, "age": null, "middle_name": "", "salary": null}`
  // (age is null because zero-value singular fields are not in the wire format in proto3).
  //
  //
  // With this flag it would be:
  // `{"name": "", "age": 0, "middle_name": "", "salary": null}`
  //
  // Ref: https://protobuf.dev/programming-guides/proto3/#default for information about
  //      type-specific defaults.
  // Ref: https://protobuf.dev/programming-guides/field_presence/ for information about
  //      what information is available in a serialized proto.
  val emitDefaultValues: Boolean =
    parameters.getOrElse("emit.default.values", false.toString).toBoolean
}

private[sql] object ProtobufOptions {
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    val hadoopConf = SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new ProtobufOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }
}
