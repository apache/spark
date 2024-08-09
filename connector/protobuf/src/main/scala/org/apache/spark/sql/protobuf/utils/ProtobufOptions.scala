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

  import ProtobufOptions._

  def this(parameters: Map[String, String], conf: Configuration) = {
    this(CaseInsensitiveMap(parameters), conf)
  }

  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(FailFastMode)

  /**
   * Adds support for recursive fields. If this option is is not specified, recursive fields are
   * not permitted. Setting it to 1 drops the recursive fields, 0 allows it to be recursed once,
   * and 3 allows it to be recursed twice and so on, up to 10. Values larger than 10 are not
   * allowed in order avoid inadvertently creating very large schemas. If a Protobuf message
   * has depth beyond this limit, the Spark struct returned is truncated after the recursion limit.
   *
   * Examples. Consider a Protobuf with a recursive field:
   *   `message Person { string name = 1; Person friend = 2; }`
   * The following lists the schema with different values for this setting.
   *  1:  `struct<name: string>`
   *  2:  `struct<name: string, friend: struct<name: string>>`
   *  3:  `struct<name: string, friend: struct<name: string, friend: struct<name: string>>>`
   * and so on.
   */
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

  /**
   * This option ("convert.any.fields.to.json") enables converting Protobuf 'Any' fields to JSON.
   * At runtime, such 'Any' fields can contain arbitrary Protobuf messages as binary data.
   *
   * By default when this option is not enabled, such field behaves like normal Protobuf message
   * with two fields (`STRUCT<type_url: STRING, value: BINARY>`). The binary `value` field is not
   * interpreted. The binary data might not be convenient in practice to work with.
   *
   * One option is to deserialize it into actual Protobuf message and convert it to Spark STRUCT.
   * But this is not feasible since the schema for `from_protobuf()` is needed at query compile
   * time and can not change at runtime. As a result, this option is not feasible.
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
   * like this:
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
   *
   * This feature should be enabled carefully. JSON conversion and processing are inefficient.
   * In addition schema safety is also reduced making downstream processing error prone.
   */
  val convertAnyFieldsToJson: Boolean =
    parameters.getOrElse(CONVERT_ANY_FIELDS_TO_JSON_CONFIG, "false").toBoolean

  // Whether to render fields with zero values when deserializing Protobuf to a Spark struct.
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
  // `Person(age=0, middle_name="")`
  //
  // The result after calling from_protobuf() without this flag set would be:
  // `{"name": null, "age": null, "middle_name": "", "salary": null}`
  // (age is null because zero-value singular fields are not in the wire format in proto3).
  //
  //
  // With this flag it would be:
  // `{"name": "", "age": 0, "middle_name": "", "salary": null}`
  // ("salary" remains null, since it is declared explicitly as an optional field)
  //
  // Ref: https://protobuf.dev/programming-guides/proto3/#default for information about
  //      type-specific defaults.
  // Ref: https://protobuf.dev/programming-guides/field_presence/ for information about
  //      what information is available in a serialized proto.
  val emitDefaultValues: Boolean =
    parameters.getOrElse("emit.default.values", false.toString).toBoolean

  // Whether to render enum fields as their integer values.
  //
  // As an example, consider the following message type:
  // ```
  // syntax = "proto3";
  // message Person {
  //   enum Job {
  //     NONE = 0;
  //     ENGINEER = 1;
  //     DOCTOR = 2;
  //   }
  //   Job job = 1;
  // }
  // ```
  //
  // If we have an instance of this message like `Person(job = ENGINEER)`, then the
  // default deserialization will be:
  // `{"job": "ENGINEER"}`
  //
  // But with this option set the deserialization will be:
  // `{"job": 1}`
  //
  // Please note the output struct type will now contain an int column
  // instead of string, so use caution if changing existing parsing logic.
  val enumsAsInts: Boolean =
    parameters.getOrElse("enums.as.ints", false.toString).toBoolean

  // Protobuf supports unsigned integer types uint32 and uint64. By default this library
  // will serialize them as the signed IntegerType and LongType respectively. For very
  // large unsigned values this can cause overflow, causing these numbers
  // to be represented as negative (above 2^31 for uint32
  // and above 2^63 for uint64).
  //
  // Enabling this option will upcast unsigned integers into a larger type,
  // i.e. LongType for uint32 and Decimal(20, 0) for uint64 so their representation
  // can contain large unsigned values without overflow.
  val upcastUnsignedInts: Boolean =
    parameters.getOrElse("upcast.unsigned.ints", false.toString).toBoolean

  // Whether to unwrap the struct representation for well known primitive wrapper types when
  // deserializing. By default, the wrapper types for primitives (i.e. google.protobuf.Int32Value,
  // google.protobuf.Int64Value, etc.) will get deserialized as structs. We allow the option to
  // deserialize them as their respective primitives.
  // https://protobuf.dev/reference/protobuf/google.protobuf/
  //
  // For example, given a message like:
  // ```
  // syntax = "proto3";
  // message Example {
  //   google.protobuf.Int32Value int_val = 1;
  // }
  // ```
  //
  // The message Example(Int32Value(1)) would be deserialized by default as
  // {int_val: {value: 5}}
  //
  // However, with this option set, it would be deserialized as
  // {int_val: 5}
  //
  // NOTE: With `emit.default.values`, we won't fill in the default primitive value during
  // this unwrapping; this behavior preserves as much information as possible.
  // Concretely, the behavior with emit defaults and this option set is:
  //    nil => nil, Int32Value(0) => 0, Int32Value(100) => 100.
  val unwrapWellKnownTypes: Boolean =
    parameters.getOrElse("unwrap.primitive.wrapper.types", false.toString).toBoolean

  // Since Spark doesn't allow writing empty StructType, empty proto message type will be
  // dropped by default. Setting this option to true will insert a dummy column to empty proto
  // message so that the empty message will be retained.
  // For example, an empty message is used as field in another message:
  //
  // ```
  // message A {}
  // message B {A a = 1, string name = 2}
  // ```
  //
  // By default, in the spark schema field a will be dropped, which result in schema
  // b struct<name: string>
  // If retain.empty.message.types=true, field a will be retained by inserting a dummy column.
  // b struct<a: struct<__dummy_field_in_empty_struct: string>, name: string>
  val retainEmptyMessage: Boolean =
    parameters.getOrElse("retain.empty.message.types", false.toString).toBoolean
}

private[sql] object ProtobufOptions {
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    val hadoopConf = SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new ProtobufOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }

  val CONVERT_ANY_FIELDS_TO_JSON_CONFIG = "convert.any.fields.to.json"
}
