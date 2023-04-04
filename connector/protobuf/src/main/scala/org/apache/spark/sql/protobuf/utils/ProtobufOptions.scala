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

  // Setting the `recursive.fields.max.depth` to 1 allows it to be recurse once,
  // and 2 allows it to be recursed twice and so on. A value of `recursive.fields.max.depth`
  // greater than 10 is not permitted. If it is not  specified, the default value is -1;
  // A value of 0 or below disallows any recursive fields. If a protobuf
  // record has more depth than the allowed value for recursive fields, it will be truncated
  // and corresponding fields are ignored (dropped).
  val recursiveFieldMaxDepth: Int = parameters.getOrElse("recursive.fields.max.depth", "-1").toInt

  // Whether or not to explicitly materialize the default values in the resulting struct.
  // For example, if we have a proto like
  // syntax = "proto3";
  // message Example {
  //   string s = 1;
  //   int64 i = 2;
  // }
  //
  // And have the following proto created in proto
  // Example(s="", i=0)
  //
  // The resulting dataframe from parsing would be
  // {"s": null, "i": null}
  //
  // Since the default values are nulled out.
  // If you would like them to be filled in instead:
  // {"s": "", "i": 0}
  //
  // you can achieve that by enabling this flag.
  // Note: I am not sure what this does with proto2, as i have not tried.
  val materializeDefaults: Boolean =
    parameters.getOrElse(materializeDefaultsOption, "false").toBoolean
}

private[sql] object ProtobufOptions {
  val materializeDefaultsOption = "materializeDefaults"
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    val hadoopConf = SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new ProtobufOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }
}
