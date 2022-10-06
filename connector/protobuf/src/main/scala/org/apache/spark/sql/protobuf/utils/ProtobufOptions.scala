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
}

private[sql] object ProtobufOptions {
  def apply(parameters: Map[String, String]): ProtobufOptions = {
    val hadoopConf = SparkSession.getActiveSession
      .map(_.sessionState.newHadoopConf())
      .getOrElse(new Configuration())
    new ProtobufOptions(CaseInsensitiveMap(parameters), hadoopConf)
  }
}
