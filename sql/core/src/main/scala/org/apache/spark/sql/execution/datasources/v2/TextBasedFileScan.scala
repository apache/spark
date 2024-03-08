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
package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

abstract class TextBasedFileScan(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap) extends FileScan {
  @transient private lazy val codecFactory: CompressionCodecFactory = new CompressionCodecFactory(
    sparkSession.sessionState.newHadoopConfWithOptions(options.asScala.toMap))

  override def isSplitable(path: Path): Boolean = Utils.isFileSplittable(path, codecFactory)

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    "the file is compressed by unsplittable compression codec"
  }
}
