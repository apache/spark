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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FileSourceOptions
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

abstract class TextBasedFileScan(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap) extends FileScan {
  private def newHadoopConfWithScanOptions(
      legacyLowercaseKeys: Boolean = false): Configuration = {
    val map = if (legacyLowercaseKeys) {
      options.asScala.toMap
    } else {
      options.asCaseSensitiveMap.asScala.toMap
    }
    sparkSession.sessionState.newHadoopConfWithOptions(map)
  }

  @transient private lazy val codecFactory: CompressionCodecFactory =
    new CompressionCodecFactory(newHadoopConfWithScanOptions(
      conf.getConf(SQLConf.LEGACY_LOWERCASE_DATA_SOURCE_OPTIONS_IN_HADOOP_CONF)))

  // The splittability of the `decompression` read option's codec, or `None` when it is absent.
  @transient private lazy val decompressionCodecSplittable: Option[Boolean] =
    new FileSourceOptions(options.asScala.toMap).decompressionCodecSplittable

  /**
   * Whether the file at `path` can be split by its compression codec. When the `decompression`
   * read option is set, the splittability of that codec wins over the file name extension, so
   * the split decision always agrees with how the readers decompress (see [[newReadHadoopConf]]).
   */
  override def isSplitable(path: Path): Boolean =
    decompressionCodecSplittable.getOrElse(Utils.isFileSplittable(path, codecFactory))

  /**
   * Creates the Hadoop configuration the readers use, from the scan options (Hadoop configurations
   * are case sensitive), with the `decompression` read option, when set, forced onto it so
   * the readers decompress every file with that codec instead of inferring it from the file name
   * extension (see [[CompressionCodecs.setReadCodecConfiguration]]).
   */
  protected def newReadHadoopConf(): Configuration = {
    val hadoopConf = newHadoopConfWithScanOptions()
    CompressionCodecs.setReadCodecConfiguration(
      hadoopConf, new FileSourceOptions(options.asScala.toMap).decompressionCodec)
    hadoopConf
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    "the file is compressed by unsplittable compression codec"
  }
}
