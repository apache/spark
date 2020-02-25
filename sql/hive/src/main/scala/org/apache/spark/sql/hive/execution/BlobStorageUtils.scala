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

package org.apache.spark.sql.hive.execution

import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object BlobStorageUtils {
  def isBlobStoragePath(
      hadoopConf: Configuration,
      path: Path): Boolean = {
    path != null && isBlobStorageScheme(hadoopConf, Option(path.toUri.getScheme).getOrElse(""))
  }

  def isBlobStorageScheme(
      hadoopConf: Configuration,
      scheme: String): Boolean = {
    val supportedBlobSchemes = hadoopConf.get("hive.blobstore.supported.schemes", "s3,s3a,s3n")
    supportedBlobSchemes.toLowerCase(Locale.ROOT)
      .split(",")
      .map(_.trim)
      .toList
      .contains(scheme.toLowerCase(Locale.ROOT))
  }

  def useBlobStorageAsScratchDir(hadoopConf: Configuration): Boolean = {
    hadoopConf.get("hive.blobstore.use.blobstore.as.scratchdir", "true").toBoolean
  }
}
