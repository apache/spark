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

package org.apache.spark.sql.catalyst.catalog

abstract class BucketingInfoExtractor extends Serializable {
  /**
   * Given a input `filename`, computes the corresponding bucket id
   */
  def getBucketId(fileName: String): Option[Int]

  /**
   * Given a bucket id returns the string representation to be used in output file name
   */
  def bucketIdToString(id: Int): String

  def getBucketedFilename(split: Int,
                          uniqueWriteJobId: String,
                          bucketId: Option[Int],
                          extension: String): String
}

class DefaultBucketingInfoExtractor extends BucketingInfoExtractor {
  // The file name of bucketed data should have 3 parts:
  //   1. some other information in the head of file name
  //   2. bucket id part, some numbers, starts with "_"
  //      * The other-information part may use `-` as separator and may have numbers at the end,
  //        e.g. a normal parquet file without bucketing may have name:
  //        part-r-00000-2dd664f9-d2c4-4ffe-878f-431234567891.gz.parquet, and we will mistakenly
  //        treat `431234567891` as bucket id. So here we pick `_` as separator.
  //   3. optional file extension part, in the tail of file name, starts with `.`
  // An example of bucketed parquet file name with bucket id 3:
  //   part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
  private val bucketedFileName = """.*_(\d+)(?:\..*)?$""".r

  override def getBucketId(fileName: String): Option[Int] = fileName match {
    case bucketedFileName(bucketId) => Some(bucketId.toInt)
    case other => None
  }

  override def bucketIdToString(id: Int): String = f"_$id%05d"

  override def getBucketedFilename(split: Int,
                                   uniqueWriteJobId: String,
                                   bucketId: Option[Int],
                                   extension: String): String = {
    val bucketString = bucketId.map(bucketIdToString).getOrElse("")
    f"part-r-$split%05d-$uniqueWriteJobId$bucketString$extension"
  }
}

object DefaultBucketingInfoExtractor {
  val Instance = new DefaultBucketingInfoExtractor
}
