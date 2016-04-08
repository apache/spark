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

package org.apache.spark.deploy

import java.util.Date

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.{InputSplit, FileInputFormat, JobConf}

import org.apache.spark.SparkFunSuite

class SparkS3UtilSuite extends SparkFunSuite {
  test("s3ListingEnabled function") {
    val jobConf = new JobConf()

    // Disabled by user
    SparkS3Util.sparkConf.set("spark.s3.bulk.listing.enabled", "false")
    FileInputFormat.setInputPaths(jobConf, "s3://bucket/dir/file")
    assert(SparkS3Util.s3BulkListingEnabled(jobConf) == false)

    // Input paths contain wildcards
    SparkS3Util.sparkConf.set("spark.s3.bulk.listing.enabled", "true")
    FileInputFormat.setInputPaths(jobConf, "s3://bucket/dir/*")
    assert(SparkS3Util.s3BulkListingEnabled(jobConf) == false)

    // Input paths copntain non-S3 files
    SparkS3Util.sparkConf.set("spark.s3.bulk.listing.enabled", "true")
    FileInputFormat.setInputPaths(jobConf, "file://dir/file")
    assert(SparkS3Util.s3BulkListingEnabled(jobConf) == false)
  }

  test("isSplitable function") {
    val jobConf: JobConf = new JobConf()

    // Splitable files
    val parquetFile: Path = new Path("file.parquet")
    assert(SparkS3Util.isSplitable(jobConf, parquetFile) == true)
    val textFile: Path = new Path("file.txt")
    assert(SparkS3Util.isSplitable(jobConf, textFile) == true)

    // Non-splitable files
    val gzipFile: Path = new Path("file.gz")
    assert(SparkS3Util.isSplitable(jobConf, gzipFile) == false)
  }

  test("computeSplits function") {
    val jobConf: JobConf = new JobConf()
    // Set S3 block size to 64mb
    jobConf.set("fs.s3.block.size", "67108864")

    val files: ArrayBuffer[FileStatus] = ArrayBuffer()
    for (i <- 1 to 10) {
      val status: FileStatus = new FileStatus(
        256 * 1024 * 1024,
        false,
        1,
        64 * 1024 * 1024,
        new Date().getTime,
        new Path(s"s3://bucket/dir/file${i}"))
      files += status
    }

    val splits: Array[InputSplit] = SparkS3Util.computeSplits(jobConf, files.toArray, 1)

    // 40 splits are expected: 256mb * 10 files / 64mb
    assert(splits.length == 40)

    // Zero-length array is expected for data local hosts
    for (s <- splits) {
      assert(s.getLocations.length == 0)
    }
  }
}
