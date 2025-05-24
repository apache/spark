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

package org.apache.spark

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.util.Utils

class SparkContextSuite extends SparkFunSuite with BeforeAndAfterEach {
  @transient var sc: SparkContext = _

  override def afterEach(): Unit = {
    try {
      if (sc != null) {
        sc.stop()
      }
    } finally {
      super.afterEach()
    }
  }

  test("SPARK-35383: Fill missing S3A magic committer configs if needed") {
    Seq(
      "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
      "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
    ).foreach { className =>
      assert(Utils.classIsLoadable(className))
    }

    val c1 = new SparkConf().setAppName("s3a-test").setMaster("local")
    sc = new SparkContext(c1)
    assert(!sc.getConf.contains("spark.hadoop.fs.s3a.committer.name"))
    sc.stop()

    val c2 = c1.clone.set("spark.hadoop.fs.s3a.bucket.mybucket.committer.magic.enabled", "false")
    sc = new SparkContext(c2)
    assert(!sc.getConf.contains("spark.hadoop.fs.s3a.committer.name"))
    sc.stop()

    val c3 = c1.clone.set("spark.hadoop.fs.s3a.bucket.mybucket.committer.magic.enabled", "true")
    sc = new SparkContext(c3)
    Seq(
      "spark.hadoop.fs.s3a.committer.magic.enabled" -> "true",
      "spark.hadoop.fs.s3a.committer.name" -> "magic",
      "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" ->
        "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
      "spark.sql.parquet.output.committer.class" ->
        "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
      "spark.sql.sources.commitProtocolClass" ->
        "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"
    ).foreach { case (k, v) =>
      assert(v == sc.getConf.get(k))
    }
    sc.stop()

    // Respect a user configuration
    val c4 = c1.clone
      .set("spark.hadoop.fs.s3a.committer.magic.enabled", "false")
      .set("spark.hadoop.fs.s3a.bucket.mybucket.committer.magic.enabled", "true")
    sc = new SparkContext(c4)
    Seq(
      "spark.hadoop.fs.s3a.committer.magic.enabled" -> "false",
      "spark.hadoop.fs.s3a.committer.name" -> null,
      "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a" -> null,
      "spark.sql.parquet.output.committer.class" -> null,
      "spark.sql.sources.commitProtocolClass" -> null
    ).foreach { case (k, v) =>
      if (v == null) {
        assert(!sc.getConf.contains(k))
      } else {
        assert(v == sc.getConf.get(k))
      }
    }
    sc.stop()
  }
}
