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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton

class BlobStorageUtilsSuite extends QueryTest with TestHiveSingleton with GivenWhenThen{
  test("s3,s3a,s3n are BlobStorage path by default config") {
    val conf = new Configuration()
    assert(BlobStorageUtils.isBlobStoragePath(conf, new Path("s3://test/path")))
    assert(BlobStorageUtils.isBlobStoragePath(conf, new Path("s3n://test/path")))
    assert(BlobStorageUtils.isBlobStoragePath(conf, new Path("s3a://test/path")))
    assert(BlobStorageUtils.isBlobStoragePath(conf, new Path("S3A://test/path")))
    assert(BlobStorageUtils.isBlobStoragePath(conf, new Path("S3n://test/path")))
  }

  test("file,hdfs are NOT BlobStorage path by default config") {
    val conf = new Configuration()
    assert(!BlobStorageUtils.isBlobStoragePath(conf, new Path("hdfs:///test/path")))
    assert(!BlobStorageUtils.isBlobStoragePath(conf, new Path("file:///test/path")))
    assert(!BlobStorageUtils.isBlobStoragePath(conf, new Path("/test/new_path")))
  }

  test("s3,s3n,s3a are BlobStorage scheme by default config") {
    val conf = new Configuration()
    assert(BlobStorageUtils.isBlobStorageScheme(conf, "s3"))
    assert(BlobStorageUtils.isBlobStorageScheme(conf, "s3a"))
    assert(BlobStorageUtils.isBlobStorageScheme(conf, "s3n"))
    assert(BlobStorageUtils.isBlobStorageScheme(conf, "S3N"))
    assert(BlobStorageUtils.isBlobStorageScheme(conf, "S3a"))
  }

  test("custom BlobStorage scheme") {
    val customConf = new Configuration()
    customConf.set("hive.blobstore.supported.schemes", "s3bfs,s3e")
    assert(BlobStorageUtils.isBlobStorageScheme(customConf, "s3bfs"))
    assert(BlobStorageUtils.isBlobStorageScheme(customConf, "s3e"))
    assert(!BlobStorageUtils.isBlobStorageScheme(customConf, "s3"))
    assert(!BlobStorageUtils.isBlobStorageScheme(customConf, "s3a"))
  }

  test("file,HDFS,swift,s3bfs are NOT BlobStorage scheme by default config") {
    val conf = new Configuration()
    assert(!BlobStorageUtils.isBlobStorageScheme(conf, "file"))
    assert(!BlobStorageUtils.isBlobStorageScheme(conf, ""))
    assert(!BlobStorageUtils.isBlobStorageScheme(conf, "HDFS"))
    assert(!BlobStorageUtils.isBlobStorageScheme(conf, "s3bfs"))
    assert(!BlobStorageUtils.isBlobStorageScheme(conf, "swift"))
    assert(!BlobStorageUtils.isBlobStorageScheme(conf, "s3bfs"))
  }

  test("BlobStorageUtils.useBlobStorageAsScratchDir") {
    Given("default 'hive.blobstore.use.blobstore.as.scratchdir' config")
    val defaultConf = new Configuration()

    When("evaluate BlobStorageUtils.useBlobStorageAsScratchDir(defaultConf)")
    val useBlobStorageByDefault = BlobStorageUtils.useBlobStorageAsScratchDir(defaultConf)

    Then("the result should be true")
    assert(useBlobStorageByDefault)

    And("In the case of")
    Given("'hive.blobstore.use.blobstore.as.scratchdir=false'")
    val conf = new Configuration()
    conf.set("hive.blobstore.use.blobstore.as.scratchdir", "false")

    When("evaluate BlobStorageUtils.useBlobStorageAsScratchDir(conf)")
    val useBlobStorage = BlobStorageUtils.useBlobStorageAsScratchDir(conf)

    Then("the result should NOT use BlogStorage as scratch")
    assert(!useBlobStorage)
  }
}
