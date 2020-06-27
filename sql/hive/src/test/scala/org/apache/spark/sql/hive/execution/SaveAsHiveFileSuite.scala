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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class SaveAsHiveFileSuite extends QueryTest with TestHiveSingleton {
  test("sessionScratchDir = '/tmp/hive/user_a/session_b' & scratchDir = '/tmp/hive_scratch'") {
    val insertIntoHiveTable = InsertIntoHiveTable(null, Map.empty, null, true, false, null)
    val hadoopConf = new Configuration()
    val scratchDir = "/tmp/hive_scratch"
    val sessionScratchDir = "/tmp/hive/user_a/session_b"

    val resultFromSessionDir = insertIntoHiveTable.getNonBlobTmpPath(
      hadoopConf, sessionScratchDir, scratchDir).toString

    // The result should start with 'file:$sessionScratchDir/' & end with '/-mr-10000'.
    // file:/tmp/hive/user_a/session_b/hive_2017-10-07_03-18-53_479_412884638516200146-1/-mr-10000.
    assert(resultFromSessionDir.startsWith(s"file:$sessionScratchDir/"))
    assert(resultFromSessionDir.endsWith("/-mr-10000"))

    // The result should NOT start with 'file:$scratchDir/'.
    assert(!resultFromSessionDir.startsWith(s"file:$scratchDir/"))
  }

  test("sessionScratchDir = empty & scratchDir = '/tmp/hive_scratch'") {
    val insertIntoHiveTable = InsertIntoHiveTable(null, Map.empty, null, true, false, null)
    val hadoopConf = new Configuration()
    val scratchDir = "/tmp/hive_scratch"
    val emptySessionScratchDir = ""

    val resultFromScratchDir = insertIntoHiveTable.getNonBlobTmpPath(
      hadoopConf, emptySessionScratchDir, scratchDir).toString

    // The result should start with 'file:$scratchDir/' & end with '/-mr-10000'.
    // e.g) file:/tmp/hive/hive_2017-10-07_03-18-53_479_412884638516200146-1/-mr-10000
    assert(resultFromScratchDir.startsWith(s"file:$scratchDir/"))
    assert(resultFromScratchDir.endsWith("/-mr-10000"))

    // The result should NOT start with '-mr-10000' nor equal to '-mr-10000'.
    assert(!resultFromScratchDir.startsWith("-mr-10000"))
    assert(!resultFromScratchDir.equals("-mr-10000"))
  }

  test("default config & hdfs path") {
    val insertIntoHiveTable = InsertIntoHiveTable(null, Map.empty, null, true, false, null)

    val hadoopConf = new Configuration()
    val stagingDir = ".hive-staging"

    val localPath = new Path("/tmp/test/", "path")
    val localStagingTmpPath = insertIntoHiveTable.getExternalTmpPath(
      spark, hadoopConf, localPath).toString

    // The path should start with 'file:$localPath/$stagingDir' & end with '/-ext-10000'.
    assert(localStagingTmpPath.startsWith(s"file:$localPath/$stagingDir"))
    assert(localStagingTmpPath.endsWith("/-ext-10000"))
  }

  test("'spark.sql.hive.supportedSchemesToUseNonBlobstore=false' & s3 path") {
    val insertIntoHiveTable = InsertIntoHiveTable(null, Map.empty, null, true, false, null)

    val hadoopConf = new Configuration()
    val scratchDir = "/tmp/hive_scratch"

    val s3Path = new Path("s3a://bucket/", "path")
    spark.conf.set(SQLConf.HIVE_SUPPORTED_SCHEMES_TO_USE_NONBLOBSTORE.key, "s3a, s3, s3n")
    val localMRTmpPathForS3 = insertIntoHiveTable.getExternalTmpPath(
      spark, hadoopConf, s3Path).toString

    // The path should start with 'file:/' & end with '/-mr-10000'.
    assert(localMRTmpPathForS3.startsWith(s"file:/"))
    assert(localMRTmpPathForS3.endsWith("/-mr-10000"))

    // It should NOT start with s3.
    assert(!localMRTmpPathForS3.startsWith("s3"))
    assert(!localMRTmpPathForS3.startsWith(s"file:$scratchDir"))
  }
}
