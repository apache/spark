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
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.test.TestHiveSingleton

class SaveAsHiveFileSuite extends QueryTest with TestHiveSingleton
    with MockitoSugar with GivenWhenThen {
  test("getMRTmpPath method") {
    val insertIntoHiveTable = new InsertIntoHiveTable(
      mock[CatalogTable], Map.empty[String, Option[String]],
      mock[LogicalPlan], true, false, Seq.empty[String])
    val hadoopConf = new Configuration()
    val scratchDir = "/tmp/hive_scratch"
    val sessionScratchDir = "/tmp/hive/user_a/session_b"

    Given(s"sessionScratchDir = '$sessionScratchDir' & scratchDir = '$scratchDir'")
    When("get the path from getMRTmpPath(hadoopConf, sessionScratchDir, scratchDir)")
    val resultFromSessionDir = insertIntoHiveTable.getMRTmpPath(
      hadoopConf, sessionScratchDir, scratchDir).toString

    Then(s"the result should start with 'file:$sessionScratchDir/' & end with '/-mr-10000'")
    // file:/tmp/hive/user_a/session_b/hive_2017-10-07_03-18-53_479_412884638516200146-1/-mr-10000
    assert(resultFromSessionDir.startsWith(s"file:$sessionScratchDir/"))
    assert(resultFromSessionDir.endsWith("/-mr-10000"))

    And(s"the result should NOT start with 'file:$scratchDir/'")
    assert(!resultFromSessionDir.startsWith(s"file:$scratchDir/"))

    info("However in the case of")
    Given(s"sessionScratchDir is empty string & scratchDir = '$scratchDir'")
    val emptySessionScratchDir = ""

    When("get the path from getMRTmpPath(hadoopConf, '')")
    val resultFromScratchDir = insertIntoHiveTable.getMRTmpPath(
      hadoopConf, emptySessionScratchDir, scratchDir).toString

    Then(s"the result should start with 'file:$scratchDir/' & end with '/-mr-10000'")
    // e.g) file:/tmp/hive/hive_2017-10-07_03-18-53_479_412884638516200146-1/-mr-10000
    assert(resultFromScratchDir.startsWith(s"file:$scratchDir/"))
    assert(resultFromScratchDir.endsWith("/-mr-10000"))

    And(s"the result should NOT start with '-mr-10000' nor equal to '-mr-10000'")
    assert(!resultFromScratchDir.startsWith("-mr-10000"))
    assert(!resultFromScratchDir.equals("-mr-10000"))
  }

  test("getExternalTmpPath method") {
    val insertIntoHiveTable = new InsertIntoHiveTable(
      mock[CatalogTable], Map.empty[String, Option[String]],
      mock[LogicalPlan], true, false, Seq.empty[String])

    val hadoopConf = new Configuration()
    val stagingDir = ".hive-staging"
    val scratchDir = "/tmp/hive_scratch"
    val sessionScratchDir = "/tmp/hive/user_a/session_b"

    Given("default config & hdfs path")
    val localPath = new Path("/tmp/test/", "path")

    When("getExternalTmpPath returns path")
    val localStagingTmpPath = insertIntoHiveTable.getExternalTmpPath(
      spark, hadoopConf, localPath).toString

    Then(s"the path should start with 'file:$localPath/$stagingDir' & end with '/-ext-10000'")
    assert(localStagingTmpPath.startsWith(s"file:$localPath/$stagingDir"))
    assert(localStagingTmpPath.endsWith("/-ext-10000"))

    info("However in the case of")
    Given("'hive.blobstore.use.blobstore.as.scratchdir=false' & s3 path")
    val s3Path = new Path("s3a://bucket/", "path")
    hadoopConf.set("hive.blobstore.use.blobstore.as.scratchdir", "false")

    When("getExternalTmpPath returns path")
    val localMRTmpPathForS3 = insertIntoHiveTable.getExternalTmpPath(
      spark, hadoopConf, s3Path).toString

    Then(s"the path should start with 'file:/' & end with '/-mr-10000'")
    assert(localMRTmpPathForS3.startsWith(s"file:/"))
    assert(localMRTmpPathForS3.endsWith("/-mr-10000"))

    And("it should NOT start with s3")
    assert(!localMRTmpPathForS3.startsWith("s3"))
    assert(!localMRTmpPathForS3.startsWith(s"file:$scratchDir"))
  }
}
