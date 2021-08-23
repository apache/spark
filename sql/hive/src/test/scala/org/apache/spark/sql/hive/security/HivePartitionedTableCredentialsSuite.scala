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

package org.apache.spark.sql.hive.security

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

object HivePartitionedTableCredentialsSuite extends QueryTest
  with SQLTestUtils with TestHiveSingleton {

  private val credentialsTempDirectory = Utils.createTempDir()

  test("SPARK-36328: Reuse the FileSystem delegation token" +
    " while querying partitioned hive table.") {
    // The suite is based on the repro provided in SPARK-36328
    val hadoopKeytabFile = writeCredentials("hadoop.keytab", "hadoop-keytab")
    // scalastyle:off hadoopconfiguration
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authorization", "true")
    spark.sparkContext.hadoopConfiguration.set("hadoop.security.authentication", "kerberos")
    spark.sparkContext.hadoopConfiguration.set("dfs.block.access.token.enable", "true")
    spark.sparkContext.hadoopConfiguration.set(
      "dfs.namenode.keytab.file", hadoopKeytabFile.getCanonicalPath)
    spark.sparkContext.hadoopConfiguration.set(
      "dfs.namenode.kerberos.principal", "HTTP/localhost@LOCALHOST")
    // scalastyle:on hadoopconfiguration
    // Initialize a buffer to store Credentials during querying partitioned table
    val credentialsBuf = ListBuffer[Credentials]()
    withTable("parttable") {
      // Create partitioned table
      sql("create table parttable (key char(1), value int) partitioned by (p int);")
      sql("insert into table parttable partition(p=100) values ('d', 1), ('e', 2), ('f', 3);")
      sql("insert into table parttable partition(p=200) values ('d', 1), ('e', 2), ('f', 3);")
      sql("insert into table parttable partition(p=300) values ('d', 1), ('e', 2), ('f', 3);")
      // Execute query
      checkAnswer(sql("select value, count(*) from parttable group by value;"),
        Seq[Row](Row(1, 3), Row(2, 3), Row(3, 3)))
      // Get stage data from AppStatusStore and then generate HadoopRDD jobConf cache keys
      val jobConfCacheKeys = spark.sparkContext.statusStore.stageList(null)
        .flatMap(stageData => stageData.rddIds.map("rdd_%d_job_conf".format(_)))
      // Get jobConf from SparkEnv
      jobConfCacheKeys.foreach(jobConfCacheKey => {
        val credentials = Option(HadoopRDD.getCachedMetadata(jobConfCacheKey))
          .map(_.asInstanceOf[JobConf])
          .map(_.getCredentials).orNull
        if (credentials != null) credentialsBuf += credentials
      })
    }
    // Check
    assert(UserGroupInformation.isSecurityEnabled)
    // To avoid hadoop 2 building failed with SBT, use getAllTokens instead of getTokenMap.
    val allTokensBuf = credentialsBuf.map(_.getAllTokens.asScala.toSet)
    // The tokens used during operation should be the same.
    assert(allTokensBuf.head.size == 1)
    allTokensBuf.foreach(token => assert(token == allTokensBuf.head))
  }

  private def writeCredentials(credentialsFileName: String, credentialsContents: String): File = {
    val credentialsFile = new File(credentialsTempDirectory, credentialsFileName)
    Files.write(credentialsContents, credentialsFile, Charsets.UTF_8)
    credentialsFile
  }
}
