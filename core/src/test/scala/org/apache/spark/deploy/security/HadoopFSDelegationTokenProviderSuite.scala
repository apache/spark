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

package org.apache.spark.deploy.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.{STAGING_DIR, SUBMIT_DEPLOY_MODE}

class HadoopFSDelegationTokenProviderSuite extends SparkFunSuite with Matchers {
  test("hadoopFSsToAccess should return defaultFS even if not configured") {
    val sparkConf = new SparkConf()
    val defaultFS = "hdfs://localhost:8020"
    val statingDir = "hdfs://localhost:8021"
    sparkConf.setMaster("yarn")
    sparkConf.set(SUBMIT_DEPLOY_MODE, "client")
    sparkConf.set(STAGING_DIR, statingDir)
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", defaultFS)
    val expected = Set(
      new Path(defaultFS).getFileSystem(hadoopConf),
      new Path(statingDir).getFileSystem(hadoopConf)
    )
    val result = HadoopFSDelegationTokenProvider.hadoopFSsToAccess(sparkConf, hadoopConf)
    result should be (expected)
  }
}
