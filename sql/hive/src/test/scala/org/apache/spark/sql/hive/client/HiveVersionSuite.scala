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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.conf.Configuration
import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkFunSuite

private[client] abstract class HiveVersionSuite(version: String) extends SparkFunSuite {
  override protected val enableAutoThreadAudit = false
  protected var client: HiveClient = null

  protected def buildClient(hadoopConf: Configuration): HiveClient = {
    // Hive changed the default of datanucleus.schema.autoCreateAll from true to false and
    // hive.metastore.schema.verification from false to true since 2.0
    // For details, see the JIRA HIVE-6113 and HIVE-12463
    if (version == "2.0" || version == "2.1" || version == "2.2" || version == "2.3" ||
        version == "3.0" || version == "3.1") {
      hadoopConf.set("datanucleus.schema.autoCreateAll", "true")
      hadoopConf.set("datanucleus.autoStartMechanismMode", "ignored")
      hadoopConf.set("hive.metastore.schema.verification", "false")
    }
    // Since Hive 3.0, HIVE-19310 skipped `ensureDbInit` if `hive.in.test=false`.
    if (version == "3.0" || version == "3.1") {
      hadoopConf.set("hive.in.test", "true")
      hadoopConf.set("hive.query.reexecution.enabled", "false")
    }
    HiveClientBuilder.buildClient(version, hadoopConf)
  }

  override def suiteName: String = s"${super.suiteName}($version)"

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(s"$version: $testName", testTags: _*)(testFun)
  }
}
