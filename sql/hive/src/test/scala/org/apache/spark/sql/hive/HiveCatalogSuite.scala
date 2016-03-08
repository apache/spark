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

package org.apache.spark.sql.hive

import org.apache.hadoop.util.VersionInfo

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.client.{HiveClient, IsolatedClientLoader}
import org.apache.spark.util.Utils


/**
 * Test suite for the [[HiveCatalog]].
 */
class HiveCatalogSuite extends CatalogTestCases {

  private val client: HiveClient = {
    IsolatedClientLoader.forVersion(
      hiveMetastoreVersion = HiveContext.hiveExecutionVersion,
      hadoopVersion = VersionInfo.getVersion).createClient()
  }

  protected override val tableInputFormat: String =
    "org.apache.hadoop.mapred.SequenceFileInputFormat"
  protected override val tableOutputFormat: String =
    "org.apache.hadoop.mapred.SequenceFileOutputFormat"

  protected override def newUriForDatabase(): String = Utils.createTempDir().getAbsolutePath

  protected override def resetState(): Unit = client.reset()

  protected override def newEmptyCatalog(): ExternalCatalog = new HiveCatalog(client)

}
