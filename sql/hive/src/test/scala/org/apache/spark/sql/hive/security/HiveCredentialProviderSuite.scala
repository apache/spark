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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.deploy.security.ConfigurableCredentialManager
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.{BeforeAndAfter, Matchers}

class HiveCredentialProviderSuite extends SparkFunSuite with Matchers with BeforeAndAfter {
  private var sparkConf: SparkConf = null
  private var hadoopConf: Configuration = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    sparkConf = new SparkConf()
    hadoopConf = new Configuration()

    System.setProperty("SPARK_YARN_MODE", "true")
  }

  override def afterAll(): Unit = {
    System.clearProperty("SPARK_YARN_MODE")

    super.afterAll()
  }

  test("Correctly Hive credential provider with other default credential providers") {
    val credentialManager = new ConfigurableCredentialManager(sparkConf, hadoopConf)
    credentialManager.getServiceCredentialProvider("hadoopfs") should not be (None)
    credentialManager.getServiceCredentialProvider("hbase") should not be (None)
    credentialManager.getServiceCredentialProvider("hive") should not be (None)
  }

  test("obtain tokens For HiveMetastore") {
    val hadoopConf = new Configuration()
    hadoopConf.set("hive.metastore.kerberos.principal", "bob")
    // thrift picks up on port 0 and bails out, without trying to talk to endpoint
    hadoopConf.set("hive.metastore.uris", "http://localhost:0")

    val hiveCredentialProvider = new HiveCredentialProvider()
    val credentials = new Credentials()
    hiveCredentialProvider.obtainCredentials(hadoopConf, sparkConf, credentials)

    credentials.getAllTokens.size() should be (0)
  }
}
