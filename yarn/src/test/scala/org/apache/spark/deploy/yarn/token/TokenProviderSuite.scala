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

package org.apache.spark.deploy.yarn.token

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.scalatest.Matchers

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.token.ConfigurableTokenManager._

class TokenProviderSuite extends SparkFunSuite with Matchers {
  private var sparkConf: SparkConf = null

  override def beforeAll(): Unit = {
    sparkConf = new SparkConf()
    configurableTokenManager(sparkConf)
  }

  override def afterAll(): Unit = {
    configurableTokenManager(sparkConf).stop()
  }

  test("Obtain tokens For HiveMetastore") {
    val hadoopConf = new Configuration()
    hadoopConf.set("hive.metastore.kerberos.principal", "bob")
    // thrift picks up on port 0 and bails out, without trying to talk to endpoint
    hadoopConf.set("hive.metastore.uris", "http://localhost:0")
    val hiveTokenProvider = configurableTokenManager(sparkConf).getServiceTokenProvider("hive")
    val credentials = new Credentials()
    val tokens =
      hiveTokenProvider.map(_.obtainTokensFromService(sparkConf, hadoopConf, credentials)).get
    tokens.isEmpty should be (true)

  }

  test("Obtain tokens For HBase") {
    val hadoopConf = new Configuration()
    hadoopConf.set("hbase.security.authentication", "kerberos")
    val hbaseTokenProvider = configurableTokenManager(sparkConf).getServiceTokenProvider("hbase")
    val creds = new Credentials()
    val tokens = hbaseTokenProvider.map(_.obtainTokensFromService(sparkConf, hadoopConf, creds)).get
    tokens.isEmpty should be (true)

  }
}
