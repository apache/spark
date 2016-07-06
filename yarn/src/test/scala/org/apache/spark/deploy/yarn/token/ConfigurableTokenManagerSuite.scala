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
import org.apache.hadoop.io.Text
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.Token
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.yarn.token.ConfigurableTokenManager._

class ConfigurableTokenManagerSuite extends SparkFunSuite with Matchers with BeforeAndAfter {
  private var tokenManager: ConfigurableTokenManager = null
  private var sparkConf: SparkConf = null

  before {
    sparkConf = new SparkConf()
  }

  after {
    if (tokenManager != null) {
      tokenManager.stop()
      tokenManager = null
    }

    if (sparkConf != null) {
      sparkConf = null
    }
  }

  test("Correctly load default token providers") {
    tokenManager = configurableTokenManager(sparkConf)

    tokenManager.getServiceTokenProvider("hdfs") should not be (None)
    tokenManager.getServiceTokenProvider("hbase") should not be (None)
    tokenManager.getServiceTokenProvider("hive") should not be (None)
  }

  test("disable hive token provider") {
    sparkConf.set("spark.yarn.security.tokens.hive.enabled", "false")
    tokenManager = configurableTokenManager(sparkConf)

    tokenManager.getServiceTokenProvider("hdfs") should not be (None)
    tokenManager.getServiceTokenProvider("hbase") should not be (None)
    tokenManager.getServiceTokenProvider("hive") should be (None)
  }

  test("plug in customized token provider") {
    sparkConf.set("spark.yarn.security.tokens.test.enabled", "true")
    sparkConf.set(
      "spark.yarn.security.tokens.test.class", classOf[TestTokenProvider].getCanonicalName)
    tokenManager = configurableTokenManager(sparkConf)

    tokenManager.getServiceTokenProvider("test") should not be (None)
  }

  test("verify obtaining tokens from provider") {
    sparkConf.set("spark.yarn.security.tokens.test.enabled", "true")
    sparkConf.set(
      "spark.yarn.security.tokens.test.class", classOf[TestTokenProvider].getCanonicalName)
    tokenManager = configurableTokenManager(sparkConf)
    val hadoopConf = new Configuration()
    val creds = new Credentials()

    // Tokens can only be obtained from TestTokenProvider, for hdfs, hbase and hive tokens cannot
    // be obtained.
    val tokens = tokenManager.obtainTokens(hadoopConf, creds)
    tokens.length should be (1)
    tokens.head.getService should be (new Text())
  }

  test("verify getting token renewal info") {
    sparkConf.set("spark.yarn.security.tokens.test.enabled", "true")
    sparkConf.set(
      "spark.yarn.security.tokens.test.class", classOf[TestTokenProvider].getCanonicalName)
    tokenManager = configurableTokenManager(sparkConf)
    val hadoopConf = new Configuration()
    val creds = new Credentials()

    val testTokenProvider =
      tokenManager.getServiceTokenProvider("test").get.asInstanceOf[TestTokenProvider]

    // Only TestTokenProvider can get the token renewal interval and time to next renewal interval.
    val tokenRenewalInterval = tokenManager.getSmallestTokenRenewalInterval(hadoopConf)
    tokenRenewalInterval should be (testTokenProvider.tokenRenewalInterval)

    val timeToNextRenewalInterval = tokenManager.getNearestTimeFromNowToRenewal(
      hadoopConf, 0.8, creds)
    timeToNextRenewalInterval should be (testTokenProvider.timeToNextTokenRenewal)
  }

  test("Obtain tokens For HiveMetastore") {
    tokenManager = configurableTokenManager(sparkConf)

    val hadoopConf = new Configuration()
    hadoopConf.set("hive.metastore.kerberos.principal", "bob")
    // thrift picks up on port 0 and bails out, without trying to talk to endpoint
    hadoopConf.set("hive.metastore.uris", "http://localhost:0")
    val hiveTokenProvider = tokenManager.getServiceTokenProvider("hive")
    val credentials = new Credentials()
    val tokens =
      hiveTokenProvider.map(_.obtainTokensFromService(sparkConf, hadoopConf, credentials)).get
    tokens.isEmpty should be (true)
  }

  test("Obtain tokens For HBase") {
    tokenManager = configurableTokenManager(sparkConf)

    val hadoopConf = new Configuration()
    hadoopConf.set("hbase.security.authentication", "kerberos")
    val hbaseTokenProvider = tokenManager.getServiceTokenProvider("hbase")
    val creds = new Credentials()
    val tokens = hbaseTokenProvider.map(_.obtainTokensFromService(sparkConf, hadoopConf, creds)).get
    tokens.isEmpty should be (true)
  }
}

class TestTokenProvider extends ServiceTokenProvider with ServiceTokenRenewable {
  val tokenRenewalInterval = 86400 * 1000L
  var timeToNextTokenRenewal = 0L

  override def serviceName: String = "test"

  override def isTokenRequired(conf: Configuration): Boolean = true

  override def obtainTokensFromService(
      sparkConf: SparkConf,
      serviceConf: Configuration,
      creds: Credentials): Array[Token[_]] = {
    val emptyToken = new Token()
    creds.addToken(emptyToken.getService, emptyToken)

    Array(emptyToken)
  }

  override def getTokenRenewalInterval(sparkConf: SparkConf, serviceConf: Configuration): Long = {
    tokenRenewalInterval
  }

  override def getTimeFromNowToRenewal(
      sparkConf: SparkConf,
      fractional: Double,
      credentials: Credentials): Long = {
    val currTime = System.currentTimeMillis()
    val nextRenewalTime = (currTime - currTime % tokenRenewalInterval) + tokenRenewalInterval

    timeToNextTokenRenewal = ((nextRenewalTime - currTime) * fractional).toLong
    timeToNextTokenRenewal
  }
}
