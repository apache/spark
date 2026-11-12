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

package org.apache.spark.sql.kafka010

import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.ExecutionException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclOperation, AclPermissionType}
import org.apache.kafka.common.errors.DelegationTokenAuthorizationException
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT
import org.apache.kafka.common.security.token.delegation.TokenInformation

import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.config.{KEYTAB, PRINCIPAL}
import org.apache.spark.kafka010.{KafkaTokenSparkConf, KafkaTokenUtil}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.test.SharedSparkSession

class KafkaDelegationTokenSuite extends StreamTest with SharedSparkSession with KafkaTest {

  import testImplicits._

  private val clusterIdentifier = "cluster1"

  // The client principal is allowed to obtain tokens for `proxyUser` but not for
  // `deniedProxyUser`.
  private val proxyUser = "proxyUser"
  private val deniedProxyUser = "deniedProxyUser"

  protected var testUtils: KafkaTestUtils = _

  protected override def sparkConf = super.sparkConf
    .set("spark.security.credentials.hadoopfs.enabled", "false")
    .set("spark.security.credentials.hbase.enabled", "false")
    .set(KEYTAB, testUtils.clientKeytab)
    .set(PRINCIPAL, testUtils.clientPrincipal)
    .set(s"spark.kafka.clusters.$clusterIdentifier.auth.bootstrap.servers", testUtils.brokerAddress)
    .set(s"spark.kafka.clusters.$clusterIdentifier.security.protocol", SASL_PLAINTEXT.name)

  override def beforeAll(): Unit = {
    testUtils = new KafkaTestUtils(Map.empty, true)
    try {
      testUtils.setup(
        testUtils.allowAllAcls(testUtils.kafkaPrincipal(proxyUser).toString) :+
          createTokensAcl(proxyUser))
    } catch {
      case e: Throwable =>
        // ScalaTest skips afterAll when beforeAll throws, so tear down here to avoid leaking
        // the KDC, broker, and the global JAAS system property into later suites.
        try {
          testUtils.teardown()
        } finally {
          testUtils = null
        }
        throw e
    }
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (testUtils != null) {
        testUtils.teardown()
        testUtils = null
      }
      UserGroupInformation.reset()
    } finally {
      super.afterAll()
    }
  }

  /**
   * Allow the client principal to obtain tokens owned by `owner`. The broker looks the ACL up
   * by the `KafkaPrincipal.toString` of the owner, hence the `User:` prefixed resource name.
   */
  private def createTokensAcl(owner: String): AclBinding = new AclBinding(
    new ResourcePattern(
      ResourceType.USER, testUtils.kafkaPrincipal(owner).toString, PatternType.LITERAL),
    new AccessControlEntry(
      testUtils.clientKafkaPrincipal, "*", AclOperation.CREATE_TOKENS, AclPermissionType.ALLOW))

  private def proxyUgi(user: String): UserGroupInformation =
    UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser())

  private def obtainDelegationTokens(ugi: UserGroupInformation): Credentials = {
    val credentials = new Credentials()
    ugi.doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val manager = new HadoopDelegationTokenManager(
          spark.sparkContext.conf, new Configuration(), null)
        manager.obtainDelegationTokens(credentials)
      }
    })
    credentials
  }

  /** Ask the broker what it recorded for the token which was just obtained. */
  private def describeToken(credentials: Credentials): TokenInformation = {
    val token = credentials.getToken(KafkaTokenUtil.getTokenService(clusterIdentifier))
    assert(token != null, s"No delegation token was obtained for cluster $clusterIdentifier")
    val tokenId = new String(token.getIdentifier)
    testUtils.describeDelegationTokens().find(_.tokenId() == tokenId)
      .getOrElse(fail(s"Token $tokenId is unknown to the broker"))
  }

  private def distributeTokens(credentials: Credentials): Unit = {
    val serializedCredentials = SparkHadoopUtil.get.serialize(credentials)
    SparkHadoopUtil.get.addDelegationTokens(serializedCredentials, spark.sparkContext.conf)
  }

  private def createTopic(): String = {
    val topic = "topic-" + UUID.randomUUID().toString
    testUtils.createTopic(topic, partitions = 5)
    topic
  }

  /** Write to and read back from `topic`, authenticating with the distributed token. */
  private def roundtrip(topic: String): Unit = {
    withTempDir { checkpointDir =>
      val input = MemoryStream[String]

      val df = input.toDF()
      val writer = df.writeStream
        .outputMode(OutputMode.Append)
        .format("kafka")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("kafka.bootstrap.servers", testUtils.brokerAddress)
        .option("topic", topic)
        .start()

      try {
        input.addData("1", "2", "3", "4", "5")
        failAfter(streamingTimeout) {
          writer.processAllAvailable()
        }
      } finally {
        writer.stop()
      }
    }

    val streamingDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingOffsets", s"earliest")
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map(kv => kv._2.toInt + 1)

    testStream(streamingDf)(
      StartStream(),
      AssertOnQuery { q =>
        q.processAllAvailable()
        true
      },
      CheckAnswer(2, 3, 4, 5, 6),
      StopStream
    )
  }

  testRetry("Roundtrip", 3) {
    val credentials = obtainDelegationTokens(UserGroupInformation.getCurrentUser())

    // Without impersonation no owner is sent, so the broker assigns ownership to the requester.
    val tokenInfo = describeToken(credentials)
    assert(tokenInfo.owner().toString === testUtils.clientKafkaPrincipal)
    assert(tokenInfo.tokenRequester().toString === testUtils.clientKafkaPrincipal)

    distributeTokens(credentials)
    roundtrip(createTopic())
  }

  testRetry("SPARK-28173: Roundtrip with proxy user", 3) {
    // The manager preserves the proxy UGI here despite KEYTAB/PRINCIPAL being set only because
    // Hadoop security is SIMPLE in this JVM (a keytab re-login is then a no-op). The production
    // proxy flow (ticket cache or direct providers, no principal) preserves it by design.
    val credentials = obtainDelegationTokens(proxyUgi(proxyUser))

    // The token is requested with the client's credentials but owned by the proxy user, so
    // connectors authenticate to Kafka as the proxy user.
    val tokenInfo = describeToken(credentials)
    assert(tokenInfo.owner() === testUtils.kafkaPrincipal(proxyUser))
    assert(tokenInfo.tokenRequester().toString === testUtils.clientKafkaPrincipal)

    distributeTokens(credentials)
    val topic = createTopic()
    // Deny the client principal on the topic so the roundtrip only succeeds when the connectors
    // authenticate as the proxy user (deny overrides the wildcard allow).
    testUtils.createAcls(Seq(AclOperation.READ, AclOperation.WRITE).map { op =>
      new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(testUtils.clientKafkaPrincipal, "*", op, AclPermissionType.DENY))
    })
    roundtrip(topic)
  }

  test("SPARK-28173: Obtaining token for proxy user without CreateTokens permission fails") {
    val conf = spark.sparkContext.conf
    val clusterConf = KafkaTokenSparkConf.getClusterConfig(conf, clusterIdentifier)
    proxyUgi(deniedProxyUser).doAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        val e = intercept[SparkException] {
          KafkaTokenUtil.obtainToken(conf, clusterConf)
        }
        assert(e.getMessage.contains("CreateTokens"))
        assert(e.getCause.asInstanceOf[ExecutionException]
          .getCause.isInstanceOf[DelegationTokenAuthorizationException])
      }
    })
  }
}
