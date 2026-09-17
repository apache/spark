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

package org.apache.spark.kafka010

import java.{util => ju}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_SSL
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config.SECRET_REDACTION_PATTERN
import org.apache.spark.util.Utils.REDACTION_REPLACEMENT_TEXT

class KafkaRedactionUtilSuite extends SparkFunSuite with KafkaDelegationTokenTest {
  test("redactParams shouldn't throw exception when no SparkEnv available") {
    // This is effecting only tests and should never happen in production
    assert(KafkaRedactionUtil.redactParams(Seq()) === Seq())
  }

  test("redactParams should give back empty parameters") {
    setSparkEnv(Map.empty)
    assert(KafkaRedactionUtil.redactParams(Seq()) === Seq())
  }

  test("redactParams should give back null value") {
    setSparkEnv(Map.empty)
    val kafkaParams = Seq(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> null
    )

    assert(KafkaRedactionUtil.redactParams(kafkaParams) === kafkaParams)
  }

  test("redactParams should redact non String parameters") {
    setSparkEnv(
      Map(
        SECRET_REDACTION_PATTERN.key -> ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
      )
    )
    val kafkaParams = Seq(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val redactedParams = KafkaRedactionUtil.redactParams(kafkaParams).toMap

    assert(redactedParams.size === 1)
    assert(redactedParams.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).get
      === REDACTION_REPLACEMENT_TEXT)
  }

  test("redactParams should redact token password from parameters") {
    setSparkEnv(Map.empty)
    val groupId = "id-" + ju.UUID.randomUUID().toString
    addTokenToUGI(tokenService1, tokenId1, tokenPassword1)
    val clusterConf = createClusterConf(identifier1, SASL_SSL.name)
    val jaasParams = KafkaTokenUtil.getTokenJaasParams(clusterConf)
    val kafkaParams = Seq(
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      SaslConfigs.SASL_JAAS_CONFIG -> jaasParams
    )

    val redactedParams = KafkaRedactionUtil.redactParams(kafkaParams).toMap

    assert(redactedParams.size === 2)
    assert(redactedParams.get(ConsumerConfig.GROUP_ID_CONFIG).get === groupId)
    val redactedJaasParams = redactedParams.get(SaslConfigs.SASL_JAAS_CONFIG).get
    assert(redactedJaasParams.contains(tokenId1))
    assert(!redactedJaasParams.contains(tokenPassword1))
  }

  test("redactParams should redact passwords from parameters") {
    setSparkEnv(Map.empty)
    val groupId = "id-" + ju.UUID.randomUUID().toString
    val kafkaParams = Seq(
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> trustStorePassword,
      SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> keyStorePassword,
      SslConfigs.SSL_KEY_PASSWORD_CONFIG -> keyPassword
    )

    val redactedParams = KafkaRedactionUtil.redactParams(kafkaParams).toMap

    assert(redactedParams.size === 4)
    assert(redactedParams(ConsumerConfig.GROUP_ID_CONFIG) === groupId)
    assert(redactedParams(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) === REDACTION_REPLACEMENT_TEXT)
    assert(redactedParams(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) === REDACTION_REPLACEMENT_TEXT)
    assert(redactedParams(SslConfigs.SSL_KEY_PASSWORD_CONFIG) === REDACTION_REPLACEMENT_TEXT)
  }

  test("redactJaasParam should give back null") {
    assert(KafkaRedactionUtil.redactJaasParam(null) === null)
  }

  test("redactJaasParam should give back empty string") {
    assert(KafkaRedactionUtil.redactJaasParam("") === "")
  }

  test("redactJaasParam should redact token password") {
    addTokenToUGI(tokenService1, tokenId1, tokenPassword1)
    val clusterConf = createClusterConf(identifier1, SASL_SSL.name)
    val jaasParams = KafkaTokenUtil.getTokenJaasParams(clusterConf)

    val redactedJaasParams = KafkaRedactionUtil.redactJaasParam(jaasParams)

    assert(redactedJaasParams.contains(tokenId1))
    assert(!redactedJaasParams.contains(tokenPassword1))
  }

  test("redactJaasParam should redact secret options in all quoting styles, keeping context") {
    val prefix = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" "
    val oauth = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
      "clientId=\"id\" "
    // (input, secret value that must be gone, non-secret context that must survive)
    val cases = Seq(
      (prefix + "password=\"double-quoted-secret\";", "double-quoted-secret", "username=\"u\""),
      (prefix + "password='single-quoted-secret';", "single-quoted-secret", "username=\"u\""),
      (prefix + "password=unquoted-secret;", "unquoted-secret", "username=\"u\""),
      (oauth + "clientSecret=\"oauth-secret\";", "oauth-secret", "clientId=\"id\"")
    )
    cases.foreach { case (param, secret, context) =>
      val redacted = KafkaRedactionUtil.redactJaasParam(param)
      assert(!redacted.contains(secret), s"credential not redacted in output: $redacted")
      assert(redacted.contains(REDACTION_REPLACEMENT_TEXT), s"no redaction marker in: $redacted")
      assert(redacted.contains(context), s"non-secret context dropped from output: $redacted")
    }
  }

  test("redactJaasParam should not leak the tail of an escaped-quote credential value") {
    // The value contains an escaped double quote, so the whole value must be treated as one token
    // rather than ending at the embedded quote (which would leak the trailing characters).
    val param = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      "username=\"u\" password=\"ab\\\"TAIL_SECRET\";"
    val redacted = KafkaRedactionUtil.redactJaasParam(param)
    assert(!redacted.contains("TAIL_SECRET"), s"escaped-quote credential tail leaked: $redacted")
    assert(redacted.contains(REDACTION_REPLACEMENT_TEXT), s"no redaction marker in: $redacted")
    assert(redacted.contains("username=\"u\""), s"non-secret context dropped: $redacted")
  }

  test("redactJaasParam always redacts credentials even when spark.redaction.regex omits them") {
    // spark.redaction.regex replaces the default rather than extending it. A user pattern that does
    // not mention `password` must not disable the built-in credential masking (fail-open guard).
    setSparkEnv(Map(SECRET_REDACTION_PATTERN.key -> "(?i)my_custom_key"))
    val param = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      "username=\"u\" password=\"PLAINTEXT_SECRET\";"
    val redacted = KafkaRedactionUtil.redactJaasParam(param)
    assert(!redacted.contains("PLAINTEXT_SECRET"),
      s"password leaked despite custom regex: $redacted")
    assert(redacted.contains(REDACTION_REPLACEMENT_TEXT), s"no redaction marker in: $redacted")
    assert(redacted.contains("username=\"u\""), s"non-secret context dropped: $redacted")
  }

  test("redactJaasParam additionally honors a configured spark.redaction.regex") {
    // A user-configured pattern widens coverage on top of the always-redacted options: an option
    // whose name matches the configured regex is redacted too, while non-matching options survive.
    setSparkEnv(Map(SECRET_REDACTION_PATTERN.key -> "(?i)my_custom_key"))
    val param = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
      "username=\"u\" my_custom_key=\"CUSTOM_SECRET\";"
    val redacted = KafkaRedactionUtil.redactJaasParam(param)
    assert(!redacted.contains("CUSTOM_SECRET"), s"configured secret not redacted: $redacted")
    assert(redacted.contains(REDACTION_REPLACEMENT_TEXT), s"no redaction marker in: $redacted")
    assert(redacted.contains("username=\"u\""), s"non-secret context dropped: $redacted")
  }
}
