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

package org.apache.spark.streaming.kinesis

import org.apache.spark.streaming.TestSuiteBase
import org.apache.spark.util.Utils

class SparkAWSCredentialsBuilderSuite extends TestSuiteBase {
  private def builder = SparkAWSCredentials.builder

  private val basicCreds = BasicCredentials(
    awsAccessKeyId = "a-very-nice-access-key",
    awsSecretKey = "a-very-nice-secret-key")

  private val stsCreds = STSCredentials(
    stsRoleArn = "a-very-nice-role-arn",
    stsSessionName = "a-very-nice-secret-key",
    stsExternalId = Option("a-very-nice-external-id"),
    longLivedCreds = basicCreds)

  test("should build DefaultCredentials when given no params") {
    assert(builder.build() == DefaultCredentials)
  }

  test("should build BasicCredentials") {
    assertResult(basicCreds) {
      builder.basicCredentials(basicCreds.awsAccessKeyId, basicCreds.awsSecretKey)
        .build()
    }
  }

  test("should build STSCredentials") {
    // No external ID, default long-lived creds
    assertResult(stsCreds.copy(stsExternalId = None, longLivedCreds = DefaultCredentials)) {
      builder.stsCredentials(stsCreds.stsRoleArn, stsCreds.stsSessionName)
        .build()
    }
    // Default long-lived creds
    assertResult(stsCreds.copy(longLivedCreds = DefaultCredentials)) {
      builder.stsCredentials(
          stsCreds.stsRoleArn,
          stsCreds.stsSessionName,
          stsCreds.stsExternalId.get)
        .build()
    }
    // No external ID, basic keypair for long-lived creds
    assertResult(stsCreds.copy(stsExternalId = None)) {
      builder.stsCredentials(stsCreds.stsRoleArn, stsCreds.stsSessionName)
        .basicCredentials(basicCreds.awsAccessKeyId, basicCreds.awsSecretKey)
        .build()
    }
    // Basic keypair for long-lived creds
    assertResult(stsCreds) {
      builder.stsCredentials(
          stsCreds.stsRoleArn,
          stsCreds.stsSessionName,
          stsCreds.stsExternalId.get)
        .basicCredentials(basicCreds.awsAccessKeyId, basicCreds.awsSecretKey)
        .build()
    }
    // Order shouldn't matter
    assertResult(stsCreds) {
      builder.basicCredentials(basicCreds.awsAccessKeyId, basicCreds.awsSecretKey)
        .stsCredentials(
          stsCreds.stsRoleArn,
          stsCreds.stsSessionName,
          stsCreds.stsExternalId.get)
        .build()
    }
  }

  test("SparkAWSCredentials classes should be serializable") {
    assertResult(basicCreds) {
      Utils.deserialize[BasicCredentials](Utils.serialize(basicCreds))
    }
    assertResult(stsCreds) {
      Utils.deserialize[STSCredentials](Utils.serialize(stsCreds))
    }
    // Will also test if DefaultCredentials can be serialized
    val stsDefaultCreds = stsCreds.copy(longLivedCreds = DefaultCredentials)
    assertResult(stsDefaultCreds) {
      Utils.deserialize[STSCredentials](Utils.serialize(stsDefaultCreds))
    }
  }
}
