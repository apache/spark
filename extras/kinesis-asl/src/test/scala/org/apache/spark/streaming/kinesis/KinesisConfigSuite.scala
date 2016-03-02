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

import scala.language.postfixOps


import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}

class KinesisConfigSuite extends KinesisFunSuite {

  private val workerId = "dummyWorkerId"
  private val kinesisAppName = "testApp"
  private val kinesisStreamName = "testStream"
  private val regionName = "us-east-1"
  private val endpointUrl = "https://testendpoint.local"
  private val streamPosition = InitialPositionInStream.TRIM_HORIZON

  private val awsAccessKey = "accessKey"
  private val awsSecretKey = "secretKey"
  private val awsCreds = new SerializableAWSCredentials(awsAccessKey, awsSecretKey)



  test("builds a KinesisClientLibConfiguration with defaults set") {
    val kinesisConfig = new KinesisConfig(kinesisAppName, kinesisStreamName, endpointUrl, regionName, streamPosition)
    val kclConfig = kinesisConfig.buildKCLConfig(workerId)
    assert(kclConfig.getApplicationName() == kinesisAppName)
    assert(kclConfig.getStreamName() == kinesisStreamName)
    assert(kclConfig.getInitialPositionInStream() == streamPosition)
    assert(kclConfig.getApplicationName() == kinesisAppName)
    assert(kclConfig.getKinesisEndpoint() == endpointUrl)
  }

  test("returns given creds if creds are specified") {
    val kinesisConfig = new KinesisConfig(kinesisAppName, kinesisStreamName, regionName, endpointUrl, streamPosition, Some(awsCreds))
    assert(kinesisConfig.awsCredentials == awsCreds)
  }

}
