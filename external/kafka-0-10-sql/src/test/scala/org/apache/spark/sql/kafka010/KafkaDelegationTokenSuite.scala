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

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.kafka.common.security.auth.SecurityProtocol.SASL_PLAINTEXT

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.config.{KEYTAB, PRINCIPAL}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.apache.spark.sql.test.SharedSparkSession

class KafkaDelegationTokenSuite extends StreamTest with SharedSparkSession with KafkaTest {

  import testImplicits._

  protected var testUtils: KafkaTestUtils = _

  protected override def sparkConf = super.sparkConf
    .set("spark.security.credentials.hadoopfs.enabled", "false")
    .set("spark.security.credentials.hbase.enabled", "false")
    .set(KEYTAB, testUtils.clientKeytab)
    .set(PRINCIPAL, testUtils.clientPrincipal)
    .set("spark.kafka.clusters.cluster1.auth.bootstrap.servers", testUtils.brokerAddress)
    .set("spark.kafka.clusters.cluster1.security.protocol", SASL_PLAINTEXT.name)

  override def beforeAll(): Unit = {
    testUtils = new KafkaTestUtils(Map.empty, true)
    testUtils.setup()
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

  testRetry("Roundtrip", 3) {
    val hadoopConf = new Configuration()
    val manager = new HadoopDelegationTokenManager(spark.sparkContext.conf, hadoopConf, null)
    val credentials = new Credentials()
    manager.obtainDelegationTokens(credentials)
    val serializedCredentials = SparkHadoopUtil.get.serialize(credentials)
    SparkHadoopUtil.get.addDelegationTokens(serializedCredentials, spark.sparkContext.conf)

    val topic = "topic-" + UUID.randomUUID().toString
    testUtils.createTopic(topic, partitions = 5)

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
}
