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

import java.{util => ju}

import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.spark.sql.test.SharedSQLContext

class CanonicalizeKafkaParamsSuite extends SharedSQLContext {

  test("Same unique id is returned for same set of kafka parameters") {
    CanonicalizeKafkaParams.clear()
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("acks", "0")
    // Here only host should be resolvable, it does not need a running instance of kafka server.
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    val canonicalKp = CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams)
    val kafkaParams2 = new ju.HashMap[String, Object](kafkaParams)
    val canonicalKp2 = CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams2)
    val uid1 = canonicalKp.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId).toString
    val uid2 = canonicalKp2.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId).toString
    assert(uid1 == uid2)
  }

  test("New unique id is generated for any modification in kafka parameters.") {
    CanonicalizeKafkaParams.clear()
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("acks", "0")
    // Here only host should be resolvable, it does not need a running instance of kafka server.
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    val canonicalKp = CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams)
    val kafkaParams2 = new ju.HashMap[String, Object](kafkaParams)
    kafkaParams2.put("acks", "1")
    val canonicalKp2 = CanonicalizeKafkaParams.computeUniqueCanonicalForm(kafkaParams2)
    val uid1 = canonicalKp.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId).toString
    val uid2 = canonicalKp2.get(CanonicalizeKafkaParams.sparkKafkaParamsUniqueId).toString
    assert(uid1 != uid2)
  }
}
