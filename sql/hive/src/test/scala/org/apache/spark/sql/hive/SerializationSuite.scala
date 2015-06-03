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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.hive.test.TestHive

class SerializationSuite extends SparkFunSuite {

  test("[SPARK-5840] HiveContext should be serializable") {
    val hiveContext = TestHive
    hiveContext.hiveconf
    val serializer = new JavaSerializer(new SparkConf()).newInstance()
    val bytes = serializer.serialize(hiveContext)
    val deSer = serializer.deserialize[AnyRef](bytes)
  }
}
