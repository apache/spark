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

package org.apache.spark.sql.avro

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}

class SerializableConfigurationSuite extends SparkFunSuite {

  private def testSerialization(serializer: SerializerInstance): Unit = {
    import AvroFileFormat.SerializableConfiguration
    val conf = new SerializableConfiguration(new Configuration())

    val serialized = serializer.serialize(conf)

    serializer.deserialize[Any](serialized) match {
      case c: SerializableConfiguration =>
        assert(c.log != null, "log was null")
        assert(c.value != null, "value was null")
      case other => fail(
        s"Expecting ${classOf[SerializableConfiguration]}, but got ${other.getClass}.")
    }
  }

  test("serialization with JavaSerializer") {
    testSerialization(new JavaSerializer(new SparkConf()).newInstance())
  }

  test("serialization with KryoSerializer") {
    testSerialization(new KryoSerializer(new SparkConf()).newInstance())
  }

}
