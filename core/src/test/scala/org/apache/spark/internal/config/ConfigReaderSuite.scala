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

package org.apache.spark.internal.config

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite

class ConfigReaderSuite extends SparkFunSuite {

  test("variable expansion") {
    val env = Map("ENV1" -> "env1")
    val conf = Map("key1" -> "value1", "key2" -> "value2")

    val reader = new ConfigReader(conf.asJava)
    reader.bindEnv(new MapProvider(env.asJava))

    assert(reader.substitute(null) === null)
    assert(reader.substitute("${key1}") === "value1")
    assert(reader.substitute("key1 is: ${key1}") === "key1 is: value1")
    assert(reader.substitute("${key1} ${key2}") === "value1 value2")
    assert(reader.substitute("${key3}") === "${key3}")
    assert(reader.substitute("${env:ENV1}") === "env1")
    assert(reader.substitute("${system:user.name}") === sys.props("user.name"))
    assert(reader.substitute("${key1") === "${key1")

    // Unknown prefixes.
    assert(reader.substitute("${unknown:value}") === "${unknown:value}")
  }

  test("circular references") {
    val conf = Map("key1" -> "${key2}", "key2" -> "${key1}")
    val reader = new ConfigReader(conf.asJava)
    val e = intercept[IllegalArgumentException] {
      reader.substitute("${key1}")
    }
    assert(e.getMessage().contains("Circular"))
  }

  test("spark conf provider filters config keys") {
    val conf = Map("nonspark.key" -> "value", "spark.key" -> "value")
    val reader = new ConfigReader(new SparkConfigProvider(conf.asJava))
    assert(reader.get("nonspark.key") === None)
    assert(reader.get("spark.key") === Some("value"))
  }

}
