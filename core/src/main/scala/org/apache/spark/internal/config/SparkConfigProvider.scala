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

import java.util.{Map => JMap}

import org.apache.spark.SparkConf

/**
 * A config provider that only reads Spark config keys.
 */
private[spark] class SparkConfigProvider(conf: JMap[String, String]) extends ConfigProvider {

  override def get(key: String): Option[String] = {
    if (key.startsWith("spark.")) {
      Option(conf.get(key)).orElse(SparkConf.getDeprecatedConfig(key, conf))
    } else {
      None
    }
  }
}
