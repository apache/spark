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

package org.apache.spark.network.yarn.util;

import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;

import org.apache.spark.network.util.ConfigProvider;

/** Use the Hadoop configuration to obtain config values. */
public class HadoopConfigProvider extends ConfigProvider {
  private final Configuration conf;

  public HadoopConfigProvider(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String get(String name) {
    String value = conf.get(name);
    if (value == null) {
      throw new NoSuchElementException(name);
    }
    return value;
  }

  @Override
  public String get(String name, String defaultValue) {
    String value = conf.get(name);
    return value == null ? defaultValue : value;
  }

  @Override
  public Iterable<Map.Entry<String, String>> getAll() {
    return conf;
  }

}
