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
package org.apache.spark.sql.execution.datasources.v2;


import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkContext;
import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Map;

import scala.reflect.ClassTag;
/**
 * A helper interface to serialize and broadcast the Hadoop configuration for readers.
 */
@DeveloperApi
public interface BroadcastedHadoopConf {
  SparkSession sparkSession();
  CaseInsensitiveStringMap options();

  /**
   * Override this if you need to rewrite your Hadoop configuration differently
   */
  default Configuration withHadoopConfRewrite(Configuration hadoopConf) {
    return hadoopConf;
  }

  /**
   * Override this if you need to generate your Hadoop configuration differently
   */
  default Configuration hadoopConf() {
    // Hadoop Configurations are case sensitive.
    Map<String, String> caseSensitiveMap = options().asCaseSensitiveMap();
    return withHadoopConfRewrite(
      sparkSession().sessionState().newHadoopConfWithOptions(caseSensitiveMap));
  }

  default Broadcast<SerializableConfiguration> broadcastedConf() {
    SparkContext sc = sparkSession().sparkContext();
    SerializableConfiguration config = new SerializableConfiguration(hadoopConf());
    ClassTag<SerializableConfiguration> tag = ClassTag.apply(SerializableConfiguration.class);
    return sc.broadcast(config, tag);
  }
}
