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

package org.apache.spark.streaming.kafka010;

import java.util.Map;

import scala.collection.JavaConversions;

import org.apache.kafka.common.TopicPartition;
import org.apache.spark.annotation.Experimental;

/**
 * :: Experimental ::
 * Abstract class representing choice of how to schedule consumers for a given TopicPartition on an
 * executor. Kafka 0.10 consumers prefetch messages, so it's important for performance to keep
 * cached consumers on appropriate executors, not recreate them for every partition. Choice of
 * location is only a preference, not an absolute; partitions may be scheduled elsewhere.
 */
@Experimental
public abstract class LocationStrategy {

  /**
   * :: Experimental ::
   * Use this in most cases, it will consistently distribute partitions across all executors.
   */
  public static LocationStrategy PreferConsistent() {
    return PreferConsistent$.MODULE$;
  }

  /**
   * :: Experimental ::
   * Use this only if your executors are on the same nodes as your Kafka brokers.
   */
  public static LocationStrategy PreferBrokers() {
    return PreferBrokers$.MODULE$;
  }


  public static LocationStrategy PreferFixed(Map<TopicPartition, String> hostMap) {
    return new PreferFixed(hostMap);
  }

  public static LocationStrategy PreferFixed(scala.collection.Map<TopicPartition, String> hostMap) {
    return PreferFixed(JavaConversions.mapAsJavaMap(hostMap));
  }
}
