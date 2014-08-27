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

package org.apache.spark.streaming.redis;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.junit.Test;

import org.apache.spark.streaming.LocalJavaStreamingContext;

import scala.collection.JavaConversions;
import java.util.ArrayList;
import java.util.List;


public class JavaRedisStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testRedisStream() {
    String redisUrl = "abc";
    List<String> channels = new ArrayList<String>();
    
    channels.add("abc");

    List<String> patterns = new ArrayList<String>();

    // tests the API, does not actually test data receiving
    JavaReceiverInputDStream<String> test1 = RedisUtils.createStream(ssc, redisUrl, JavaConversions.asScalaBuffer(channels), JavaConversions.asScalaBuffer(patterns));
    JavaReceiverInputDStream<String> test2 = RedisUtils.createStream(ssc, redisUrl, JavaConversions.asScalaBuffer(channels), JavaConversions.asScalaBuffer(patterns),
      StorageLevel.MEMORY_AND_DISK_SER_2());
  }
}
