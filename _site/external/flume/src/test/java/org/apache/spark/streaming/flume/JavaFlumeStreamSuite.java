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

package org.apache.spark.streaming.flume;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.junit.Test;

public class JavaFlumeStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testFlumeStream() {
    // tests the API, does not actually test data receiving
    JavaReceiverInputDStream<SparkFlumeEvent> test1 = FlumeUtils.createStream(ssc, "localhost", 12345);
    JavaReceiverInputDStream<SparkFlumeEvent> test2 = FlumeUtils.createStream(ssc, "localhost", 12345,
      StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaReceiverInputDStream<SparkFlumeEvent> test3 = FlumeUtils.createStream(ssc, "localhost", 12345,
      StorageLevel.MEMORY_AND_DISK_SER_2(), false);
  }
}
