package org.apache.spark.streaming.flume;/*
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

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.flume.FlumeFunctions;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.junit.Test;

public class JavaFlumeStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testFlumeStream() {
    FlumeFunctions flumeFunc = new FlumeFunctions(ssc);

    // tests the API, does not actually test data receiving
    JavaDStream<SparkFlumeEvent> test1 = flumeFunc.flumeStream("localhost", 12345);
    JavaDStream<SparkFlumeEvent> test2 = flumeFunc.flumeStream("localhost", 12345,
      StorageLevel.MEMORY_AND_DISK_SER_2());
  }
}
