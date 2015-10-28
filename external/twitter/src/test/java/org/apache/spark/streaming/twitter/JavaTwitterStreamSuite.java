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

package org.apache.spark.streaming.twitter;

import org.junit.Test;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.NullAuthorization;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

public class JavaTwitterStreamSuite extends LocalJavaStreamingContext {
  @Test
  public void testTwitterStream() {
    String[] filters = { "filter1", "filter2" };
    Authorization auth = NullAuthorization.getInstance();

    // tests the API, does not actually test data receiving
    JavaDStream<Status> test1 = TwitterUtils.createStream(ssc);
    JavaDStream<Status> test2 = TwitterUtils.createStream(ssc, filters);
    JavaDStream<Status> test3 = TwitterUtils.createStream(
      ssc, filters, StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaDStream<Status> test4 = TwitterUtils.createStream(ssc, auth);
    JavaDStream<Status> test5 = TwitterUtils.createStream(ssc, auth, filters);
    JavaDStream<Status> test6 = TwitterUtils.createStream(ssc,
      auth, filters, StorageLevel.MEMORY_AND_DISK_SER_2());
  }
}
