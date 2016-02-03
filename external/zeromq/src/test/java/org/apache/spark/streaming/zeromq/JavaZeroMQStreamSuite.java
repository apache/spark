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

package org.apache.spark.streaming.zeromq;

import akka.actor.ActorSystem;
import akka.actor.SupervisorStrategy;
import akka.util.ByteString;
import akka.zeromq.Subscribe;
import org.junit.Test;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.LocalJavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

public class JavaZeroMQStreamSuite extends LocalJavaStreamingContext {

  @Test // tests the API, does not actually test data receiving
  public void testZeroMQStream() {
    String publishUrl = "abc";
    Subscribe subscribe = new Subscribe((ByteString)null);
    Function<byte[][], Iterable<String>> bytesToObjects = new BytesToObjects();
    Function0<ActorSystem> actorSystemCreator = new ActorSystemCreatorForTest();

    JavaReceiverInputDStream<String> test1 = ZeroMQUtils.<String>createStream(
      ssc, publishUrl, subscribe, bytesToObjects);
    JavaReceiverInputDStream<String> test2 = ZeroMQUtils.<String>createStream(
      ssc, publishUrl, subscribe, bytesToObjects, StorageLevel.MEMORY_AND_DISK_SER_2());
    JavaReceiverInputDStream<String> test3 = ZeroMQUtils.<String>createStream(
      ssc, publishUrl, subscribe, bytesToObjects, StorageLevel.MEMORY_AND_DISK_SER_2(), actorSystemCreator,
      SupervisorStrategy.defaultStrategy());
  }
}

class BytesToObjects implements Function<byte[][], Iterable<String>> {
  @Override
  public Iterable<String> call(byte[][] bytes) throws Exception {
    return null;
  }
}

class ActorSystemCreatorForTest implements Function0<ActorSystem> {
  @Override
  public ActorSystem call() {
    return null;
  }
}
