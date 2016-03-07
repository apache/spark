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

package org.apache.spark.streaming.akka;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.util.Timeout;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import org.apache.spark.api.java.function.Function0;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

public class JavaAkkaUtilsSuite {

  @Test // tests the API, does not actually test data receiving
  public void testAkkaUtils() {
    JavaStreamingContext jsc = new JavaStreamingContext("local[2]", "test", new Duration(1000));
    try {
      JavaReceiverInputDStream<String> test1 = AkkaUtils.<String>createStream(
        jsc, Props.create(JavaTestActor.class), "test");
      JavaReceiverInputDStream<String> test2 = AkkaUtils.<String>createStream(
        jsc, Props.create(JavaTestActor.class), "test", StorageLevel.MEMORY_AND_DISK_SER_2());
      JavaReceiverInputDStream<String> test3 = AkkaUtils.<String>createStream(
        jsc,
        Props.create(JavaTestActor.class),
        "test", StorageLevel.MEMORY_AND_DISK_SER_2(),
        new ActorSystemCreatorForTest(),
        SupervisorStrategy.defaultStrategy());
    } finally {
      jsc.stop();
    }
  }
}

class ActorSystemCreatorForTest implements Function0<ActorSystem> {
  @Override
  public ActorSystem call() {
    return null;
  }
}


class JavaTestActor extends JavaActorReceiver {
  @Override
  public void onReceive(Object message) throws Exception {
    store((String) message);
    store((String) message, new Timeout(1000));
  }
}
