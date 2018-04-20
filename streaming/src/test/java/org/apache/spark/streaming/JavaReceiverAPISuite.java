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

package org.apache.spark.streaming;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.io.Closeables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.api.java.function.Function;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class JavaReceiverAPISuite implements Serializable {

  @Before
  public void setUp() {
    System.clearProperty("spark.streaming.clock");
  }

  @After
  public void tearDown() {
    System.clearProperty("spark.streaming.clock");
  }

  @Test
  public void testReceiver() throws InterruptedException {
    TestServer server = new TestServer(0);
    server.start();

    AtomicLong dataCounter = new AtomicLong(0);

    try {
      JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "test", new Duration(200));
      JavaReceiverInputDStream<String> input =
        ssc.receiverStream(new JavaSocketReceiver("localhost", server.port()));
      JavaDStream<String> mapped = input.map((Function<String, String>) v1 -> v1 + ".");
      mapped.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
        long count = rdd.count();
        dataCounter.addAndGet(count);
      });

      ssc.start();
      long startTime = System.currentTimeMillis();
      long timeout = 10000;

      Thread.sleep(200);
      for (int i = 0; i < 6; i++) {
        server.send(i + "\n"); // \n to make sure these are separate lines
        Thread.sleep(100);
      }
      while (dataCounter.get() == 0 && System.currentTimeMillis() - startTime < timeout) {
        Thread.sleep(100);
      }
      ssc.stop();
      Assert.assertTrue(dataCounter.get() > 0);
    } finally {
      server.stop();
    }
  }

  private static class JavaSocketReceiver extends Receiver<String> {

    private String host = null;
    private int port = -1;

    JavaSocketReceiver(String host_ , int port_) {
      super(StorageLevel.MEMORY_AND_DISK());
      host = host_;
      port = port_;
    }

    @Override
    public void onStart() {
      new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
    }

    private void receive() {
      try {
        Socket socket = null;
        BufferedReader in = null;
        try {
          socket = new Socket(host, port);
          in = new BufferedReader(
              new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
          String userInput;
          while ((userInput = in.readLine()) != null) {
            store(userInput);
          }
        } finally {
          Closeables.close(in, /* swallowIOException = */ true);
          Closeables.close(socket,  /* swallowIOException = */ true);
        }
      } catch(ConnectException ce) {
        ce.printStackTrace();
        restart("Could not connect", ce);
      } catch(Throwable t) {
        t.printStackTrace();
        restart("Error receiving data", t);
      }
    }
  }

}
