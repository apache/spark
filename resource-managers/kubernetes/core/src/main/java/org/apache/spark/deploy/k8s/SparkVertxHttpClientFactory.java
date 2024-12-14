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

package org.apache.spark.deploy.k8s;

import io.fabric8.kubernetes.client.http.HttpClient;
import io.fabric8.kubernetes.client.http.HttpClient.Builder;
import io.fabric8.kubernetes.client.vertx.VertxHttpClientBuilder;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;

public class SparkVertxHttpClientFactory implements HttpClient.Factory {

  private static volatile SparkVertxHttpClientFactory INSTANCE;

  private final Vertx vertx;

  private SparkVertxHttpClientFactory() {
    this.vertx = createVertxInstance();
  }

  public static SparkVertxHttpClientFactory instance() {
    if (INSTANCE == null) {
      synchronized (SparkVertxHttpClientFactory.class) {
        if (INSTANCE == null) {
          INSTANCE = new SparkVertxHttpClientFactory();
        }
      }
    }
    return INSTANCE;
  }

  @Override
  public Builder newBuilder() {
    return new VertxHttpClientBuilder<>(this, vertx);
  }

  private static synchronized Vertx createVertxInstance() {
    String originalValue = System.getProperty("vertx.disableDnsResolver");

    Vertx vertx;
    try {
      System.setProperty("vertx.disableDnsResolver", "true");
      vertx = Vertx.vertx(new VertxOptions()
        .setUseDaemonThread(true)
        .setFileSystemOptions(new FileSystemOptions()
          .setFileCachingEnabled(false)
          .setClassPathResolvingEnabled(false)
        )
      );
    } finally {
      if (originalValue == null) {
        System.clearProperty("vertx.disableDnsResolver");
      } else {
        System.setProperty("vertx.disableDnsResolver", originalValue);
      }
    }

    return vertx;
  }
}
