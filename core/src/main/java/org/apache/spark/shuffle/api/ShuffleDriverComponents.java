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

package org.apache.spark.shuffle.api;

import java.util.Map;
import java.util.Optional;

import org.apache.spark.annotation.Private;
import org.apache.spark.shuffle.api.metadata.MapOutputMetadata;
import org.apache.spark.shuffle.api.metadata.NoOpShuffleOutputTracker;
import org.apache.spark.shuffle.api.metadata.ShuffleOutputTracker;

/**
 * :: Private ::
 * An interface for building shuffle support modules for the Driver.
 */
@Private
public interface ShuffleDriverComponents {

  /**
   * Provide additional configuration for the executors when their plugin system is initialized
   * via {@link ShuffleDataIO#initializeShuffleExecutorComponents(String, String, Map)} ()}
   *
   * @return additional SparkConf settings necessary for initializing the executor components.
   * This would include configurations that cannot be statically set on the application, like
   * the host:port of external services for shuffle storage.
   */
  Map<String, String> getAddedExecutorSparkConf();

  /**
   * Called once at the end of the Spark application to clean up any existing shuffle state.
   */
  void cleanupApplication();

  default ShuffleOutputTracker shuffleOutputTracker() {
    return new NoOpShuffleOutputTracker();
  }
}
