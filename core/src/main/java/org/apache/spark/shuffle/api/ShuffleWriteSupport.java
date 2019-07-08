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

import java.io.IOException;

import org.apache.spark.annotation.Experimental;

/**
 * :: Experimental ::
 * A module that returns shuffle writers to persist data that is written by shuffle map tasks.
 *
 * @since 3.0.0
 */
@Experimental
public interface ShuffleWriteSupport {

  /**
   * Called once per map task to create a writer that will be responsible for persisting all the
   * partitioned bytes written by that map task.
   * <p>
   * The caller of this method will also call either
   * {@link ShuffleMapOutputWriter#commitAllPartitions()} upon successful completion of the map
   * task, or {@link ShuffleMapOutputWriter#abort(Throwable)} if the map task fails.
   */
  ShuffleMapOutputWriter createMapOutputWriter(
    int shuffleId,
    int mapId,
    long mapTaskAttemptId,
    int numPartitions) throws IOException;
}
