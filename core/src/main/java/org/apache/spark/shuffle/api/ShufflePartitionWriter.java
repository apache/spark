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
import java.io.OutputStream;

import org.apache.spark.annotation.Private;

/**
 * :: Experimental ::
 * An interface for opening streams to persist partition bytes to a backing data store.
 *
 * @since 3.0.0
 */
@Private
public interface ShufflePartitionWriter {

  /**
   * Open and return an {@link OutputStream} that can write bytes to the underlying
   * data store.
   * <p>
   * This method will only be called once to write the bytes to the partition.
   */
  OutputStream openStream() throws IOException;

  /**
   * Get the number of bytes written by this writer's stream returned by {@link #openStream()}.
   * <p>
   * This can be different from the number of bytes given by the caller. For example, the
   * stream might compress or encrypt the bytes before persisting the data to the backing
   * data store.
   */
  long getNumBytesWritten();
}
