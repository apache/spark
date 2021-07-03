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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;

/**
 * Represents a {@link ReadLimit} where the {@link MicroBatchStream} must scan all the data
 * available at the streaming source. This is meant to be a hard specification as being able
 * to return all available data is necessary for {@code Trigger.Once()} to work correctly.
 * If a source is unable to scan all available data, then it must throw an error.
 *
 * @see SupportsAdmissionControl#latestOffset(Offset, ReadLimit)
 * @since 3.0.0
 */
@Evolving
public final class ReadAllAvailable implements ReadLimit {
  static final ReadAllAvailable INSTANCE = new ReadAllAvailable();

  private ReadAllAvailable() {}

  @Override
  public String toString() {
    return "All Available";
  }
}
