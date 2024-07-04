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
 * Interface representing limits on how much to read from a {@link MicroBatchStream} when it
 * implements {@link SupportsAdmissionControl}. There are several child interfaces representing
 * various kinds of limits.
 *
 * @see SupportsAdmissionControl#latestOffset(Offset, ReadLimit)
 * @see ReadAllAvailable
 * @see ReadMaxRows
 * @since 3.0.0
 */
@Evolving
public interface ReadLimit {
  static ReadLimit minRows(long rows, long maxTriggerDelayMs) {
    return new ReadMinRows(rows, maxTriggerDelayMs);
  }

  static ReadLimit maxRows(long rows) { return new ReadMaxRows(rows); }

  static ReadLimit maxFiles(int files) { return new ReadMaxFiles(files); }

  static ReadLimit maxBytes(long bytes) { return new ReadMaxBytes(bytes); }

  static ReadLimit allAvailable() { return ReadAllAvailable.INSTANCE; }

  static ReadLimit compositeLimit(ReadLimit[] readLimits) {
    return new CompositeReadLimit(readLimits);
  }
}
