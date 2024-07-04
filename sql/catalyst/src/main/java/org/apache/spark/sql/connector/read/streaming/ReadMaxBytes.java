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
 * Represents a {@link ReadLimit} where the {@link MicroBatchStream} should scan files which total
 * size doesn't go beyond a given maximum total size. Always reads at least one file so a stream
 * can make progress and not get stuck on a file larger than a given maximum.
 *
 * @see SupportsAdmissionControl#latestOffset(Offset, ReadLimit)
 * @since 4.0.0
 */
@Evolving
public class ReadMaxBytes implements ReadLimit {
  private long bytes;

  ReadMaxBytes(long bytes) {
    this.bytes = bytes;
  }

  /** Maximum total size of files to scan. */
  public long maxBytes() { return this.bytes; }

  @Override
  public String toString() {
    return "MaxBytes: " + maxBytes();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReadMaxBytes other = (ReadMaxBytes) o;
    return other.maxBytes() == maxBytes();
  }

  @Override
  public int hashCode() { return Long.hashCode(bytes); }
}
