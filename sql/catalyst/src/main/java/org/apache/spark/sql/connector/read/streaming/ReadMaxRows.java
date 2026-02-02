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
 * Represents a {@link ReadLimit} where the {@link MicroBatchStream} should scan approximately the
 * given maximum number of rows.
 *
 * @see SupportsAdmissionControl#latestOffset(Offset, ReadLimit)
 * @since 3.0.0
 */
@Evolving
public final class ReadMaxRows implements ReadLimit {
  private long rows;

  ReadMaxRows(long rows) {
    this.rows = rows;
  }

  /** Approximate maximum rows to scan. */
  public long maxRows() { return this.rows; }

  @Override
  public String toString() {
    return "MaxRows: " + maxRows();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReadMaxRows other = (ReadMaxRows) o;
    return other.maxRows() == maxRows();
  }

  @Override
  public int hashCode() { return Long.hashCode(this.rows); }
}
