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

import java.util.Objects;

/**
 /**
 * Represents a {@link ReadLimit} where the {@link MicroBatchStream} should scan approximately
 * given maximum number of rows with at least the given minimum number of rows.
 *
 * @see SupportsAdmissionControl#latestOffset(Offset, ReadLimit)
 * @since 3.1.2
 */
@Evolving
public final class CompositeReadLimit implements ReadLimit {
  private long minRows, maxRows;

  CompositeReadLimit(long minRows, long maxRows) {
    this.minRows = minRows;
    this.maxRows = maxRows;
  }

  /** Approximate minimum rows to scan. */
  public long minRows() { return this.minRows; }


  /** Approximate maximum rows to scan. */
  public long maxRows() { return this.maxRows; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompositeReadLimit that = (CompositeReadLimit) o;
    return minRows == that.minRows &&
            maxRows == that.maxRows;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.minRows, this.maxRows);
  }
}
