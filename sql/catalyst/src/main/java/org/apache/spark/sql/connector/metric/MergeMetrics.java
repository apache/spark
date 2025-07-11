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
package org.apache.spark.sql.connector.metric;

public interface MergeMetrics {

  class Builder {
    private long numTargetRowsCopied = -1;
    private long numTargetRowsInserted = -1;
    private long numTargetRowsDeleted = -1;
    private long numTargetRowsUpdated = -1;
    private long numTargetRowsMatchedUpdated = -1;
    private long numTargetRowsMatchedDeleted = -1;
    private long numTargetRowsNotMatchedBySourceUpdated = -1;
    private long numTargetRowsNotMatchedBySourceDeleted = -1;
    private long numSourceRows = -1;

    public Builder numTargetRowsCopied(long numTargetRowsCopied) {
      this.numTargetRowsCopied = numTargetRowsCopied;
      return this;
    }

    public Builder numTargetRowsInserted(long numTargetRowsInserted) {
      this.numTargetRowsInserted = numTargetRowsInserted;
      return this;
    }

    public Builder numTargetRowsDeleted(long numTargetRowsDeleted) {
      this.numTargetRowsDeleted = numTargetRowsDeleted;
      return this;
    }

    public Builder numTargetRowsUpdated(long numTargetRowsUpdated) {
      this.numTargetRowsUpdated = numTargetRowsUpdated;
      return this;
    }

    public Builder numTargetRowsMatchedUpdated(long numTargetRowsMatchedUpdated) {
      this.numTargetRowsMatchedUpdated = numTargetRowsMatchedUpdated;
      return this;
    }

    public Builder numTargetRowsMatchedDeleted(long numTargetRowsMatchedDeleted) {
      this.numTargetRowsMatchedDeleted = numTargetRowsMatchedDeleted;
      return this;
    }

    public Builder numTargetRowsNotMatchedBySourceUpdated(long numTargetRowsNotMatchedBySourceUpdated) {
      this.numTargetRowsNotMatchedBySourceUpdated = numTargetRowsNotMatchedBySourceUpdated;
      return this;
    }

    public Builder numTargetRowsNotMatchedBySourceDeleted(long numTargetRowsNotMatchedBySourceDeleted) {
      this.numTargetRowsNotMatchedBySourceDeleted = numTargetRowsNotMatchedBySourceDeleted;
      return this;
    }

    public MergeMetrics build() {
      return new MergeMetrics() {
        @Override
        public long numTargetRowsCopied() {
          return numTargetRowsCopied;
        }

        @Override
        public long numTargetRowsInserted() {
          return numTargetRowsInserted;
        }

        @Override
        public long numTargetRowsDeleted() {
          return numTargetRowsDeleted;
        }

        @Override
        public long numTargetRowsUpdated() {
          return numTargetRowsUpdated;
        }

        @Override
        public long numTargetRowsMatchedUpdated() {
          return numTargetRowsMatchedUpdated;
        }

        @Override
        public long numTargetRowsMatchedDeleted() {
          return numTargetRowsMatchedDeleted;
        }

        @Override
        public long numTargetRowsNotMatchedBySourceUpdated() {
          return numTargetRowsNotMatchedBySourceUpdated;
        }

        @Override
        public long numTargetRowsNotMatchedBySourceDeleted() {
          return numTargetRowsNotMatchedBySourceDeleted;
        }
      };
    }
  }

  /**
   * Returns a new builder for MergeMetrics.
   */
  static Builder builder() {
    return new MergeMetrics.Builder();
  }

  /**
   * Returns the number of target rows copied unmodified because they did not match any action.
   */
  long numTargetRowsCopied();

  /**
   * Returns the number of target rows inserted.
   */
  long numTargetRowsInserted();

  /**
   * Returns the number of target rows deleted.
   */
  long numTargetRowsDeleted();

  /**
   * Returns the number of target rows updated.
   */
  long numTargetRowsUpdated();

  /**
   * Returns the number of target rows matched and updated by a matched clause.
   */
  long numTargetRowsMatchedUpdated();

  /**
   * Returns the number of target rows matched and deleted by a matched clause.
   */
  long numTargetRowsMatchedDeleted();

  /**
   * Returns the number of target rows updated by a not matched by source clause.
   */
  long numTargetRowsNotMatchedBySourceUpdated();

  /**
   * Returns the number of target rows deleted by a not matched by source clause.
   */
  long numTargetRowsNotMatchedBySourceDeleted();
}
