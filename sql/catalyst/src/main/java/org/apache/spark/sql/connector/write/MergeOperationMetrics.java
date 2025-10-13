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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Evolving;

/**
 * Implementation of {@link OperationMetrics} that provides merge operation metrics.
 *
 * @since 4.1.0
 */
@Evolving
public class MergeOperationMetrics implements OperationMetrics {
  
  private final long numTargetRowsCopied;
  private final long numTargetRowsDeleted;
  private final long numTargetRowsUpdated;
  private final long numTargetRowsInserted;
  private final long numTargetRowsMatchedUpdated;
  private final long numTargetRowsMatchedDeleted;
  private final long numTargetRowsNotMatchedBySourceUpdated;
  private final long numTargetRowsNotMatchedBySourceDeleted;

  /**
   * Constructs a MergeMetrics instance with the specified metric values.
   *
   * @param numTargetRowsCopied number of target rows copied unmodified because
   *                            they did not match any action.
   * @param numTargetRowsDeleted number of target rows deleted.
   * @param numTargetRowsUpdated number of target rows updated.
   * @param numTargetRowsInserted number of target rows inserted.
   * @param numTargetRowsMatchedUpdated number of target rows updated by a matched clause.
   * @param numTargetRowsMatchedDeleted number of target rows deleted by a matched clause.
   * @param numTargetRowsNotMatchedBySourceUpdated number of target rows updated by a not
   *                                               matched by source clause.
   * @param numTargetRowsNotMatchedBySourceDeleted number of target rows deleted by a not matched
   *                                               by source clause.
   */
  public MergeOperationMetrics(
      long numTargetRowsCopied,
      long numTargetRowsDeleted,
      long numTargetRowsUpdated,
      long numTargetRowsInserted,
      long numTargetRowsMatchedUpdated,
      long numTargetRowsMatchedDeleted,
      long numTargetRowsNotMatchedBySourceUpdated,
      long numTargetRowsNotMatchedBySourceDeleted) {
    this.numTargetRowsCopied = numTargetRowsCopied;
    this.numTargetRowsDeleted = numTargetRowsDeleted;
    this.numTargetRowsUpdated = numTargetRowsUpdated;
    this.numTargetRowsInserted = numTargetRowsInserted;
    this.numTargetRowsMatchedUpdated = numTargetRowsMatchedUpdated;
    this.numTargetRowsMatchedDeleted = numTargetRowsMatchedDeleted;
    this.numTargetRowsNotMatchedBySourceUpdated = numTargetRowsNotMatchedBySourceUpdated;
    this.numTargetRowsNotMatchedBySourceDeleted = numTargetRowsNotMatchedBySourceDeleted;
  }

  /**
   * Returns the number of target rows copied unmodified because they did not match any action,
   * or -1 if not available.
   */
  public long getNumTargetRowsCopied() {
    return numTargetRowsCopied;
  }

  /**
   * Returns the number of target rows deleted, or -1 if not available.
   */
  public long getNumTargetRowsDeleted() {
    return numTargetRowsDeleted;
  }

  /**
   * Returns the number of target rows updated, or -1 if not available.
   */
  public long getNumTargetRowsUpdated() {
    return numTargetRowsUpdated;
  }

  /**
   * Returns the number of target rows inserted, or -1 if not available.
   */
  public long getNumTargetRowsInserted() {
    return numTargetRowsInserted;
  }

  /**
   * Returns the number of target rows updated by a matched clause, or -1 if not available.
   */
  public long getNumTargetRowsMatchedUpdated() {
    return numTargetRowsMatchedUpdated;
  }

  /**
   * Returns the number of target rows deleted by a matched clause, or -1 if not available.
   */
  public long getNumTargetRowsMatchedDeleted() {
    return numTargetRowsMatchedDeleted;
  }

  /**
   * Returns the number of target rows updated by a not matched by source clause,
   * or -1 if not available.
   */
  public long getNumTargetRowsNotMatchedBySourceUpdated() {
    return numTargetRowsNotMatchedBySourceUpdated;
  }

  /**
   * Returns the number of target rows deleted by a not matched by source clause,
   * or -1 if not available.
   */
  public long getNumTargetRowsNotMatchedBySourceDeleted() {
    return numTargetRowsNotMatchedBySourceDeleted;
  }
}
