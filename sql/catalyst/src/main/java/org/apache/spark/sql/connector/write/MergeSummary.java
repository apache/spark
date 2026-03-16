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
 * Provides an informational summary of the MERGE operation producing write.
 *
 * @since 4.1.0
 */
@Evolving
public interface MergeSummary extends WriteSummary {

  /**
   * Returns the number of target rows copied unmodified because they did not match any action,
   * or -1 if not found.
   */
  long numTargetRowsCopied();

  /**
   * Returns the number of target rows deleted, or -1 if not found.
   */
  long numTargetRowsDeleted();

  /**
   * Returns the number of target rows updated, or -1 if not found.
   */
  long numTargetRowsUpdated();

  /**
   * Returns the number of target rows inserted, or -1 if not found.
   */
  long numTargetRowsInserted();

  /**
   * Returns the number of target rows updated by a matched clause, or -1 if not found.
   */
  long numTargetRowsMatchedUpdated();

  /**
   * Returns the number of target rows deleted by a matched clause, or -1 if not found.
   */
  long numTargetRowsMatchedDeleted();

  /**
   * Returns the number of target rows updated by a not matched by source clause,
   * or -1 if not found.
   */
  long numTargetRowsNotMatchedBySourceUpdated();

  /**
   * Returns the number of target rows deleted by a not matched by source clause,
   * or -1 if not found.
   */
  long numTargetRowsNotMatchedBySourceDeleted();
}
