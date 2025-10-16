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
 * Interface that provides MERGE operation metrics.
 *
 * @since 4.1.0
 */
@Evolving
public interface MergeMetrics extends WriteMetrics {

  /**
   * Returns the number of target rows copied unmodified because they did not match any action.
   */
  long numTargetRowsCopied();

  /**
   * Returns the number of target rows deleted.
   */
  long numTargetRowsDeleted();

  /**
   * Returns the number of target rows updated.
   */
  long numTargetRowsUpdated();

  /**
   * Returns the number of target rows inserted.
   */
  long numTargetRowsInserted();

  /**
   * Returns the number of target rows updated by a matched clause.
   */
  long numTargetRowsMatchedUpdated();

  /**
   * Returns the number of target rows deleted by a matched clause
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
