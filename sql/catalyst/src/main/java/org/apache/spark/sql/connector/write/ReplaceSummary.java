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
 * Provides an informational summary of the REPLACE operation producing write.
 *
 * @since 4.2.0
 */
@Evolving
public interface ReplaceSummary extends WriteSummary {

  /**
   * Returns the number of source rows appended by the replace, including rows that share a scope
   * tuple, or -1 if not found.
   */
  long numInsertedRows();

  /**
   * Returns the number of target rows removed because their scope tuple matched the source,
   * or -1 if not found.
   */
  long numDeletedRows();

  /**
   * Returns the number of target rows physically re-emitted by copy-on-write replacement, or -1 if
   * not found. This is 0 for merge-on-read row-level replacement, and it is not the number of
   * logical target rows left unaffected by the replace.
   */
  long numCopiedRows();
}
