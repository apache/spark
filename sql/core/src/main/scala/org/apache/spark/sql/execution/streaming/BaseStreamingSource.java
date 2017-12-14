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

package org.apache.spark.sql.execution.streaming;

import org.apache.spark.sql.sources.v2.reader.Offset;

/**
 * The shared interface between V1 streaming sources and V2 streaming readers.
 *
 * This is a temporary interface for compatibility during migration. It should not be implemented
 * directly, and will be removed in future versions.
 */
public interface BaseStreamingSource {
  /**
   * Informs the source that Spark has completed processing all data for offsets less than or
   * equal to `end` and will only request offsets greater than `end` in the future.
   */
  void commit(Offset end);

  /** Stop this source and free any resources it has allocated. */
  void stop();
}
