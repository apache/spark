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

package org.apache.spark.sql.catalyst.streaming;

/**
 * Interface for streaming sources that can signal completion for sequential execution.
 * This enables sources to indicate when they have finished processing their bounded data,
 * allowing sequential union operations to transition to the next source.
 */
public interface SupportsSequentialExecution {
  /**
   * Returns true if this source has finished processing all available data.
   * This is typically used for bounded sources (like file streams with maxFilesPerTrigger)
   * to indicate when they should be considered complete in sequential execution scenarios.
   */
  boolean isSourceComplete();
}