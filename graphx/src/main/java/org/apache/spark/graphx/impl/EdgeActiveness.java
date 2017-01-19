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

package org.apache.spark.graphx.impl;

/**
 * Criteria for filtering edges based on activeness. For internal use only.
 */
public enum EdgeActiveness {
  /** Neither the source vertex nor the destination vertex need be active. */
  Neither,
  /** The source vertex must be active. */
  SrcOnly,
  /** The destination vertex must be active. */
  DstOnly,
  /** Both vertices must be active. */
  Both,
  /** At least one vertex must be active. */
  Either
}
