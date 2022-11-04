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

/**
 * Mixed-in interface to notice to Spark that the offset instance is comparable with another
 * offset instance. Both DSv1 Offset and DSv2 Offset are eligible to extend this interface.
 *
 * @see ValidateOffsetRange
 * @since 3.4.0
 */
public interface ComparableOffset {
  /**
   * The result of comparison between two offsets.
   * <p>
   * - EQUAL: two offsets are equal<br/>
   * - GREATER: this offset is greater than the other offset<br/>
   * - LESS: this offset is less than the other offset<br/>
   * - UNDETERMINED: can't determine which is greater<br/>
   *   e.g. greater for some parts but less for some other parts<br/>
   * - NOT_COMPARABLE: both are not comparable offsets e.g. different types
   */
  enum CompareResult {
    EQUAL,
    GREATER,
    LESS,
    UNDETERMINED,
    NOT_COMPARABLE
  }

  /**
   * Compare this offset with given offset.
   *
   * @see CompareResult
   */
  CompareResult compareTo(ComparableOffset other);
}
