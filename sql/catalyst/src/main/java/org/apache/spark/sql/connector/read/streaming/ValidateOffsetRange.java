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
 * Mixed-in interface for streaming source class to notice to Spark that this source wants to
 * validate the range of offset (start, end). Both DSv1 and DSv2 streaming source are eligible to
 * extend this interface.
 *
 * The Offset class for this source should implement {@link ComparableOffset} to allow Spark to
 * perform validation.
 *
 * @see ComparableOffset
 * @since 3.4.0
 */
public interface ValidateOffsetRange {
  /**
   * Whether to validate offset range. Default implementation always returns true.
   *
   * Data sources wanting to "selectively" validate the range should override this method to
   * control the return value. (e.g. failOnLoss option in Kafka data source)
   *
   * @return true if Spark should validate the offset range, false otherwise.
   */
  default boolean shouldValidate() { return true; }
}
