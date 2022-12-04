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

package org.apache.spark.sql.connector.read.stats;

import org.apache.spark.annotation.Evolving;
import java.math.BigInteger;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * An interface to represent column statistics, which is part of
 * {@link Statistics}.
 *
 * @since 3.4.0
 */
@Evolving
public interface ColumnStatistics {
  default Optional<BigInteger> distinctCount() {
    return Optional.empty();
  }

  default Optional<Object> min() {
    return Optional.empty();
  }

  default Optional<Object> max() {
    return Optional.empty();
  }

  default Optional<BigInteger> nullCount() {
    return Optional.empty();
  }

  default OptionalLong avgLen() {
    return OptionalLong.empty();
  }

  default OptionalLong maxLen() {
    return OptionalLong.empty();
  }

  default Optional<Histogram> histogram() {
    return Optional.empty();
  }
}
