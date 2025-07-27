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

package org.apache.spark.util;

/**
 * An immutable pair of values. Note that the fields are intentionally designed to be `getLeft` and
 * `getRight` instead of `left` and `right` in order to mitigate the migration burden
 * from `org.apache.commons.lang3.tuple.Pair`.
 */
public record Pair<L, R>(L getLeft, R getRight) {
  public static <L, R> Pair<L, R> of(L left, R right) {
    return new Pair<>(left, right);
  }
}
