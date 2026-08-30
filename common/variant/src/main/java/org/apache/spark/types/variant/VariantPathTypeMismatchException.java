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

package org.apache.spark.types.variant;

/**
 * An exception indicating that an insertion path segment is incompatible with the value it targets
 * (e.g. an object key on a scalar, or an array index on an object). {@code depth} is the index of
 * the failing segment.
 */
public class VariantPathTypeMismatchException extends RuntimeException {
  public final int depth;

  public VariantPathTypeMismatchException(int depth) {
    this.depth = depth;
  }
}
