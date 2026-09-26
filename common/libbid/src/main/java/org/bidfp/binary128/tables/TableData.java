/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bidfp.binary128.tables;

/**
 * Read-only view of generated QUAD UX table words.
 *
 * <p>Scalar reads are used by the numerical kernels and do not allocate.
 * {@link #copy()} is intended for diagnostics and tests that need bulk access.
 */
public final class TableData {
  private final long[] words;

  TableData(long[] words) {
    this.words = words;
  }

  public static TableData copyOf(long[] words) {
    return new TableData(words.clone());
  }

  public long get(int index) {
    return words[index];
  }

  public int length() {
    return words.length;
  }

  public long[] copy() {
    return words.clone();
  }
}
