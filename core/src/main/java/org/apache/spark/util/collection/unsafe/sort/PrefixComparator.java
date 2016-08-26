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

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.annotation.Private;

/**
 * Compares 8-byte key prefixes in prefix sort. Subclasses may implement type-specific
 * comparisons, such as lexicographic comparison for strings.
 */
@Private
public abstract class PrefixComparator {
  public enum NullOrder {FIRST, LAST}
  public abstract int compare(long prefix1, long prefix2);
}
