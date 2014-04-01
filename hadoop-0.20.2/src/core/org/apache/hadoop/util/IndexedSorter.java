/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.util;

/**
 * Interface for sort algorithms accepting {@link IndexedSortable} items.
 *
 * A sort algorithm implementing this interface may only
 * {@link IndexedSortable#compare} and {@link IndexedSortable#swap} items
 * for a range of indices to effect a sort across that range.
 */
public interface IndexedSorter {

  /**
   * Sort the items accessed through the given IndexedSortable over the given
   * range of logical indices. From the perspective of the sort algorithm,
   * each index between l (inclusive) and r (exclusive) is an addressable
   * entry.
   * @see IndexedSortable#compare
   * @see IndexedSortable#swap
   */
  void sort(IndexedSortable s, int l, int r);

  /**
   * Same as {@link #sort(IndexedSortable,int,int)}, but indicate progress
   * periodically.
   * @see #sort(IndexedSortable,int,int)
   */
  void sort(IndexedSortable s, int l, int r, Progressable rep);

}
