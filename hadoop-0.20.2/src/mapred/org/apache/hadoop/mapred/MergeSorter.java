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
package org.apache.hadoop.mapred;

import java.util.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.MergeSort;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;

/** This class implements the sort method from BasicTypeSorterBase class as
 * MergeSort. Note that this class is really a wrapper over the actual
 * mergesort implementation that is there in the util package. The main intent
 * of providing this class is to setup the input data for the util.MergeSort
 * algo so that the latter doesn't need to bother about the various data 
 * structures that have been created for the Map output but rather concentrate 
 * on the core algorithm (thereby allowing easy integration of a mergesort
 * implementation). The bridge between this class and the util.MergeSort class
 * is the Comparator.
 */
class MergeSorter extends BasicTypeSorterBase 
implements Comparator<IntWritable> {
  private static int progressUpdateFrequency = 10000;
  private int progressCalls = 0;
  
  /** The sort method derived from BasicTypeSorterBase and overridden here*/
  public RawKeyValueIterator sort() {
    MergeSort m = new MergeSort(this);
    int count = super.count;
    if (count == 0) return null;
    int [] pointers = super.pointers;
    int [] pointersCopy = new int[count];
    System.arraycopy(pointers, 0, pointersCopy, 0, count);
    m.mergeSort(pointers, pointersCopy, 0, count);
    return new MRSortResultIterator(super.keyValBuffer, pointersCopy, 
                                    super.startOffsets, super.keyLengths, super.valueLengths);
  }
  /** The implementation of the compare method from Comparator. Note that
   * Comparator.compare takes objects as inputs and so the int values are
   * wrapped in (reusable) IntWritables from the class util.MergeSort
   * @param i
   * @param j
   * @return int as per the specification of Comparator.compare
   */
  public int compare (IntWritable i, IntWritable j) {
    // indicate we're making progress but do a batch update
    if (progressCalls < progressUpdateFrequency) {
      progressCalls++;
    } else {
      progressCalls = 0;
      reporter.progress();
    }  
    return comparator.compare(keyValBuffer.getData(), startOffsets[i.get()],
                              keyLengths[i.get()],
                              keyValBuffer.getData(), startOffsets[j.get()], 
                              keyLengths[j.get()]);
  }
  
  /** Add the extra memory that will be utilized by the sort method */
  public long getMemoryUtilized() {
    //this is memory that will be actually utilized (considering the temp
    //array that will be allocated by the sort() method (mergesort))
    return super.getMemoryUtilized() + super.count * 4; 
  }

}
