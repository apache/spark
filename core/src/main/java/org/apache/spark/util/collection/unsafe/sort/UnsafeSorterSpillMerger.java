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

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

final class UnsafeSorterSpillMerger {

  private int numRecords = 0;
  private final PriorityQueue<UnsafeSorterIterator> priorityQueue;

  UnsafeSorterSpillMerger(
      final RecordComparator recordComparator,
      final PrefixComparator prefixComparator,
      final int numSpills) {
    final Comparator<UnsafeSorterIterator> comparator = new Comparator<UnsafeSorterIterator>() {

      @Override
      public int compare(UnsafeSorterIterator left, UnsafeSorterIterator right) {
        final int prefixComparisonResult =
          prefixComparator.compare(left.getKeyPrefix(), right.getKeyPrefix());
        if (prefixComparisonResult == 0) {
          return recordComparator.compare(
            left.getBaseObject(), left.getBaseOffset(),
            right.getBaseObject(), right.getBaseOffset());
        } else {
          return prefixComparisonResult;
        }
      }
    };
    priorityQueue = new PriorityQueue<>(numSpills, comparator);
  }

  /**
   * Add an UnsafeSorterIterator to this merger
   */
  public void addSpillIfNotEmpty(UnsafeSorterIterator spillReader) throws IOException {
    if (spillReader.hasNext()) {
      // We only add the spillReader to the priorityQueue if it is not empty. We do this to
      // make sure the hasNext method of UnsafeSorterIterator returned by getSortedIterator
      // does not return wrong result because hasNext will returns true
      // at least priorityQueue.size() times. If we allow n spillReaders in the
      // priorityQueue, we will have n extra empty records in the result of UnsafeSorterIterator.
      spillReader.loadNext();
      priorityQueue.add(spillReader);
      numRecords += spillReader.getNumRecords();
    }
  }

  public UnsafeSorterIterator getSortedIterator() throws IOException {
    return new UnsafeSorterIterator() {

      private UnsafeSorterIterator spillReader;

      @Override
      public int getNumRecords() {
        return numRecords;
      }

      @Override
      public boolean hasNext() {
        return !priorityQueue.isEmpty() || (spillReader != null && spillReader.hasNext());
      }

      @Override
      public void loadNext() throws IOException {
        if (spillReader != null) {
          if (spillReader.hasNext()) {
            spillReader.loadNext();
            priorityQueue.add(spillReader);
          }
        }
        spillReader = priorityQueue.remove();
      }

      @Override
      public Object getBaseObject() { return spillReader.getBaseObject(); }

      @Override
      public long getBaseOffset() { return spillReader.getBaseOffset(); }

      @Override
      public int getRecordLength() { return spillReader.getRecordLength(); }

      @Override
      public long getKeyPrefix() { return spillReader.getKeyPrefix(); }
    };
  }
}
