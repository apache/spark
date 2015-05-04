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

package org.apache.spark.unsafe.sort;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import static org.apache.spark.unsafe.sort.UnsafeSorter.*;

public final class UnsafeSorterSpillMerger {

  private final PriorityQueue<MergeableIterator> priorityQueue;

  public static abstract class MergeableIterator {
    public abstract boolean hasNext();

    public abstract void loadNextRecord();

    public abstract long getPrefix();

    public abstract Object getBaseObject();

    public abstract long getBaseOffset();
  }

  public static final class RecordAddressAndKeyPrefix {
    public Object baseObject;
    public long baseOffset;
    public int recordLength;
    public long keyPrefix;
  }

  public UnsafeSorterSpillMerger(
    final RecordComparator recordComparator,
    final UnsafeSorter.PrefixComparator prefixComparator) {
    final Comparator<MergeableIterator> comparator = new Comparator<MergeableIterator>() {

      @Override
      public int compare(MergeableIterator left, MergeableIterator right) {
        final int prefixComparisonResult =
          prefixComparator.compare(left.getPrefix(), right.getPrefix());
        if (prefixComparisonResult == 0) {
          return recordComparator.compare(
            left.getBaseObject(), left.getBaseOffset(),
            right.getBaseObject(), right.getBaseOffset());
        } else {
          return prefixComparisonResult;
        }
      }
    };
    priorityQueue = new PriorityQueue<MergeableIterator>(10, comparator);
  }

  public void addSpill(MergeableIterator spillReader) {
    if (spillReader.hasNext()) {
      spillReader.loadNextRecord();
    }
    priorityQueue.add(spillReader);
  }

  public Iterator<RecordAddressAndKeyPrefix> getSortedIterator() {
    return new Iterator<RecordAddressAndKeyPrefix>() {

      private MergeableIterator spillReader;
      private final RecordAddressAndKeyPrefix record = new RecordAddressAndKeyPrefix();

      @Override
      public boolean hasNext() {
        return !priorityQueue.isEmpty() || (spillReader != null && spillReader.hasNext());
      }

      @Override
      public RecordAddressAndKeyPrefix next() {
        if (spillReader != null) {
          if (spillReader.hasNext()) {
            spillReader.loadNextRecord();
            priorityQueue.add(spillReader);
          }
        }
        spillReader = priorityQueue.remove();
        record.baseObject = spillReader.getBaseObject();
        record.baseOffset = spillReader.getBaseOffset();
        record.keyPrefix = spillReader.getPrefix();
        return record;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

}
