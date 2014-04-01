/**
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

/**
 * Keeps the Ranges sorted by startIndex.
 * The added ranges are always ensured to be non-overlapping.
 * Provides the SkipRangeIterator, which skips the Ranges 
 * stored in this object.
 */
class SortedRanges implements Writable{
  
  private static final Log LOG = 
    LogFactory.getLog(SortedRanges.class);
  
  private TreeSet<Range> ranges = new TreeSet<Range>();
  private long indicesCount;
  
  /**
   * Get Iterator which skips the stored ranges.
   * The Iterator.next() call return the index starting from 0.
   * @return SkipRangeIterator
   */
  synchronized SkipRangeIterator skipRangeIterator(){
    return new SkipRangeIterator(ranges.iterator());
  }
  
  /**
   * Get the no of indices stored in the ranges.
   * @return indices count
   */
  synchronized long getIndicesCount() {
    return indicesCount;
  }
  
  /**
   * Get the sorted set of ranges.
   * @return ranges
   */
  synchronized SortedSet<Range> getRanges() {
  	return ranges;
 	}
  
  /**
   * Add the range indices. It is ensured that the added range 
   * doesn't overlap the existing ranges. If it overlaps, the 
   * existing overlapping ranges are removed and a single range 
   * having the superset of all the removed ranges and this range 
   * is added. 
   * If the range is of 0 length, doesn't do anything.
   * @param range Range to be added.
   */
  synchronized void add(Range range){
    if(range.isEmpty()) {
      return;
    }
    
    long startIndex = range.getStartIndex();
    long endIndex = range.getEndIndex();
    //make sure that there are no overlapping ranges
    SortedSet<Range> headSet = ranges.headSet(range);
    if(headSet.size()>0) {
      Range previousRange = headSet.last();
      LOG.debug("previousRange "+previousRange);
      if(startIndex<previousRange.getEndIndex()) {
        //previousRange overlaps this range
        //remove the previousRange
        if(ranges.remove(previousRange)) {
          indicesCount-=previousRange.getLength();
        }
        //expand this range
        startIndex = previousRange.getStartIndex();
        endIndex = endIndex>=previousRange.getEndIndex() ?
                          endIndex : previousRange.getEndIndex();
      }
    }
    
    Iterator<Range> tailSetIt = ranges.tailSet(range).iterator();
    while(tailSetIt.hasNext()) {
      Range nextRange = tailSetIt.next();
      LOG.debug("nextRange "+nextRange +"   startIndex:"+startIndex+
          "  endIndex:"+endIndex);
      if(endIndex>=nextRange.getStartIndex()) {
        //nextRange overlaps this range
        //remove the nextRange
        tailSetIt.remove();
        indicesCount-=nextRange.getLength();
        if(endIndex<nextRange.getEndIndex()) {
          //expand this range
          endIndex = nextRange.getEndIndex();
          break;
        }
      } else {
        break;
      }
    }
    add(startIndex,endIndex);
  }
  
  /**
   * Remove the range indices. If this range is  
   * found in existing ranges, the existing ranges 
   * are shrunk.
   * If range is of 0 length, doesn't do anything.
   * @param range Range to be removed.
   */
  synchronized void remove(Range range) {
    if(range.isEmpty()) {
      return;
    }
    long startIndex = range.getStartIndex();
    long endIndex = range.getEndIndex();
    //make sure that there are no overlapping ranges
    SortedSet<Range> headSet = ranges.headSet(range);
    if(headSet.size()>0) {
      Range previousRange = headSet.last();
      LOG.debug("previousRange "+previousRange);
      if(startIndex<previousRange.getEndIndex()) {
        //previousRange overlaps this range
        //narrow down the previousRange
        if(ranges.remove(previousRange)) {
          indicesCount-=previousRange.getLength();
          LOG.debug("removed previousRange "+previousRange);
        }
        add(previousRange.getStartIndex(), startIndex);
        if(endIndex<=previousRange.getEndIndex()) {
          add(endIndex, previousRange.getEndIndex());
        }
      }
    }
    
    Iterator<Range> tailSetIt = ranges.tailSet(range).iterator();
    while(tailSetIt.hasNext()) {
      Range nextRange = tailSetIt.next();
      LOG.debug("nextRange "+nextRange +"   startIndex:"+startIndex+
          "  endIndex:"+endIndex);
      if(endIndex>nextRange.getStartIndex()) {
        //nextRange overlaps this range
        //narrow down the nextRange
        tailSetIt.remove();
        indicesCount-=nextRange.getLength();
        if(endIndex<nextRange.getEndIndex()) {
          add(endIndex, nextRange.getEndIndex());
          break;
        }
      } else {
        break;
      }
    }
  }
  
  private void add(long start, long end) {
    if(end>start) {
      Range recRange = new Range(start, end-start);
      ranges.add(recRange);
      indicesCount+=recRange.getLength();
      LOG.debug("added "+recRange);
    }
  }
  
  public synchronized void readFields(DataInput in) throws IOException {
    indicesCount = in.readLong();
    ranges = new TreeSet<Range>();
    int size = in.readInt();
    for(int i=0;i<size;i++) {
      Range range = new Range();
      range.readFields(in);
      ranges.add(range);
    }
  }

  public synchronized void write(DataOutput out) throws IOException {
    out.writeLong(indicesCount);
    out.writeInt(ranges.size());
    Iterator<Range> it = ranges.iterator();
    while(it.hasNext()) {
      Range range = it.next();
      range.write(out);
    }
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    Iterator<Range> it = ranges.iterator();
    while(it.hasNext()) {
      Range range = it.next();
      sb.append(range.toString()+"\n");
    }
    return sb.toString();
  }
  
  /**
   * Index Range. Comprises of start index and length.
   * A Range can be of 0 length also. The Range stores indices 
   * of type long.
   */
  static class Range implements Comparable<Range>, Writable{
    private long startIndex;
    private long length;
        
    Range(long startIndex, long length) {
      if(length<0) {
        throw new RuntimeException("length can't be negative");
      }
      this.startIndex = startIndex;
      this.length = length;
    }
    
    Range() {
      this(0,0);
    }
    
    /**
     * Get the start index. Start index in inclusive.
     * @return startIndex. 
     */
    long getStartIndex() {
      return startIndex;
    }
    
    /**
     * Get the end index. End index is exclusive.
     * @return endIndex.
     */
    long getEndIndex() {
      return startIndex + length;
    }
    
   /**
    * Get Length.
    * @return length
    */
    long getLength() {
      return length;
    }
    
    /**
     * Range is empty if its length is zero.
     * @return <code>true</code> if empty
     *         <code>false</code> otherwise.
     */
    boolean isEmpty() {
      return length==0;
    }
    
    public boolean equals(Object o) {
      if(o!=null && o instanceof Range) {
        Range range = (Range)o;
        return startIndex==range.startIndex &&
        length==range.length;
      }
      return false;
    }
    
    public int hashCode() {
      return Long.valueOf(startIndex).hashCode() +
          Long.valueOf(length).hashCode();
    }
    
    public int compareTo(Range o) {
      if(this.equals(o)) {
        return 0;
      }
      return (this.startIndex > o.startIndex) ? 1:-1;
    }

    public void readFields(DataInput in) throws IOException {
      startIndex = in.readLong();
      length = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(startIndex);
      out.writeLong(length);
    }
    
    public String toString() {
      return startIndex +":" + length;
    }    
  }
  
  /**
   * Index Iterator which skips the stored ranges.
   */
  static class SkipRangeIterator implements Iterator<Long> {
    Iterator<Range> rangeIterator;
    Range range = new Range();
    long next = -1;
    
    /**
     * Constructor
     * @param rangeIterator the iterator which gives the ranges.
     */
    SkipRangeIterator(Iterator<Range> rangeIterator) {
      this.rangeIterator = rangeIterator;
      doNext();
    }
    
    /**
     * Returns true till the index reaches Long.MAX_VALUE.
     * @return <code>true</code> next index exists.
     *         <code>false</code> otherwise.
     */
    public synchronized boolean hasNext() {
      return next<Long.MAX_VALUE;
    }
    
    /**
     * Get the next available index. The index starts from 0.
     * @return next index
     */
    public synchronized Long next() {
      long ci = next;
      doNext();
      return ci;
    }
    
    private void doNext() {
      next++;
      LOG.debug("currentIndex "+next +"   "+range);
      skipIfInRange();
      while(next>=range.getEndIndex() && rangeIterator.hasNext()) {
        range = rangeIterator.next();
        skipIfInRange();
      }
    }
    
    private void skipIfInRange() {
      if(next>=range.getStartIndex() && 
          next<range.getEndIndex()) {
        //need to skip the range
        LOG.warn("Skipping index " + next +"-" + range.getEndIndex());
        next = range.getEndIndex();
        
      }
    }
    
    /**
     * Get whether all the ranges have been skipped.
     * @return <code>true</code> if all ranges have been skipped.
     *         <code>false</code> otherwise.
     */
    synchronized boolean skippedAllRanges() {
      return !rangeIterator.hasNext() && next>range.getEndIndex();
    }
    
    /**
     * Remove is not supported. Doesn't apply.
     */
    public void remove() {
      throw new UnsupportedOperationException("remove not supported.");
    }
    
  }

}
