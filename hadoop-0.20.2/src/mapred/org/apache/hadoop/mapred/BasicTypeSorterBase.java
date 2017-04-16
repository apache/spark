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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.OutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.SequenceFile.Sorter.RawKeyValueIterator;
import org.apache.hadoop.util.Progressable;

/** This class implements the sort interface using primitive int arrays as 
 * the data structures (that is why this class is called 'BasicType'SorterBase)
 */
abstract class BasicTypeSorterBase implements BufferSorter {
  
  protected OutputBuffer keyValBuffer; //the buffer used for storing
                                           //key/values
  protected int[] startOffsets; //the array used to store the start offsets of
                                //keys in keyValBuffer
  protected int[] keyLengths; //the array used to store the lengths of
                              //keys
  protected int[] valueLengths; //the array used to store the value lengths 
  protected int[] pointers; //the array of startOffsets's indices. This will
                            //be sorted at the end to contain a sorted array of
                            //indices to offsets
  protected RawComparator comparator; //the comparator for the map output
  protected int count; //the number of key/values
  //the overhead of the arrays in memory 
  //12 => 4 for keyoffsets, 4 for keylengths, 4 for valueLengths, and
  //4 for indices into startOffsets array in the
  //pointers array (ignored the partpointers list itself)
  static private final int BUFFERED_KEY_VAL_OVERHEAD = 16;
  static private final int INITIAL_ARRAY_SIZE = 5;
  //we maintain the max lengths of the key/val that we encounter.  During 
  //iteration of the sorted results, we will create a DataOutputBuffer to
  //return the keys. The max size of the DataOutputBuffer will be the max
  //keylength that we encounter. Expose this value to model memory more
  //accurately.
  private int maxKeyLength = 0;
  private int maxValLength = 0;

  //Reference to the Progressable object for sending KeepAlive
  protected Progressable reporter;

  //Implementation of methods of the SorterBase interface
  //
  public void configure(JobConf conf) {
    comparator = conf.getOutputKeyComparator();
  }
  
  public void setProgressable(Progressable reporter) {
    this.reporter = reporter;  
  }

  public void addKeyValue(int recordOffset, int keyLength, int valLength) {
    //Add the start offset of the key in the startOffsets array and the
    //length in the keyLengths array.
    if (startOffsets == null || count == startOffsets.length)
      grow();
    startOffsets[count] = recordOffset;
    keyLengths[count] = keyLength;
    if (keyLength > maxKeyLength) {
      maxKeyLength = keyLength;
    }
    if (valLength > maxValLength) {
      maxValLength = valLength;
    }
    valueLengths[count] = valLength;
    pointers[count] = count;
    count++;
  }

  public void setInputBuffer(OutputBuffer buffer) {
    //store a reference to the keyValBuffer that we need to read during sort
    this.keyValBuffer = buffer;
  }

  public long getMemoryUtilized() {
    //the total length of the arrays + the max{Key,Val}Length (this will be the 
    //max size of the DataOutputBuffers during the iteration of the sorted
    //keys).
    if (startOffsets != null) {
      return (startOffsets.length) * BUFFERED_KEY_VAL_OVERHEAD + 
              maxKeyLength + maxValLength;
    }
    else { //nothing from this yet
      return 0;
    }
  }

  public abstract RawKeyValueIterator sort();
  
  public void close() {
    //set count to 0; also, we don't reuse the arrays since we want to maintain
    //consistency in the memory model
    count = 0;
    startOffsets = null;
    keyLengths = null;
    valueLengths = null;
    pointers = null;
    maxKeyLength = 0;
    maxValLength = 0;
    
    //release the large key-value buffer so that the GC, if necessary,
    //can collect it away
    keyValBuffer = null;
  }
  
  private void grow() {
    int currLength = 0;
    if (startOffsets != null) {
      currLength = startOffsets.length;
    }
    int newLength = (int)(currLength * 1.1) + 1;
    startOffsets = grow(startOffsets, newLength);
    keyLengths = grow(keyLengths, newLength);
    valueLengths = grow(valueLengths, newLength);
    pointers = grow(pointers, newLength);
  }
  
  private int[] grow(int[] old, int newLength) {
    int[] result = new int[newLength];
    if(old != null) { 
      System.arraycopy(old, 0, result, 0, old.length);
    }
    return result;
  }
} //BasicTypeSorterBase

//Implementation of methods of the RawKeyValueIterator interface. These
//methods must be invoked to iterate over key/vals after sort is done.
//
class MRSortResultIterator implements RawKeyValueIterator {
  
  private int count;
  private int[] pointers;
  private int[] startOffsets;
  private int[] keyLengths;
  private int[] valLengths;
  private int currStartOffsetIndex;
  private int currIndexInPointers;
  private OutputBuffer keyValBuffer;
  private DataOutputBuffer key = new DataOutputBuffer();
  private InMemUncompressedBytes value = new InMemUncompressedBytes();
  
  public MRSortResultIterator(OutputBuffer keyValBuffer, 
                              int []pointers, int []startOffsets,
                              int []keyLengths, int []valLengths) {
    this.count = pointers.length;
    this.pointers = pointers;
    this.startOffsets = startOffsets;
    this.keyLengths = keyLengths;
    this.valLengths = valLengths;
    this.keyValBuffer = keyValBuffer;
  }
  
  public Progress getProgress() {
    return null;
  }
  
  public DataOutputBuffer getKey() throws IOException {
    int currKeyOffset = startOffsets[currStartOffsetIndex];
    int currKeyLength = keyLengths[currStartOffsetIndex];
    //reuse the same key
    key.reset();
    key.write(keyValBuffer.getData(), currKeyOffset, currKeyLength);
    return key;
  }

  public ValueBytes getValue() throws IOException {
    //value[i] is stored in the following byte range:
    //startOffsets[i] + keyLengths[i] through valLengths[i]
    value.reset(keyValBuffer,
                startOffsets[currStartOffsetIndex] + keyLengths[currStartOffsetIndex],
                valLengths[currStartOffsetIndex]);
    return value;
  }

  public boolean next() throws IOException {
    if (count == currIndexInPointers)
      return false;
    currStartOffsetIndex = pointers[currIndexInPointers];
    currIndexInPointers++;
    return true;
  }
  
  public void close() {
    return;
  }
  
  //An implementation of the ValueBytes interface for the in-memory value
  //buffers. 
  private static class InMemUncompressedBytes implements ValueBytes {
    private byte[] data;
    int start;
    int dataSize;
    private void reset(OutputBuffer d, int start, int length) 
      throws IOException {
      data = d.getData();
      this.start = start;
      dataSize = length;
    }
            
    public int getSize() {
      return dataSize;
    }
            
    public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException {
      outStream.write(data, start, dataSize);
    }

    public void writeCompressedBytes(DataOutputStream outStream) 
      throws IllegalArgumentException, IOException {
      throw
        new IllegalArgumentException("UncompressedBytes cannot be compressed!");
    }
  
  } // InMemUncompressedBytes

} //MRSortResultIterator
