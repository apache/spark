/**
 *
 * Copyright (c) 2005, European Commission project OneLab under contract 034819 (http://www.one-lab.org)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or 
 * without modification, are permitted provided that the following 
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright 
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright 
 *    notice, this list of conditions and the following disclaimer in 
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior 
 *    written permission.
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE 
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT 
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

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

package org.apache.hadoop.util.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.BitSet;

/**
 * Implements a <i>Bloom filter</i>, as defined by Bloom in 1970.
 * <p>
 * The Bloom filter is a data structure that was introduced in 1970 and that has been adopted by 
 * the networking research community in the past decade thanks to the bandwidth efficiencies that it
 * offers for the transmission of set membership information between networked hosts.  A sender encodes 
 * the information into a bit vector, the Bloom filter, that is more compact than a conventional 
 * representation. Computation and space costs for construction are linear in the number of elements.  
 * The receiver uses the filter to test whether various elements are members of the set. Though the 
 * filter will occasionally return a false positive, it will never return a false negative. When creating 
 * the filter, the sender can choose its desired point in a trade-off between the false positive rate and the size. 
 * 
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 * 
 * @see Filter The general behavior of a filter
 * 
 * @see <a href="http://portal.acm.org/citation.cfm?id=362692&dl=ACM&coll=portal">Space/Time Trade-Offs in Hash Coding with Allowable Errors</a>
 */
public class BloomFilter extends Filter {
  private static final byte[] bitvalues = new byte[] {
    (byte)0x01,
    (byte)0x02,
    (byte)0x04,
    (byte)0x08,
    (byte)0x10,
    (byte)0x20,
    (byte)0x40,
    (byte)0x80
  };
  
  /** The bit vector. */
  BitSet bits;

  /** Default constructor - use with readFields */
  public BloomFilter() {
    super();
  }
  
  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   */
  public BloomFilter(int vectorSize, int nbHash, int hashType) {
    super(vectorSize, nbHash, hashType);

    bits = new BitSet(this.vectorSize);
  }

  @Override
  public void add(Key key) {
    if(key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for(int i = 0; i < nbHash; i++) {
      bits.set(h[i]);
    }
  }

  @Override
  public void and(Filter filter) {
    if(filter == null
        || !(filter instanceof BloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    this.bits.and(((BloomFilter) filter).bits);
  }

  @Override
  public boolean membershipTest(Key key) {
    if(key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash.hash(key);
    hash.clear();
    for(int i = 0; i < nbHash; i++) {
      if(!bits.get(h[i])) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void not() {
    bits.flip(0, vectorSize - 1);
  }

  @Override
  public void or(Filter filter) {
    if(filter == null
        || !(filter instanceof BloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }
    bits.or(((BloomFilter) filter).bits);
  }

  @Override
  public void xor(Filter filter) {
    if(filter == null
        || !(filter instanceof BloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }
    bits.xor(((BloomFilter) filter).bits);
  }

  @Override
  public String toString() {
    return bits.toString();
  }

  /**
   * @return size of the the bloomfilter
   */
  public int getVectorSize() {
    return this.vectorSize;
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    byte[] bytes = new byte[getNBytes()];
    for(int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
      if (bitIndex == 8) {
        bitIndex = 0;
        byteIndex++;
      }
      if (bitIndex == 0) {
        bytes[byteIndex] = 0;
      }
      if (bits.get(i)) {
        bytes[byteIndex] |= bitvalues[bitIndex];
      }
    }
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    bits = new BitSet(this.vectorSize);
    byte[] bytes = new byte[getNBytes()];
    in.readFully(bytes);
    for(int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
      if (bitIndex == 8) {
        bitIndex = 0;
        byteIndex++;
      }
      if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
        bits.set(i);
      }
    }
  }
  
  /* @return number of bytes needed to hold bit vector */
  private int getNBytes() {
    return (vectorSize + 7) / 8;
  }
}//end class
