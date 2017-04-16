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

/**
 * Implements a <i>dynamic Bloom filter</i>, as defined in the INFOCOM 2006 paper.
 * <p>
 * A dynamic Bloom filter (DBF) makes use of a <code>s * m</code> bit matrix but
 * each of the <code>s</code> rows is a standard Bloom filter. The creation 
 * process of a DBF is iterative. At the start, the DBF is a <code>1 * m</code>
 * bit matrix, i.e., it is composed of a single standard Bloom filter.
 * It assumes that <code>n<sub>r</sub></code> elements are recorded in the 
 * initial bit vector, where <code>n<sub>r</sub> <= n</code> (<code>n</code> is
 * the cardinality of the set <code>A</code> to record in the filter).  
 * <p>
 * As the size of <code>A</code> grows during the execution of the application,
 * several keys must be inserted in the DBF.  When inserting a key into the DBF,
 * one must first get an active Bloom filter in the matrix.  A Bloom filter is
 * active when the number of recorded keys, <code>n<sub>r</sub></code>, is 
 * strictly less than the current cardinality of <code>A</code>, <code>n</code>.
 * If an active Bloom filter is found, the key is inserted and 
 * <code>n<sub>r</sub></code> is incremented by one. On the other hand, if there
 * is no active Bloom filter, a new one is created (i.e., a new row is added to
 * the matrix) according to the current size of <code>A</code> and the element
 * is added in this new Bloom filter and the <code>n<sub>r</sub></code> value of
 * this new Bloom filter is set to one.  A given key is said to belong to the
 * DBF if the <code>k</code> positions are set to one in one of the matrix rows.
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 *
 * @see Filter The general behavior of a filter
 * @see BloomFilter A Bloom filter
 * 
 * @see <a href="http://www.cse.fau.edu/~jie/research/publications/Publication_files/infocom2006.pdf">Theory and Network Applications of Dynamic Bloom Filters</a>
 */
public class DynamicBloomFilter extends Filter {
  /** 
   * Threshold for the maximum number of key to record in a dynamic Bloom filter row.
   */
  private int nr;

  /**
   * The number of keys recorded in the current standard active Bloom filter.
   */
  private int currentNbRecord;

  /**
   * The matrix of Bloom filter.
   */
  private BloomFilter[] matrix;

  /**
   * Zero-args constructor for the serialization.
   */
  public DynamicBloomFilter() { }

  /**
   * Constructor.
   * <p>
   * Builds an empty Dynamic Bloom filter.
   * @param vectorSize The number of bits in the vector.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   * @param nr The threshold for the maximum number of keys to record in a
   * dynamic Bloom filter row.
   */
  public DynamicBloomFilter(int vectorSize, int nbHash, int hashType, int nr) {
    super(vectorSize, nbHash, hashType);

    this.nr = nr;
    this.currentNbRecord = 0;

    matrix = new BloomFilter[1];
    matrix[0] = new BloomFilter(this.vectorSize, this.nbHash, this.hashType);
  }

  @Override
  public void add(Key key) {
    if (key == null) {
      throw new NullPointerException("Key can not be null");
    }

    BloomFilter bf = getActiveStandardBF();

    if (bf == null) {
      addRow();
      bf = matrix[matrix.length - 1];
      currentNbRecord = 0;
    }

    bf.add(key);

    currentNbRecord++;
  }

  @Override
  public void and(Filter filter) {
    if (filter == null
        || !(filter instanceof DynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    DynamicBloomFilter dbf = (DynamicBloomFilter)filter;

    if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be and-ed");
    }

    for (int i = 0; i < matrix.length; i++) {
      matrix[i].and(dbf.matrix[i]);
    }
  }

  @Override
  public boolean membershipTest(Key key) {
    if (key == null) {
      return true;
    }

    for (int i = 0; i < matrix.length; i++) {
      if (matrix[i].membershipTest(key)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void not() {
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].not();
    }
  }

  @Override
  public void or(Filter filter) {
    if (filter == null
        || !(filter instanceof DynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }

    DynamicBloomFilter dbf = (DynamicBloomFilter)filter;

    if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be or-ed");
    }
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].or(dbf.matrix[i]);
    }
  }

  @Override
  public void xor(Filter filter) {
    if (filter == null
        || !(filter instanceof DynamicBloomFilter)
        || filter.vectorSize != this.vectorSize
        || filter.nbHash != this.nbHash) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }
    DynamicBloomFilter dbf = (DynamicBloomFilter)filter;

    if (dbf.matrix.length != this.matrix.length || dbf.nr != this.nr) {
      throw new IllegalArgumentException("filters cannot be xor-ed");
    }

    for(int i = 0; i<matrix.length; i++) {
        matrix[i].xor(dbf.matrix[i]);
    }
  }

  @Override
  public String toString() {
    StringBuilder res = new StringBuilder();

    for (int i = 0; i < matrix.length; i++) {
      res.append(matrix[i]);
      res.append(Character.LINE_SEPARATOR);
    }
    return res.toString();
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(nr);
    out.writeInt(currentNbRecord);
    out.writeInt(matrix.length);
    for (int i = 0; i < matrix.length; i++) {
      matrix[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    nr = in.readInt();
    currentNbRecord = in.readInt();
    int len = in.readInt();
    matrix = new BloomFilter[len];
    for (int i = 0; i < matrix.length; i++) {
      matrix[i] = new BloomFilter();
      matrix[i].readFields(in);
    }
  }

  /**
   * Adds a new row to <i>this</i> dynamic Bloom filter.
   */
  private void addRow() {
    BloomFilter[] tmp = new BloomFilter[matrix.length + 1];

    for (int i = 0; i < matrix.length; i++) {
      tmp[i] = matrix[i];
    }

    tmp[tmp.length-1] = new BloomFilter(vectorSize, nbHash, hashType);

    matrix = tmp;
  }

  /**
   * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
   * @return BloomFilter The active standard Bloom filter.
   * 			 <code>Null</code> otherwise.
   */
  private BloomFilter getActiveStandardBF() {
    if (currentNbRecord >= nr) {
      return null;
    }

    return matrix[matrix.length - 1];
  }
}
