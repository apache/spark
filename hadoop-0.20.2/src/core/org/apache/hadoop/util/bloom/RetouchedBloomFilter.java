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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Implements a <i>retouched Bloom filter</i>, as defined in the CoNEXT 2006 paper.
 * <p>
 * It allows the removal of selected false positives at the cost of introducing
 * random false negatives, and with the benefit of eliminating some random false
 * positives at the same time.
 * 
 * <p>
 * Originally created by
 * <a href="http://www.one-lab.org">European Commission One-Lab Project 034819</a>.
 * 
 * @see Filter The general behavior of a filter
 * @see BloomFilter A Bloom filter
 * @see RemoveScheme The different selective clearing algorithms
 * 
 * @see <a href="http://www-rp.lip6.fr/site_npa/site_rp/_publications/740-rbf_cameraready.pdf">Retouched Bloom Filters: Allowing Networked Applications to Trade Off Selected False Positives Against False Negatives</a>
 */
public final class RetouchedBloomFilter extends BloomFilter
implements RemoveScheme {
  /**
   * KeyList vector (or ElementList Vector, as defined in the paper) of false positives.
   */
  List<Key>[] fpVector;

  /**
   * KeyList vector of keys recorded in the filter.
   */
  List<Key>[] keyVector;

  /**
   * Ratio vector.
   */
  double[] ratio;
  
  private Random rand;

  /** Default constructor - use with readFields */
  public RetouchedBloomFilter() {}
  
  /**
   * Constructor
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash function to consider.
   * @param hashType type of the hashing function (see
   * {@link org.apache.hadoop.util.hash.Hash}).
   */
  public RetouchedBloomFilter(int vectorSize, int nbHash, int hashType) {
    super(vectorSize, nbHash, hashType);

    this.rand = null;
    createVector();
  }

  @Override
  public void add(Key key) {
    if (key == null) {
      throw new NullPointerException("key can not be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for (int i = 0; i < nbHash; i++) {
      bits.set(h[i]);
      keyVector[h[i]].add(key);
    }
  }

  /**
   * Adds a false positive information to <i>this</i> retouched Bloom filter.
   * <p>
   * <b>Invariant</b>: if the false positive is <code>null</code>, nothing happens.
   * @param key The false positive key to add.
   */
  public void addFalsePositive(Key key) {
    if (key == null) {
      throw new NullPointerException("key can not be null");
    }

    int[] h = hash.hash(key);
    hash.clear();

    for (int i = 0; i < nbHash; i++) {
      fpVector[h[i]].add(key);
    }
  }

  /**
   * Adds a collection of false positive information to <i>this</i> retouched Bloom filter.
   * @param coll The collection of false positive.
   */
  public void addFalsePositive(Collection<Key> coll) {
    if (coll == null) {
      throw new NullPointerException("Collection<Key> can not be null");
    }
    
    for (Key k : coll) {
      addFalsePositive(k);
    }
  }

  /**
   * Adds a list of false positive information to <i>this</i> retouched Bloom filter.
   * @param keys The list of false positive.
   */
  public void addFalsePositive(List<Key> keys) {
    if (keys == null) {
      throw new NullPointerException("ArrayList<Key> can not be null");
    }

    for (Key k : keys) {
      addFalsePositive(k);
    }
  }

  /**
   * Adds an array of false positive information to <i>this</i> retouched Bloom filter.
   * @param keys The array of false positive.
   */
  public void addFalsePositive(Key[] keys) {
    if (keys == null) {
      throw new NullPointerException("Key[] can not be null");
    }

    for (int i = 0; i < keys.length; i++) {
      addFalsePositive(keys[i]);
    }
  }

  /**
   * Performs the selective clearing for a given key.
   * @param k The false positive key to remove from <i>this</i> retouched Bloom filter.
   * @param scheme The selective clearing scheme to apply.
   */
  public void selectiveClearing(Key k, short scheme) {
    if (k == null) {
      throw new NullPointerException("Key can not be null");
    }

    if (!membershipTest(k)) {
      throw new IllegalArgumentException("Key is not a member");
    }

    int index = 0;
    int[] h = hash.hash(k);

    switch(scheme) {

    case RANDOM:
      index = randomRemove();
      break;
    
    case MINIMUM_FN:
      index = minimumFnRemove(h);
      break;
    
    case MAXIMUM_FP:
      index = maximumFpRemove(h);
      break;
    
    case RATIO:
      index = ratioRemove(h);
      break;
    
    default:
      throw new AssertionError("Undefined selective clearing scheme");

    }

    clearBit(index);
  }

  private int randomRemove() {
    if (rand == null) {
      rand = new Random();
    }

    return rand.nextInt(nbHash);
  }

  /**
   * Chooses the bit position that minimizes the number of false negative generated.
   * @param h The different bit positions.
   * @return The position that minimizes the number of false negative generated.
   */
  private int minimumFnRemove(int[] h) {
    int minIndex = Integer.MAX_VALUE;
    double minValue = Double.MAX_VALUE;

    for (int i = 0; i < nbHash; i++) {
      double keyWeight = getWeight(keyVector[h[i]]);

      if (keyWeight < minValue) {
        minIndex = h[i];
        minValue = keyWeight;
      }

    }

    return minIndex;
  }

  /**
   * Chooses the bit position that maximizes the number of false positive removed.
   * @param h The different bit positions.
   * @return The position that maximizes the number of false positive removed.
   */
  private int maximumFpRemove(int[] h) {
    int maxIndex = Integer.MIN_VALUE;
    double maxValue = Double.MIN_VALUE;

    for (int i = 0; i < nbHash; i++) {
      double fpWeight = getWeight(fpVector[h[i]]);

      if (fpWeight > maxValue) {
        maxValue = fpWeight;
        maxIndex = h[i];
      }
    }

    return maxIndex;
  }

  /**
   * Chooses the bit position that minimizes the number of false negative generated while maximizing.
   * the number of false positive removed.
   * @param h The different bit positions.
   * @return The position that minimizes the number of false negative generated while maximizing.
   */
  private int ratioRemove(int[] h) {
    computeRatio();
    int minIndex = Integer.MAX_VALUE;
    double minValue = Double.MAX_VALUE;

    for (int i = 0; i < nbHash; i++) {
      if (ratio[h[i]] < minValue) {
        minValue = ratio[h[i]];
        minIndex = h[i];
      }
    }

    return minIndex;
  }

  /**
   * Clears a specified bit in the bit vector and keeps up-to-date the KeyList vectors.
   * @param index The position of the bit to clear.
   */
  private void clearBit(int index) {
    if (index < 0 || index >= vectorSize) {
      throw new ArrayIndexOutOfBoundsException(index);
    }

    List<Key> kl = keyVector[index];
    List<Key> fpl = fpVector[index];

    // update key list
    int listSize = kl.size();
    for (int i = 0; i < listSize && !kl.isEmpty(); i++) {
      removeKey(kl.get(0), keyVector);
    }

    kl.clear();
    keyVector[index].clear();

    //update false positive list
    listSize = fpl.size();
    for (int i = 0; i < listSize && !fpl.isEmpty(); i++) {
      removeKey(fpl.get(0), fpVector);
    }

    fpl.clear();
    fpVector[index].clear();

    //update ratio
    ratio[index] = 0.0;

    //update bit vector
    bits.clear(index);
  }

  /**
   * Removes a given key from <i>this</i> filer.
   * @param k The key to remove.
   * @param vector The counting vector associated to the key.
   */
  private void removeKey(Key k, List<Key>[] vector) {
    if (k == null) {
      throw new NullPointerException("Key can not be null");
    }
    if (vector == null) {
      throw new NullPointerException("ArrayList<Key>[] can not be null");
    }

    int[] h = hash.hash(k);
    hash.clear();

    for (int i = 0; i < nbHash; i++) {
      vector[h[i]].remove(k);
    }
  }

  /**
   * Computes the ratio A/FP.
   */
  private void computeRatio() {
    for (int i = 0; i < vectorSize; i++) {
      double keyWeight = getWeight(keyVector[i]);
      double fpWeight = getWeight(fpVector[i]);

      if (keyWeight > 0 && fpWeight > 0) {
        ratio[i] = keyWeight / fpWeight;
      }
    }
  }

  private double getWeight(List<Key> keyList) {
    double weight = 0.0;
    for (Key k : keyList) {
      weight += k.getWeight();
    }
    return weight;
  }
  
  /**
   * Creates and initialises the various vectors.
   */
  @SuppressWarnings("unchecked")
  private void createVector() {
    fpVector = new List[vectorSize];
    keyVector = new List[vectorSize];
    ratio = new double[vectorSize];

    for (int i = 0; i < vectorSize; i++) {
      fpVector[i] = Collections.synchronizedList(new ArrayList<Key>());
      keyVector[i] = Collections.synchronizedList(new ArrayList<Key>());
      ratio[i] = 0.0;
    }
  }
  
  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    for (int i = 0; i < fpVector.length; i++) {
      List<Key> list = fpVector[i];
      out.writeInt(list.size());
      for (Key k : list) {
        k.write(out);
      }
    }
    for (int i = 0; i < keyVector.length; i++) {
      List<Key> list = keyVector[i];
      out.writeInt(list.size());
      for (Key k : list) {
        k.write(out);
      }
    }
    for (int i = 0; i < ratio.length; i++) {
      out.writeDouble(ratio[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    createVector();
    for (int i = 0; i < fpVector.length; i++) {
      List<Key> list = fpVector[i];
      int size = in.readInt();
      for (int j = 0; j < size; j++) {
        Key k = new Key();
        k.readFields(in);
        list.add(k);
      }
    }
    for (int i = 0; i < keyVector.length; i++) {
      List<Key> list = keyVector[i];
      int size = in.readInt();
      for (int j = 0; j < size; j++) {
        Key k = new Key();
        k.readFields(in);
        list.add(k);
      }
    }
    for (int i = 0; i < ratio.length; i++) {
      ratio[i] = in.readDouble();
    }
  }
}
