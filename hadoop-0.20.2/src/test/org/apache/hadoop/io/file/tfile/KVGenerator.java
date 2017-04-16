/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.util.Random;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG;

/**
 * Generate random <key, value> pairs.
 */
class KVGenerator {
  private final Random random;
  private final byte[][] dict;
  private final boolean sorted;
  private final DiscreteRNG keyLenRNG, valLenRNG;
  private BytesWritable lastKey;
  private static final int MIN_KEY_LEN = 4;
  private final byte prefix[] = new byte[MIN_KEY_LEN];

  public KVGenerator(Random random, boolean sorted, DiscreteRNG keyLenRNG,
      DiscreteRNG valLenRNG, DiscreteRNG wordLenRNG, int dictSize) {
    this.random = random;
    dict = new byte[dictSize][];
    this.sorted = sorted;
    this.keyLenRNG = keyLenRNG;
    this.valLenRNG = valLenRNG;
    for (int i = 0; i < dictSize; ++i) {
      int wordLen = wordLenRNG.nextInt();
      dict[i] = new byte[wordLen];
      random.nextBytes(dict[i]);
    }
    lastKey = new BytesWritable();
    fillKey(lastKey);
  }
  
  private void fillKey(BytesWritable o) {
    int len = keyLenRNG.nextInt();
    if (len < MIN_KEY_LEN) len = MIN_KEY_LEN;
    o.setSize(len);
    int n = MIN_KEY_LEN;
    while (n < len) {
      byte[] word = dict[random.nextInt(dict.length)];
      int l = Math.min(word.length, len - n);
      System.arraycopy(word, 0, o.get(), n, l);
      n += l;
    }
    if (sorted
        && WritableComparator.compareBytes(lastKey.get(), MIN_KEY_LEN, lastKey
            .getSize()
            - MIN_KEY_LEN, o.get(), MIN_KEY_LEN, o.getSize() - MIN_KEY_LEN) > 0) {
      incrementPrefix();
    }

    System.arraycopy(prefix, 0, o.get(), 0, MIN_KEY_LEN);
    lastKey.set(o);
  }

  private void fillValue(BytesWritable o) {
    int len = valLenRNG.nextInt();
    o.setSize(len);
    int n = 0;
    while (n < len) {
      byte[] word = dict[random.nextInt(dict.length)];
      int l = Math.min(word.length, len - n);
      System.arraycopy(word, 0, o.get(), n, l);
      n += l;
    }
  }
  
  private void incrementPrefix() {
    for (int i = MIN_KEY_LEN - 1; i >= 0; --i) {
      ++prefix[i];
      if (prefix[i] != 0) return;
    }
    
    throw new RuntimeException("Prefix overflown");
  }
  
  public void next(BytesWritable key, BytesWritable value, boolean dupKey) {
    if (dupKey) {
      key.set(lastKey);
    }
    else {
      fillKey(key);
    }
    fillValue(value);
  }
}
