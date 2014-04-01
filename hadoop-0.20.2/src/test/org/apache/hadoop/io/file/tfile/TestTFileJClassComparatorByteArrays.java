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

import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * Byte arrays test case class using GZ compression codec, base class of none
 * and LZO compression classes.
 * 
 */

public class TestTFileJClassComparatorByteArrays extends TestTFileByteArrays {
  /**
   * Test non-compression codec, using the same test cases as in the ByteArrays.
   */
  @Override
  public void setUp() throws IOException {
    init(Compression.Algorithm.GZ.getName(),
        "jclass: org.apache.hadoop.io.file.tfile.MyComparator",
        "TFileTestJClassComparator", 4480, 4263);
    super.setUp();
  }
}

class MyComparator implements RawComparator<byte[]> {

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
  }

  @Override
  public int compare(byte[] o1, byte[] o2) {
    return WritableComparator.compareBytes(o1, 0, o1.length, o2, 0, o2.length);
  }
  
}

