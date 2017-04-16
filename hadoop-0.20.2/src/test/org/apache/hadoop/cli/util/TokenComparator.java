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

package org.apache.hadoop.cli.util;

import java.util.StringTokenizer;

/**
 * Comparator for the Command line tests. 
 * 
 * This comparator compares each token in the expected output and returns true
 * if all tokens are in the actual output
 * 
 */
public class TokenComparator extends ComparatorBase {

  @Override
  public boolean compare(String actual, String expected) {
    boolean compareOutput = true;
    
    StringTokenizer tokenizer = new StringTokenizer(expected, ",\n\r");
    
    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      if (actual.indexOf(token) != -1) {
        compareOutput &= true;
      } else {
        compareOutput &= false;
      }
    }
    
    return compareOutput;
  }
}
