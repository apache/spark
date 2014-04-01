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
package org.apache.hadoop.record.compiler;

import java.util.ArrayList;

/**
 * A wrapper around StringBuffer that automatically does indentation
 */
public class CodeBuffer {
  
  static private ArrayList<Character> startMarkers = new ArrayList<Character>();
  static private ArrayList<Character> endMarkers = new ArrayList<Character>();
  
  static {
    addMarkers('{', '}');
    addMarkers('(', ')');
  }
  
  static void addMarkers(char ch1, char ch2) {
    startMarkers.add(ch1);
    endMarkers.add(ch2);
  }
  
  private int level = 0;
  private int numSpaces = 2;
  private boolean firstChar = true;
  private StringBuffer sb;
  
  /** Creates a new instance of CodeBuffer */
  CodeBuffer() {
    this(2, "");
  }
  
  CodeBuffer(String s) {
    this(2, s);
  }
  
  CodeBuffer(int numSpaces, String s) {
    sb = new StringBuffer();
    this.numSpaces = numSpaces;
    this.append(s);
  }
  
  void append(String s) {
    int length = s.length();
    for (int idx = 0; idx < length; idx++) {
      char ch = s.charAt(idx);
      append(ch);
    }
  }
  
  void append(char ch) {
    if (endMarkers.contains(ch)) {
      level--;
    }
    if (firstChar) {
      for (int idx = 0; idx < level; idx++) {
        for (int num = 0; num < numSpaces; num++) {
          rawAppend(' ');
        }
      }
    }
    rawAppend(ch);
    firstChar = false;
    if (startMarkers.contains(ch)) {
      level++;
    }
    if (ch == '\n') {
      firstChar = true;
    }
  }

  private void rawAppend(char ch) {
    sb.append(ch);
  }
  
  public String toString() {
    return sb.toString();
  }
}
