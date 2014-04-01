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

package org.apache.hadoop.security.token;

import java.io.*;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;

import junit.framework.TestCase;

/** Unit tests for Token */
public class TestToken extends TestCase {

  static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }

  static boolean checkEqual(Token<TokenIdentifier> a, Token<TokenIdentifier> b) {
    return Arrays.equals(a.getIdentifier(), b.getIdentifier())
        && Arrays.equals(a.getPassword(), b.getPassword())
        && isEqual(a.getKind(), b.getKind())
        && isEqual(a.getService(), b.getService());
  }

  /**
   * Test token serialization
   */
  public void testTokenSerialization() throws IOException {
    // Get a token
    Token<TokenIdentifier> sourceToken = new Token<TokenIdentifier>();
    sourceToken.setService(new Text("service"));

    // Write it to an output buffer
    DataOutputBuffer out = new DataOutputBuffer();
    sourceToken.write(out);

    // Read the token back
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    Token<TokenIdentifier> destToken = new Token<TokenIdentifier>();
    destToken.readFields(in);
    assertTrue(checkEqual(sourceToken, destToken));
  }
  
  private static void checkUrlSafe(String str) throws Exception {
    int len = str.length();
    for(int i=0; i < len; ++i) {
      char ch = str.charAt(i);
      if (ch == '-') continue;
      if (ch == '_') continue;
      if (ch >= '0' && ch <= '9') continue;
      if (ch >= 'A' && ch <= 'Z') continue;
      if (ch >= 'a' && ch <= 'z') continue;
      fail("Encoded string " + str + 
           " has invalid character at position " + i);
    }
  }

  public static void testEncodeWritable() throws Exception {
    String[] values = new String[]{"", "a", "bb", "ccc", "dddd", "eeeee",
        "ffffff", "ggggggg", "hhhhhhhh", "iiiiiiiii",
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLM" +
             "NOPQRSTUVWXYZ01234567890!@#$%^&*()-=_+[]{}|;':,./<>?"};
    Token<AbstractDelegationTokenIdentifier> orig;
    Token<AbstractDelegationTokenIdentifier> copy = 
      new Token<AbstractDelegationTokenIdentifier>();
    // ensure that for each string the input and output values match
    for(int i=0; i< values.length; ++i) {
      String val = values[i];
      System.out.println("Input = " + val);
      orig = new Token<AbstractDelegationTokenIdentifier>(val.getBytes(),
          val.getBytes(), new Text(val), new Text(val));
      String encode = orig.encodeToUrlString();
      copy.decodeFromUrlString(encode);
      assertEquals(orig, copy);
      checkUrlSafe(encode);
    }
  }

}
