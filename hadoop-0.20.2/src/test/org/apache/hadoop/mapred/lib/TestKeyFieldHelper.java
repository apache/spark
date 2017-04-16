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
package org.apache.hadoop.mapred.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

public class TestKeyFieldHelper extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestKeyFieldHelper.class);
  /**
   * Test is key-field-helper's parse option.
   */
  public void testparseOption() throws Exception {
    KeyFieldHelper helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    String keySpecs = "-k1.2,3.4";
    String eKeySpecs = keySpecs;
    helper.parseOption(keySpecs);
    String actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    // test -k a.b
    keySpecs = "-k 1.2";
    eKeySpecs = "-k1.2,0.0";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-nr -k1.2,3.4";
    eKeySpecs = "-k1.2,3.4nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-nr -k1.2,3.4n";
    eKeySpecs = "-k1.2,3.4n";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-nr -k1.2,3.4r";
    eKeySpecs = "-k1.2,3.4r";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-nr -k1.2,3.4 -k5.6,7.8n -k9.10,11.12r -k13.14,15.16nr";
    //1st
    eKeySpecs = "-k1.2,3.4nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    // 2nd
    eKeySpecs = "-k5.6,7.8n";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    //3rd
    eKeySpecs = "-k9.10,11.12r";
    actKeySpecs = helper.keySpecs().get(2).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    //4th
    eKeySpecs = "-k13.14,15.16nr";
    actKeySpecs = helper.keySpecs().get(3).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2n,3.4";
    eKeySpecs = "-k1.2,3.4n";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2r,3.4";
    eKeySpecs = "-k1.2,3.4r";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2nr,3.4";
    eKeySpecs = "-k1.2,3.4nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2,3.4n";
    eKeySpecs = "-k1.2,3.4n";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2,3.4r";
    eKeySpecs = "-k1.2,3.4r";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2,3.4nr";
    eKeySpecs = "-k1.2,3.4nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-nr -k1.2,3.4 -k5.6,7.8";
    eKeySpecs = "-k1.2,3.4nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    eKeySpecs = "-k5.6,7.8nr";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-n -k1.2,3.4 -k5.6,7.8";
    eKeySpecs = "-k1.2,3.4n";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    eKeySpecs = "-k5.6,7.8n";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-r -k1.2,3.4 -k5.6,7.8";
    eKeySpecs = "-k1.2,3.4r";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    eKeySpecs = "-k5.6,7.8r";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2,3.4n -k5.6,7.8";
    eKeySpecs = "-k1.2,3.4n";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    eKeySpecs = "-k5.6,7.8";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2,3.4r -k5.6,7.8";
    eKeySpecs = "-k1.2,3.4r";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    eKeySpecs = "-k5.6,7.8";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-k1.2,3.4nr -k5.6,7.8";
    eKeySpecs = "-k1.2,3.4nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    eKeySpecs = "-k5.6,7.8";
    actKeySpecs = helper.keySpecs().get(1).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-n";
    eKeySpecs = "-k1.1,0.0n";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-r";
    eKeySpecs = "-k1.1,0.0r";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
    
    keySpecs = "-nr";
    eKeySpecs = "-k1.1,0.0nr";
    helper = new KeyFieldHelper();
    helper.parseOption(keySpecs);
    actKeySpecs = helper.keySpecs().get(0).toString();
    assertEquals("KeyFieldHelper's parsing is garbled", eKeySpecs, actKeySpecs);
  }
  
  /**
   * Test is key-field-helper's getWordLengths.
   */
  public void testGetWordLengths() throws Exception {
    KeyFieldHelper helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    // test getWordLengths with unspecified key-specifications
    String input = "hi";
    int[] result = helper.getWordLengths(input.getBytes(), 0, 2);
    assertTrue(equals(result, new int[] {1}));
    
    // set the key specs
    helper.setKeyFieldSpec(1, 2);
    
    // test getWordLengths with 3 words
    input = "hi\thello there";
    result = helper.getWordLengths(input.getBytes(), 0, input.length());
    assertTrue(equals(result, new int[] {2, 2, 11}));
    
    // test getWordLengths with 4 words but with a different separator
    helper.setKeyFieldSeparator(" ");
    input = "hi hello\tthere you";
    result = helper.getWordLengths(input.getBytes(), 0, input.length());
    assertTrue(equals(result, new int[] {3, 2, 11, 3}));
    
    // test with non zero start index
    input = "hi hello there you where me there";
    //                 .....................
    result = helper.getWordLengths(input.getBytes(), 10, 33);
    assertTrue(equals(result, new int[] {5, 4, 3, 5, 2, 3}));
    
    input = "hi hello there you where me ";
    //                 ..................
    result = helper.getWordLengths(input.getBytes(), 10, input.length());
    assertTrue(equals(result, new int[] {5, 4, 3, 5, 2, 0}));
    
    input = "";
    result = helper.getWordLengths(input.getBytes(), 0, 0);
    assertTrue(equals(result, new int[] {1, 0}));
    
    input = "  abc";
    result = helper.getWordLengths(input.getBytes(), 0, 5);
    assertTrue(equals(result, new int[] {3, 0, 0, 3}));
    
    input = "  abc";
    result = helper.getWordLengths(input.getBytes(), 0, 2);
    assertTrue(equals(result, new int[] {3, 0, 0, 0}));
    
    input = " abc ";
    result = helper.getWordLengths(input.getBytes(), 0, 2);
    assertTrue(equals(result, new int[] {2, 0, 1}));
    
    helper.setKeyFieldSeparator("abcd");
    input = "abc";
    result = helper.getWordLengths(input.getBytes(), 0, 3);
    assertTrue(equals(result, new int[] {1, 3}));
  }
  
  /**
   * Test is key-field-helper's getStartOffset/getEndOffset.
   */
  public void testgetStartEndOffset() throws Exception {
    KeyFieldHelper helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    // test getStartOffset with -k1,2
    helper.setKeyFieldSpec(1, 2);
    String input = "hi\thello";
    String expectedOutput = input;
    testKeySpecs(input, expectedOutput, helper);
    
    // test getStartOffset with -k1.0,0 .. should result into start = -1
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k1.0,0");
    testKeySpecs(input, null, helper);
    
    // test getStartOffset with -k1,0
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k1,0");
    expectedOutput = input;
    testKeySpecs(input, expectedOutput, helper);
    
    // test getStartOffset with -k1.2,0
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k1.2,0");
    expectedOutput = "i\thello";
    testKeySpecs(input, expectedOutput, helper);
    
    // test getWordLengths with -k1.0,2.3
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k1.1,2.3");
    expectedOutput = "hi\thel";
    testKeySpecs(input, expectedOutput, helper);
    
    // test getWordLengths with -k1.2,2.3
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k1.2,2.3");
    expectedOutput = "i\thel";
    testKeySpecs(input, expectedOutput, helper);
    
    // test getStartOffset with -k1.2,3.0
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k1.2,3.0");
    expectedOutput = "i\thello";
    testKeySpecs(input, expectedOutput, helper);
    
    // test getStartOffset with -k2,2
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k2,2");
    expectedOutput = "hello";
    testKeySpecs(input, expectedOutput, helper);
    
    // test getStartOffset with -k3.0,4.0
    helper = new KeyFieldHelper();
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k3.1,4.0");
    testKeySpecs(input, null, helper);
    
    // test getStartOffset with -k2.1
    helper = new KeyFieldHelper();
    input = "123123123123123hi\thello\thow";
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k2.1");
    expectedOutput = "hello\thow";
    testKeySpecs(input, expectedOutput, helper, 15, input.length());
    
    // test getStartOffset with -k2.1,4 with end ending on \t
    helper = new KeyFieldHelper();
    input = "123123123123123hi\thello\t\thow\tare";
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k2.1,3");
    expectedOutput = "hello\t";
    testKeySpecs(input, expectedOutput, helper, 17, input.length());
    
    // test getStartOffset with -k2.1 with end ending on \t
    helper = new KeyFieldHelper();
    input = "123123123123123hi\thello\thow\tare";
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k2.1");
    expectedOutput = "hello\thow\t";
    testKeySpecs(input, expectedOutput, helper, 17, 28);
    
    // test getStartOffset with -k2.1,3 with smaller length
    helper = new KeyFieldHelper();
    input = "123123123123123hi\thello\thow";
    helper.setKeyFieldSeparator("\t");
    helper.parseOption("-k2.1,3");
    expectedOutput = "hello";
    testKeySpecs(input, expectedOutput, helper, 15, 23);
  }
  
  private void testKeySpecs(String input, String expectedOutput, 
                            KeyFieldHelper helper) {
    testKeySpecs(input, expectedOutput, helper, 0, -1);
  }
  
  private void testKeySpecs(String input, String expectedOutput, 
                            KeyFieldHelper helper, int s1, int e1) {
    LOG.info("input : " + input);
    String keySpecs = helper.keySpecs().get(0).toString();
    LOG.info("keyspecs : " + keySpecs);
    byte[] inputBytes = input.getBytes(); // get the input bytes
    if (e1 == -1) {
      e1 = inputBytes.length;
    }
    LOG.info("length : " + e1);
    // get the word lengths
    int[] indices = helper.getWordLengths(inputBytes, s1, e1);
    // get the start index
    int start = helper.getStartOffset(inputBytes, s1, e1, indices, 
                                      helper.keySpecs().get(0));
    LOG.info("start : " + start);
    if (expectedOutput == null) {
      assertEquals("Expected -1 when the start index is invalid", -1, start);
      return;
    }
    // get the end index
    int end = helper.getEndOffset(inputBytes, s1, e1, indices, 
                                  helper.keySpecs().get(0));
    LOG.info("end : " + end);
    //my fix
    end = (end >= inputBytes.length) ? inputBytes.length -1 : end;
    int length = end + 1 - start;
    LOG.info("length : " + length);
    byte[] outputBytes = new byte[length];
    System.arraycopy(inputBytes, start, outputBytes, 0, length);
    String output = new String(outputBytes);
    LOG.info("output : " + output);
    LOG.info("expected-output : " + expectedOutput);
    assertEquals(keySpecs + " failed on input '" + input + "'", 
                 expectedOutput, output);
  }

  // check for equality of 2 int arrays
  private boolean equals(int[] test, int[] expected) {
    // check array length
    if (test[0] != expected[0]) {
      return false;
    }
    // if length is same then check the contents
    for (int i = 0; i < test[0] && i < expected[0]; ++i) {
      if (test[i] != expected[i]) {
        return false;
      }
    }
    return true;
  }
}
