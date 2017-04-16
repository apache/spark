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

package org.apache.hadoop.io;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Random;

/** Unit tests for LargeUTF8. */
public class TestText extends TestCase {
  private static final int NUM_ITERATIONS = 100;
  public TestText(String name) { super(name); }

  private static final Random RANDOM = new Random(1);

  private static final int RAND_LEN = -1;
  
  // generate a valid java String
  private static String getTestString(int len) throws Exception {
    StringBuffer buffer = new StringBuffer();    
    int length = (len==RAND_LEN) ? RANDOM.nextInt(1000) : len;
    while (buffer.length()<length) {
      int codePoint = RANDOM.nextInt(Character.MAX_CODE_POINT);
      char tmpStr[] = new char[2];
      if (Character.isDefined(codePoint)) {
        //unpaired surrogate
        if (codePoint < Character.MIN_SUPPLEMENTARY_CODE_POINT &&
            !Character.isHighSurrogate((char)codePoint) &&
            !Character.isLowSurrogate((char)codePoint)) {
          Character.toChars(codePoint, tmpStr, 0);
          buffer.append(tmpStr);
        }
      }
    }
    return buffer.toString();
  }
  
  public static String getTestString() throws Exception {
    return getTestString(RAND_LEN);
  }
  
  public static String getLongString() throws Exception {
    String str = getTestString();
    int length = Short.MAX_VALUE+str.length();
    StringBuffer buffer = new StringBuffer();
    while(buffer.length()<length)
      buffer.append(str);
      
    return buffer.toString();
  }

  public void testWritable() throws Exception {
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      String str;
      if (i == 0)
        str = getLongString();
      else
        str = getTestString();
      TestWritable.testWritable(new Text(str));
    }
  }


  public void testCoding() throws Exception {
    String before = "Bad \t encoding \t testcase";
    Text text = new Text(before);
    String after = text.toString();
    assertTrue(before.equals(after));

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      // generate a random string
      if (i == 0)
        before = getLongString();
      else
        before = getTestString();
    
      // test string to utf8
      ByteBuffer bb = Text.encode(before);
          
      byte[] utf8Text = bb.array();
      byte[] utf8Java = before.getBytes("UTF-8");
      assertEquals(0, WritableComparator.compareBytes(
                                                      utf8Text, 0, bb.limit(),
                                                      utf8Java, 0, utf8Java.length));
              
      // test utf8 to string
      after = Text.decode(utf8Java);
      assertTrue(before.equals(after));
    }
  }
  
  
  public void testIO() throws Exception {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();

    for (int i = 0; i < NUM_ITERATIONS; i++) {
      // generate a random string
      String before;          
      if (i == 0)
        before = getLongString();
      else
        before = getTestString();
        
      // write it
      out.reset();
      Text.writeString(out, before);
        
      // test that it reads correctly
      in.reset(out.getData(), out.getLength());
      String after = Text.readString(in);
      assertTrue(before.equals(after));
        
      // Test compatibility with Java's other decoder 
      int strLenSize = WritableUtils.getVIntSize(Text.utf8Length(before));
      String after2 = new String(out.getData(), strLenSize, 
                                 out.getLength()-strLenSize, "UTF-8");
      assertTrue(before.equals(after2));
    }
  }

  public void testCompare() throws Exception {
    DataOutputBuffer out1 = new DataOutputBuffer();
    DataOutputBuffer out2 = new DataOutputBuffer();
    DataOutputBuffer out3 = new DataOutputBuffer();
    Text.Comparator comparator = new Text.Comparator();
    for (int i=0; i<NUM_ITERATIONS; i++) {
      // reset output buffer
      out1.reset();
      out2.reset();
      out3.reset();

      // generate two random strings
      String str1 = getTestString();
      String str2 = getTestString();
      if (i == 0) {
        str1 = getLongString();
        str2 = getLongString();
      } else {
        str1 = getTestString();
        str2 = getTestString();
      }
          
      // convert to texts
      Text txt1 = new Text(str1);
      Text txt2 = new Text(str2);
      Text txt3 = new Text(str1);
          
      // serialize them
      txt1.write(out1);
      txt2.write(out2);
      txt3.write(out3);
          
      // compare two strings by looking at their binary formats
      int ret1 = comparator.compare(out1.getData(), 0, out1.getLength(),
                                    out2.getData(), 0, out2.getLength());
      // compare two strings
      int ret2 = txt1.compareTo(txt2);
          
      assertEquals(ret1, ret2);
          
      // test equal
      assertEquals(txt1.compareTo(txt3), 0);
      assertEquals(comparator.compare(out1.getData(), 0, out3.getLength(),
                                      out3.getData(), 0, out3.getLength()), 0);
    }
  }
      
  public void testFind() throws Exception {
    Text text = new Text("abcd\u20acbdcd\u20ac");
    assertTrue(text.find("abd")==-1);
    assertTrue(text.find("ac")==-1);
    assertTrue(text.find("\u20ac")==4);
    assertTrue(text.find("\u20ac", 5)==11);
  }

  public void testFindAfterUpdatingContents() throws Exception {
    Text text = new Text("abcd");
    text.set("a".getBytes());
    assertEquals(text.getLength(),1);
    assertEquals(text.find("a"), 0);
    assertEquals(text.find("b"), -1);
  }

  public void testValidate() throws Exception {
    Text text = new Text("abcd\u20acbdcd\u20ac");
    byte [] utf8 = text.getBytes();
    int length = text.getLength();
    Text.validateUTF8(utf8, 0, length);
  }

  public void testTextText() throws CharacterCodingException {
    Text a=new Text("abc");
    Text b=new Text("a");
    b.set(a);
    assertEquals("abc", b.toString());
    a.append("xdefgxxx".getBytes(), 1, 4);
    assertEquals("modified aliased string", "abc", b.toString());
    assertEquals("appended string incorrectly", "abcdefg", a.toString());
  }
  
  private class ConcurrentEncodeDecodeThread extends Thread {
    public ConcurrentEncodeDecodeThread(String name) {
      super(name);
    }

    public void run() {
      String name = this.getName();
      DataOutputBuffer out = new DataOutputBuffer();
      DataInputBuffer in = new DataInputBuffer();
      for (int i=0; i < 1000; ++i) {
        try {
          out.reset();
          WritableUtils.writeString(out, name);
          
          in.reset(out.getData(), out.getLength());
          String s = WritableUtils.readString(in);
          
          assertEquals(name, s);
        } catch (Exception ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }
  
  public void testConcurrentEncodeDecode() throws Exception{
    Thread thread1 = new ConcurrentEncodeDecodeThread("apache");
    Thread thread2 = new ConcurrentEncodeDecodeThread("hadoop");
    
    thread1.start();
    thread2.start();
    
    thread2.join();
    thread2.join();
  }

  public static void main(String[] args)  throws Exception
  {
    TestText test = new TestText("main");
    test.testIO();
    test.testCompare();
    test.testCoding();
    test.testWritable();
    test.testFind();
    test.testValidate();
  }
}
