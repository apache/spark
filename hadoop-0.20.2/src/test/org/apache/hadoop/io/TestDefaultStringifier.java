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

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class TestDefaultStringifier extends TestCase {

  private static Configuration conf = new Configuration();
  private static final Log LOG = LogFactory.getLog(TestDefaultStringifier.class);

  private char[] alphabet = "abcdefghijklmnopqrstuvwxyz".toCharArray();

  public void testWithWritable() throws Exception {

    conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");

    LOG.info("Testing DefaultStringifier with Text");

    Random random = new Random();

    //test with a Text
    for(int i=0;i<10;i++) {
      //generate a random string
      StringBuilder builder = new StringBuilder();
      int strLen = random.nextInt(40);
      for(int j=0; j< strLen; j++) {
        builder.append(alphabet[random.nextInt(alphabet.length)]);
      }
      Text text = new Text(builder.toString());
      DefaultStringifier<Text> stringifier = new DefaultStringifier<Text>(conf, Text.class);

      String str = stringifier.toString(text);
      Text claimedText = stringifier.fromString(str);
      LOG.info("Object: " + text);
      LOG.info("String representation of the object: " + str);
      assertEquals(text, claimedText);
    }
  }

  public void testWithJavaSerialization() throws Exception {
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");

    LOG.info("Testing DefaultStringifier with Serializable Integer");

    //Integer implements Serializable
    Integer testInt = Integer.valueOf(42);
    DefaultStringifier<Integer> stringifier = new DefaultStringifier<Integer>(conf, Integer.class);

    String str = stringifier.toString(testInt);
    Integer claimedInt = stringifier.fromString(str);
    LOG.info("String representation of the object: " + str);

    assertEquals(testInt, claimedInt);
  }

  public void testStoreLoad() throws IOException {

    LOG.info("Testing DefaultStringifier#store() and #load()");
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.WritableSerialization");
    Text text = new Text("uninteresting test string");
    String keyName = "test.defaultstringifier.key1";

    DefaultStringifier.store(conf,text, keyName);

    Text claimedText = DefaultStringifier.load(conf, keyName, Text.class);
    assertEquals("DefaultStringifier#load() or #store() might be flawed"
        , text, claimedText);

  }

  public void testStoreLoadArray() throws IOException {
    LOG.info("Testing DefaultStringifier#storeArray() and #loadArray()");
    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization");

    String keyName = "test.defaultstringifier.key2";

    Integer[] array = new Integer[] {1,2,3,4,5};


    DefaultStringifier.storeArray(conf, array, keyName);

    Integer[] claimedArray = DefaultStringifier.<Integer>loadArray(conf, keyName, Integer.class);
    for (int i = 0; i < array.length; i++) {
      assertEquals("two arrays are not equal", array[i], claimedArray[i]);
    }

  }

}
