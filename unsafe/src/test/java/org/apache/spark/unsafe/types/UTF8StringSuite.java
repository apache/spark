/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.unsafe.types;

import java.io.UnsupportedEncodingException;

import junit.framework.Assert;
import org.junit.Test;

public class UTF8StringSuite {

  private void checkBasic(String str, int len) throws UnsupportedEncodingException {
    Assert.assertEquals(UTF8String.fromString(str).length(), len);
    Assert.assertEquals(UTF8String.fromBytes(str.getBytes("utf8")).length(), len);

    Assert.assertEquals(UTF8String.fromString(str), str);
    Assert.assertEquals(UTF8String.fromBytes(str.getBytes("utf8")), str);
    Assert.assertEquals(UTF8String.fromString(str).toString(), str);
    Assert.assertEquals(UTF8String.fromBytes(str.getBytes("utf8")).toString(), str);
    Assert.assertEquals(UTF8String.fromBytes(str.getBytes("utf8")), UTF8String.fromString(str));

    Assert.assertEquals(UTF8String.fromString(str).hashCode(),
      UTF8String.fromBytes(str.getBytes("utf8")).hashCode());
  }

  @Test
  public void basicTest() throws UnsupportedEncodingException {
    checkBasic("hello", 5);
    checkBasic("世 界", 3);
  }

  @Test
  public void contains() {
    Assert.assertFalse(UTF8String.fromString("hello").contains(null));
    Assert.assertTrue(UTF8String.fromString("hello").contains(UTF8String.fromString("ello")));
    Assert.assertFalse(UTF8String.fromString("hello").contains(UTF8String.fromString("vello")));
    Assert.assertFalse(UTF8String.fromString("hello").contains(UTF8String.fromString("hellooo")));
    Assert.assertTrue(UTF8String.fromString("大千世界").contains(UTF8String.fromString("千世")));
    Assert.assertFalse(UTF8String.fromString("大千世界").contains(UTF8String.fromString("世千")));
    Assert.assertFalse(
      UTF8String.fromString("大千世界").contains(UTF8String.fromString("大千世界好")));
  }

  @Test
  public void startsWith() {
    Assert.assertFalse(UTF8String.fromString("hello").startsWith(null));
    Assert.assertTrue(UTF8String.fromString("hello").startsWith(UTF8String.fromString("hell")));
    Assert.assertFalse(UTF8String.fromString("hello").startsWith(UTF8String.fromString("ell")));
    Assert.assertFalse(UTF8String.fromString("hello").startsWith(UTF8String.fromString("hellooo")));
    Assert.assertTrue(UTF8String.fromString("数据砖头").startsWith(UTF8String.fromString("数据")));
    Assert.assertFalse(UTF8String.fromString("大千世界").startsWith(UTF8String.fromString("千")));
    Assert.assertFalse(
      UTF8String.fromString("大千世界").startsWith(UTF8String.fromString("大千世界好")));
  }

  @Test
  public void endsWith() {
    Assert.assertFalse(UTF8String.fromString("hello").endsWith(null));
    Assert.assertTrue(UTF8String.fromString("hello").endsWith(UTF8String.fromString("ello")));
    Assert.assertFalse(UTF8String.fromString("hello").endsWith(UTF8String.fromString("ellov")));
    Assert.assertFalse(UTF8String.fromString("hello").endsWith(UTF8String.fromString("hhhello")));
    Assert.assertTrue(UTF8String.fromString("大千世界").endsWith(UTF8String.fromString("世界")));
    Assert.assertFalse(UTF8String.fromString("大千世界").endsWith(UTF8String.fromString("世")));
    Assert.assertFalse(
      UTF8String.fromString("数据砖头").endsWith(UTF8String.fromString("我的数据砖头")));
  }

  @Test
  public void substring() {
    Assert.assertEquals(
      UTF8String.fromString("hello").substring(0, 0), UTF8String.fromString(""));
    Assert.assertEquals(
      UTF8String.fromString("hello").substring(1, 3), UTF8String.fromString("el"));
    Assert.assertEquals(
      UTF8String.fromString("数据砖头").substring(0, 1), UTF8String.fromString("数"));
    Assert.assertEquals(
      UTF8String.fromString("数据砖头").substring(1, 3), UTF8String.fromString("据砖"));
    Assert.assertEquals(
      UTF8String.fromString("数据砖头").substring(3, 5), UTF8String.fromString("头"));
  }
}
