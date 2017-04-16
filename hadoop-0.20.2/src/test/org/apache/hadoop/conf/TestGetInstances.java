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
package org.apache.hadoop.conf;

import java.util.List;

import junit.framework.TestCase;

public class TestGetInstances extends TestCase {
  
  interface SampleInterface {}
  
  interface ChildInterface extends SampleInterface {}
  
  static class SampleClass implements SampleInterface {
    SampleClass() {}
  }
	
  static class AnotherClass implements ChildInterface {
    AnotherClass() {}
  }
  
  /**
   * Makes sure <code>Configuration.getInstances()</code> returns
   * instances of the required type.
   */
  public void testGetInstances() throws Exception {
    Configuration conf = new Configuration();
    
    List<SampleInterface> classes =
      conf.getInstances("no.such.property", SampleInterface.class);
    assertTrue(classes.isEmpty());

    conf.set("empty.property", "");
    classes = conf.getInstances("empty.property", SampleInterface.class);
    assertTrue(classes.isEmpty());
    
    conf.setStrings("some.classes",
        SampleClass.class.getName(), AnotherClass.class.getName());
    classes = conf.getInstances("some.classes", SampleInterface.class);
    assertEquals(2, classes.size());
    
    try {
      conf.setStrings("some.classes",
          SampleClass.class.getName(), AnotherClass.class.getName(),
          String.class.getName());
      conf.getInstances("some.classes", SampleInterface.class);
      fail("java.lang.String does not implement SampleInterface");
    } catch (RuntimeException e) {}
    
    try {
      conf.setStrings("some.classes",
          SampleClass.class.getName(), AnotherClass.class.getName(),
          "no.such.Class");
      conf.getInstances("some.classes", SampleInterface.class);
      fail("no.such.Class does not exist");
    } catch (RuntimeException e) {}
  }
}
