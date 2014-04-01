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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

public class TestSequenceFileSerialization extends TestCase {
  
  private Configuration conf;
  private FileSystem fs;
  
  @Override
  protected void setUp() throws Exception {
    conf = new Configuration();
    conf.set("io.serializations",
        "org.apache.hadoop.io.serializer.JavaSerialization");
    fs = FileSystem.getLocal(conf);  
  }
  
  @Override
  protected void tearDown() throws Exception {
    fs.close();
  }
  
  public void testJavaSerialization() throws Exception {
    Path file = new Path(System.getProperty("test.build.data",".") +
        "/test.seq");
    
    fs.delete(file, true);
    Writer writer = SequenceFile.createWriter(fs, conf, file, Long.class,
        String.class);
    
    writer.append(1L, "one");
    writer.append(2L, "two");
    
    writer.close();
    
    Reader reader = new Reader(fs, file, conf);
    assertEquals(1L, reader.next((Object) null));
    assertEquals("one", reader.getCurrentValue((Object) null));
    assertEquals(2L, reader.next((Object) null));
    assertEquals("two", reader.getCurrentValue((Object) null));
    assertNull(reader.next((Object) null));
    reader.close();
    
  }
}
