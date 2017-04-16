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
package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class TestMultiFileSplit extends TestCase{

    public void testReadWrite() throws Exception {
      MultiFileSplit split = new MultiFileSplit(new JobConf(), new Path[] {new Path("/test/path/1"), new Path("/test/path/2")}, new long[] {100,200});
        
      ByteArrayOutputStream bos = null;
      byte[] result = null;
      try {    
        bos = new ByteArrayOutputStream();
        split.write(new DataOutputStream(bos));
        result = bos.toByteArray();
      } finally {
        IOUtils.closeStream(bos);
      }
      
      MultiFileSplit readSplit = new MultiFileSplit();
      ByteArrayInputStream bis = null;
      try {
        bis = new ByteArrayInputStream(result);
        readSplit.readFields(new DataInputStream(bis));
      } finally {
        IOUtils.closeStream(bis);
      }
      
      assertTrue(split.getLength() != 0);
      assertEquals(split.getLength(), readSplit.getLength());
      assertTrue(Arrays.equals(split.getPaths(), readSplit.getPaths()));
      assertTrue(Arrays.equals(split.getLengths(), readSplit.getLengths()));
      System.out.println(split.toString());
    }
}
