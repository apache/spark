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

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Progressable;

/**
 * This test exercises the ValueIterator.
 */
public class TestReduceTask extends TestCase {

  static class NullProgress implements Progressable {
    public void progress() { }
  }

  private static class Pair {
    String key;
    String value;
    Pair(String k, String v) {
      key = k;
      value = v;
    }
  }
  private static Pair[][] testCases =
    new Pair[][]{
      new Pair[]{
                 new Pair("k1", "v1"),
                 new Pair("k2", "v2"),
                 new Pair("k3", "v3"),
                 new Pair("k3", "v4"),
                 new Pair("k4", "v5"),
                 new Pair("k5", "v6"),
      },
      new Pair[]{
                 new Pair("", "v1"),
                 new Pair("k1", "v2"),
                 new Pair("k2", "v3"),
                 new Pair("k2", "v4"),
      },
      new Pair[] {},
      new Pair[]{
                 new Pair("k1", "v1"),
                 new Pair("k1", "v2"),
                 new Pair("k1", "v3"),
                 new Pair("k1", "v4"),
      }
    };
  
  public void runValueIterator(Path tmpDir, Pair[] vals, 
                               Configuration conf, 
                               CompressionCodec codec) throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    FileSystem rfs = ((LocalFileSystem)localFs).getRaw();
    Path path = new Path(tmpDir, "data.in");
    IFile.Writer<Text, Text> writer = 
      new IFile.Writer<Text, Text>(conf, rfs, path, Text.class, Text.class,
                                   codec, null);
    for(Pair p: vals) {
      writer.append(new Text(p.key), new Text(p.value));
    }
    writer.close();
    
    @SuppressWarnings("unchecked")
    RawKeyValueIterator rawItr = 
      Merger.merge(conf, rfs, Text.class, Text.class, codec, new Path[]{path}, 
                   false, conf.getInt("io.sort.factor", 100), tmpDir, 
                   new Text.Comparator(), new NullProgress(),null,null);
    @SuppressWarnings("unchecked") // WritableComparators are not generic
    ReduceTask.ValuesIterator valItr = 
      new ReduceTask.ValuesIterator<Text,Text>(rawItr,
          WritableComparator.get(Text.class), Text.class, Text.class,
          conf, new NullProgress());
    int i = 0;
    while (valItr.more()) {
      Object key = valItr.getKey();
      String keyString = key.toString();
      // make sure it matches!
      assertEquals(vals[i].key, keyString);
      // must have at least 1 value!
      assertTrue(valItr.hasNext());
      while (valItr.hasNext()) {
        String valueString = valItr.next().toString();
        // make sure the values match
        assertEquals(vals[i].value, valueString);
        // make sure the keys match
        assertEquals(vals[i].key, valItr.getKey().toString());
        i += 1;
      }
      // make sure the key hasn't changed under the hood
      assertEquals(keyString, valItr.getKey().toString());
      valItr.nextKey();
    }
    assertEquals(vals.length, i);
    // make sure we have progress equal to 1.0
    assertEquals(1.0f, rawItr.getProgress().get());
  }

  public void testValueIterator() throws Exception {
    Path tmpDir = new Path("build/test/test.reduce.task");
    Configuration conf = new Configuration();
    for (Pair[] testCase: testCases) {
      runValueIterator(tmpDir, testCase, conf, null);
    }
  }
  
  public void testValueIteratorWithCompression() throws Exception {
    Path tmpDir = new Path("build/test/test.reduce.task.compression");
    Configuration conf = new Configuration();
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(conf);
    for (Pair[] testCase: testCases) {
      runValueIterator(tmpDir, testCase, conf, codec);
    }
  }
}
