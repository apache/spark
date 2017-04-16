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

import java.io.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

/** Support for flat files of binary key/value pairs. */
public class TestArrayFile extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestArrayFile.class);
  private static String FILE =
    System.getProperty("test.build.data",".") + "/test.array";

  public TestArrayFile(String name) { 
    super(name); 
  }

  public void testArrayFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    RandomDatum[] data = generate(10000);
    writeTest(fs, data, FILE);
    readTest(fs, data, FILE, conf);
  }

  public void testEmptyFile() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    writeTest(fs, new RandomDatum[0], FILE);
    ArrayFile.Reader reader = new ArrayFile.Reader(fs, FILE, conf);
    assertNull(reader.get(0, new RandomDatum()));
    reader.close();
  }

  private static RandomDatum[] generate(int count) {
    LOG.debug("generating " + count + " records in debug");
    RandomDatum[] data = new RandomDatum[count];
    RandomDatum.Generator generator = new RandomDatum.Generator();
    for (int i = 0; i < count; i++) {
      generator.next();
      data[i] = generator.getValue();
    }
    return data;
  }

  private static void writeTest(FileSystem fs, RandomDatum[] data, String file)
    throws IOException {
    Configuration conf = new Configuration();
    MapFile.delete(fs, file);
    LOG.debug("creating with " + data.length + " debug");
    ArrayFile.Writer writer = new ArrayFile.Writer(conf, fs, file, RandomDatum.class);
    writer.setIndexInterval(100);
    for (int i = 0; i < data.length; i++)
      writer.append(data[i]);
    writer.close();
  }

  private static void readTest(FileSystem fs, RandomDatum[] data, String file, Configuration conf)
    throws IOException {
    RandomDatum v = new RandomDatum();
    LOG.debug("reading " + data.length + " debug");
    ArrayFile.Reader reader = new ArrayFile.Reader(fs, file, conf);
    for (int i = 0; i < data.length; i++) {       // try forwards
      reader.get(i, v);
      if (!v.equals(data[i])) {
        throw new RuntimeException("wrong value at " + i);
      }
    }
    for (int i = data.length-1; i >= 0; i--) {    // then backwards
      reader.get(i, v);
      if (!v.equals(data[i])) {
        throw new RuntimeException("wrong value at " + i);
      }
    }
    reader.close();
    LOG.debug("done reading " + data.length + " debug");
  }


  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    boolean create = true;
    boolean check = true;
    String file = FILE;
    String usage = "Usage: TestArrayFile [-count N] [-nocreate] [-nocheck] file";
      
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    int i = 0;
    Path fpath = null;
    FileSystem fs = null;
    try {
      for (; i < args.length; i++) {       // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-nocreate")) {
          create = false;
        } else if (args[i].equals("-nocheck")) {
          check = false;
        } else {                                       
          // file is required parameter
          file = args[i];
          fpath=new Path(file);
        }
      }
        
      fs = fpath.getFileSystem(conf);
        
      LOG.info("count = " + count);
      LOG.info("create = " + create);
      LOG.info("check = " + check);
      LOG.info("file = " + file);

      RandomDatum[] data = generate(count);

      if (create) {
        writeTest(fs, data, file);
      }

      if (check) {
        readTest(fs, data, file, conf);
      }
    } finally {
      fs.close();
    }
  }
}
