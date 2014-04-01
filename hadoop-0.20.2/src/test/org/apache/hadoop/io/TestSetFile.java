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
import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;

/** Support for flat files of binary key/value pairs. */
public class TestSetFile extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestSetFile.class);
  private static String FILE =
    System.getProperty("test.build.data",".") + "/test.set";

  private static Configuration conf = new Configuration();
  
  public TestSetFile(String name) { super(name); }

  public void testSetFile() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    try {
      RandomDatum[] data = generate(10000);
      writeTest(fs, data, FILE, CompressionType.NONE);
      readTest(fs, data, FILE);

      writeTest(fs, data, FILE, CompressionType.BLOCK);
      readTest(fs, data, FILE);
    } finally {
      fs.close();
    }
  }

  private static RandomDatum[] generate(int count) {
    LOG.info("generating " + count + " records in memory");
    RandomDatum[] data = new RandomDatum[count];
    RandomDatum.Generator generator = new RandomDatum.Generator();
    for (int i = 0; i < count; i++) {
      generator.next();
      data[i] = generator.getValue();
    }
    LOG.info("sorting " + count + " records");
    Arrays.sort(data);
    return data;
  }

  private static void writeTest(FileSystem fs, RandomDatum[] data,
                                String file, CompressionType compress)
    throws IOException {
    MapFile.delete(fs, file);
    LOG.info("creating with " + data.length + " records");
    SetFile.Writer writer =
      new SetFile.Writer(conf, fs, file,
                         WritableComparator.get(RandomDatum.class),
                         compress);
    for (int i = 0; i < data.length; i++)
      writer.append(data[i]);
    writer.close();
  }

  private static void readTest(FileSystem fs, RandomDatum[] data, String file)
    throws IOException {
    RandomDatum v = new RandomDatum();
    int sample = (int)Math.sqrt(data.length);
    Random random = new Random();
    LOG.info("reading " + sample + " records");
    SetFile.Reader reader = new SetFile.Reader(fs, file, conf);
    for (int i = 0; i < sample; i++) {
      if (!reader.seek(data[random.nextInt(data.length)]))
        throw new RuntimeException("wrong value at " + i);
    }
    reader.close();
    LOG.info("done reading " + data.length);
  }


  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    boolean create = true;
    boolean check = true;
    String file = FILE;
    String compress = "NONE";

    String usage = "Usage: TestSetFile [-count N] [-nocreate] [-nocheck] [-compress type] file";
      
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    int i = 0;
    Path fpath=null;
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
        } else if (args[i].equals("-compress")) {
          compress = args[++i];
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
      LOG.info("compress = " + compress);
      LOG.info("file = " + file);
      
      RandomDatum[] data = generate(count);
      
      if (create) {
        writeTest(fs, data, file, CompressionType.valueOf(compress));
      }
      
      if (check) {
        readTest(fs, data, file);
      }
  
    } finally {
      fs.close();
    }
  }
}
