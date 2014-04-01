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
package org.apache.hadoop.tools;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;

/**
 * An abstract class for distributed tool for file related operations.
 */
abstract class DistTool implements org.apache.hadoop.util.Tool {
  protected static final Log LOG = LogFactory.getLog(DistTool.class);

  protected JobConf jobconf;

  /** {@inheritDoc} */
  public void setConf(Configuration conf) {
    if (jobconf != conf) {
      jobconf = conf instanceof JobConf? (JobConf)conf: new JobConf(conf);
    }
  }

  /** {@inheritDoc} */
  public JobConf getConf() {return jobconf;}

  protected DistTool(Configuration conf) {setConf(conf);}

  private static final Random RANDOM = new Random();
  protected static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }

  /** Sanity check for source */
  protected static void checkSource(Configuration conf, List<Path> srcs
      ) throws InvalidInputException {
    List<IOException> ioes = new ArrayList<IOException>();
    for(Path p : srcs) {
      try {
        if (!p.getFileSystem(conf).exists(p)) {
          ioes.add(new FileNotFoundException("Source "+p+" does not exist."));
        }
      }
      catch(IOException e) {ioes.add(e);}
    }
    if (!ioes.isEmpty()) {
      throw new InvalidInputException(ioes);
    }
  }

  protected static String readString(DataInput in) throws IOException {
    if (in.readBoolean()) {
      return Text.readString(in);
    }
    return null;
  }

  protected static void writeString(DataOutput out, String s
      ) throws IOException {
    boolean b = s != null;
    out.writeBoolean(b);
    if (b) {Text.writeString(out, s);}
  }

  protected static List<String> readFile(Configuration conf, Path inputfile
      ) throws IOException {
    List<String> result = new ArrayList<String>();
    FileSystem fs = inputfile.getFileSystem(conf);
    BufferedReader input = null;
    try {
      input = new BufferedReader(new InputStreamReader(fs.open(inputfile)));
      for(String line; (line = input.readLine()) != null;) {
        result.add(line);
      }
    } finally {
      input.close();
    }
    return result;
  }

  /** An exception class for duplicated source files. */
  public static class DuplicationException extends IOException {
    private static final long serialVersionUID = 1L;
    /** Error code for this exception */
    public static final int ERROR_CODE = -2;
    DuplicationException(String message) {super(message);}
  }
}