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
package org.apache.hadoop.fs.shell;

import java.io.*;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Count the number of directories, files, bytes, quota, and remaining quota.
 */
public class Count extends Command {
  public static final String NAME = "count";
  public static final String USAGE = "-" + NAME + "[-q] <path>";
  public static final String DESCRIPTION = CommandUtils.formatDescription(USAGE, 
      "Count the number of directories, files and bytes under the paths",
      "that match the specified file pattern.  The output columns are:",
      "DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME or",
      "QUOTA REMAINING_QUATA SPACE_QUOTA REMAINING_SPACE_QUOTA ",
      "      DIR_COUNT FILE_COUNT CONTENT_SIZE FILE_NAME");
  
  private boolean qOption;

  /** Constructor
   * 
   * @param cmd the count command
   * @param pos the starting index of the arguments 
   */
  public Count(String[] cmd, int pos, Configuration conf) {
    super(conf);
    CommandFormat c = new CommandFormat(NAME, 1, Integer.MAX_VALUE, "q");
    List<String> parameters = c.parse(cmd, pos);
    this.args = parameters.toArray(new String[parameters.size()]);
    if (this.args.length == 0) { // default path is the current working directory
      this.args = new String[] {"."};
    }
    this.qOption = c.getOpt("q") ? true: false;
  }
  
  /** Check if a command is the count command
   * 
   * @param cmd A string representation of a command starting with "-"
   * @return true if this is a count command; false otherwise
   */
  public static boolean matches(String cmd) {
    return ("-" + NAME).equals(cmd); 
  }

  @Override
  public String getCommandName() {
    return NAME;
  }

  @Override
  protected void run(Path path) throws IOException {
    FileSystem fs = path.getFileSystem(getConf());
    System.out.println(fs.getContentSummary(path).toString(qOption) + path);
  }
}
