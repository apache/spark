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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;

/**
 * An abstract class for the execution of a file system command
 */
abstract public class Command extends Configured {
  protected String[] args;
  
  /** Constructor */
  protected Command(Configuration conf) {
    super(conf);
  }
  
  /** Return the command's name excluding the leading character - */
  abstract public String getCommandName();
  
  /** 
   * Execute the command on the input path
   * 
   * @param path the input path
   * @throws IOException if any error occurs
   */
  abstract protected void run(Path path) throws IOException;
  
  /** 
   * For each source path, execute the command
   * 
   * @return 0 if it runs successfully; -1 if it fails
   */
  public int runAll() {
    int exitCode = 0;
    for (String src : args) {
      try {
        Path srcPath = new Path(src);
        FileSystem fs = srcPath.getFileSystem(getConf());
        FileStatus[] statuses = fs.globStatus(srcPath);
        if (statuses == null) {
          System.err.println("Can not find listing for " + src);
          exitCode = -1;
        } else {
          for(FileStatus s : statuses) {
            run(s.getPath());
          }
        }
      } catch (RemoteException re) {
        exitCode = -1;
        String content = re.getLocalizedMessage();
        int eol = content.indexOf('\n');
        if (eol>=0) {
          content = content.substring(0, eol);
        }
        System.err.println(getCommandName() + ": " + content);
      } catch (IOException e) {
        exitCode = -1;
        System.err.println(getCommandName() + ": " + e.getLocalizedMessage());
      }
    }
    return exitCode;
  }
}
