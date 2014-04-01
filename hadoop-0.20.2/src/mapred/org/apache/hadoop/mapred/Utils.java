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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A utility class. It provides
 *   - file-util
 *     - A path filter utility to filter out output/part files in the output dir
 */
public class Utils {
  public static class OutputFileUtils {
    /**
     * This class filters output(part) files from the given directory
     * It does not accept files with filenames _logs and _SUCCESS.
     * This can be used to list paths of output directory as follows:
     *   Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
     *                                         new OutputFilesFilter()));
     */
    public static class OutputFilesFilter extends OutputLogFilter {
      public boolean accept(Path path) {
        return super.accept(path) 
               && !FileOutputCommitter.SUCCEEDED_FILE_NAME
                   .equals(path.getName());
      }
    }
    
    /**
     * This class filters log files from directory given
     * It doesnt accept paths having _logs.
     * This can be used to list paths of output directory as follows:
     *   Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
     *                                   new OutputLogFilter()));
     */
    public static class OutputLogFilter implements PathFilter {
      public boolean accept(Path path) {
        return !(path.toString().contains("_logs"));
      }
    }
  }
}
