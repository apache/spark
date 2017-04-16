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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.gridmix.Gridmix;
import org.apache.hadoop.conf.Configuration;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.OutputStream;

/**
 * Gridmix utilities.
 */
public class UtilsForGridmix {
  private static final Log LOG = LogFactory.getLog(UtilsForGridmix.class);

  /**
   * cleanup the folder or file.
   * @param path - folder or file path.
   * @param conf - cluster configuration 
   * @throws IOException - If an I/O error occurs.
   */
  public static void cleanup(Path path, Configuration conf) 
     throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
    fs.close();
  }

  /**
   * Get the login user.
   * @return - login user as string..
   * @throws IOException - if an I/O error occurs.
   */
  public static String getUserName() throws IOException {
    return UserGroupInformation.getLoginUser().getUserName();
  }
  
  /**
   * Get the argument list for gridmix job.
   * @param gridmixDir - gridmix parent directory.
   * @param gridmixRunMode - gridmix modes either 1,2,3.
   * @param values - gridmix runtime values.
   * @param otherArgs - gridmix other generic args.
   * @return - argument list as string array.
   */
  public static String [] getArgsList(Path gridmixDir, int gridmixRunMode, 
    String [] values, String [] otherArgs) {
    
    String [] runtimeArgs = {
       "-D", GridMixConfig.GRIDMIX_LOG_MODE + 
       "=DEBUG",
       "-D", GridMixConfig.GRIDMIX_OUTPUT_DIR + 
       "=" + new Path(gridmixDir,"gridmix").toString(),
       "-D", GridMixConfig.GRIDMIX_JOB_SUBMISSION_QUEUE_IN_TRACE 
       + "=true",
       "-D", GridMixConfig.GRIDMIX_JOB_TYPE 
       + "=" + values[0],
       "-D", GridMixConfig.GRIDMIX_USER_RESOLVER + 
       "=" + values[1],
       "-D", GridMixConfig.GRIDMIX_SUBMISSION_POLICY + 
       "=" + values[2]
    };
    String [] classArgs;
    if ((gridmixRunMode == GridMixRunMode.DATA_GENERATION || 
      gridmixRunMode == GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX) && 
      values[1].indexOf("RoundRobinUserResolver") > 0) {
     classArgs = new String[]{
        "-generate", values[3], 
        "-users", values[4], 
        new Path(gridmixDir,"input").toString(), 
        values[5]};
    } else if (gridmixRunMode == GridMixRunMode.DATA_GENERATION ||
       gridmixRunMode == GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX){
      classArgs = new String[]{
         "-generate", values[3], new Path(gridmixDir,"input").toString(),
         values[4]};
    } else if(gridmixRunMode == GridMixRunMode.RUN_GRIDMIX 
       && values[1].indexOf("RoundRobinUserResolver") > 0) {
      classArgs = new String[]{         
         "-users", values[3], 
         new Path(gridmixDir,"input").toString(),
         values[4]};
    } else {
      classArgs = new String[]{
         new Path(gridmixDir,"input").toString(),values[3]};
    }
    
    String [] args = new String [runtimeArgs.length + 
       classArgs.length + ((otherArgs != null)?otherArgs.length:0)];
    System.arraycopy(runtimeArgs, 0, args, 0, runtimeArgs.length);
    if (otherArgs !=null) {
      System.arraycopy(otherArgs, 0, args, runtimeArgs.length, 
         otherArgs.length);
      System.arraycopy(classArgs, 0, args, (runtimeArgs.length + 
         otherArgs.length), classArgs.length);
    } else {
      System.arraycopy(classArgs, 0, args, runtimeArgs.length, 
         classArgs.length);
    }
    return args;
  }
  
  /**
   * Create a file with specified size in mb.
   * @param sizeInMB - file size in mb.
   * @param inputDir - input directory.
   * @param conf - cluster configuration.
   * @throws Exception - if an exception occurs.
   */
  public static void createFile(int sizeInMB, Path inputDir, 
     Configuration conf) throws Exception {
    Date d = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyy_HHmmssS");
    String formatDate = sdf.format(d);
    FileSystem fs = inputDir.getFileSystem(conf);
    OutputStream out = fs.create(new Path(inputDir,"datafile_" + formatDate));
    final byte[] b = new byte[1024 * 1024];
    for (int index = 0; index < sizeInMB; index++) {
       out.write(b);
    }    
    out.close();
    fs.close();
  }
  
  /**
   * Create directories for a path.
   * @param path - directories path.
   * @param conf  - cluster configuration.
   * @throws IOException  - if an I/O error occurs.
   */
  public static void createDirs(Path path,Configuration conf) 
     throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
       fs.mkdirs(path);
    }
  }
  
  /**
   * Run the Gridmix job with given runtime arguments.
   * @param gridmixDir - Gridmix parent directory.
   * @param conf - cluster configuration.
   * @param gridmixRunMode - gridmix run mode either 1,2,3
   * @param runtimeValues -gridmix runtime values.
   * @return - gridmix status either 0 or 1.
   * @throws Exception
   */
  public static int runGridmixJob(Path gridmixDir, Configuration conf, 
     int gridmixRunMode, String [] runtimeValues) throws Exception {
    return runGridmixJob(gridmixDir, conf, gridmixRunMode, runtimeValues, null);
  }
  /**
   * Run the Gridmix job with given runtime arguments.
   * @param gridmixDir - Gridmix parent directory
   * @param conf - cluster configuration.
   * @param gridmixRunMode - gridmix run mode.
   * @param runtimeValues - gridmix runtime values.
   * @param otherArgs - gridmix other generic args.
   * @return - gridmix status either 0 or 1.
   * @throws Exception
   */
  
  public static int runGridmixJob(Path gridmixDir, Configuration conf, 
     int gridmixRunMode, String [] runtimeValues, 
     String [] otherArgs) throws Exception {
    Path  outputDir = new Path(gridmixDir, "gridmix");
    Path inputDir = new Path(gridmixDir, "input");
    LOG.info("Cleanup the data if data already exists.");
    switch (gridmixRunMode) {
      case GridMixRunMode.DATA_GENERATION :
        cleanup(inputDir, conf);
        cleanup(outputDir, conf);
        break;
      case GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX :
        cleanup(inputDir, conf);
        cleanup(outputDir, conf);
        break;
      case GridMixRunMode.RUN_GRIDMIX :
        cleanup(outputDir, conf);
        break;
    }

    final String [] args = UtilsForGridmix.getArgsList(gridmixDir,
       gridmixRunMode, runtimeValues, otherArgs);
    Gridmix gridmix = new Gridmix();
    LOG.info("Submit a Gridmix job in " + runtimeValues[1] + 
    " mode for " + GridMixRunMode.getMode(gridmixRunMode));
    int exitCode = ToolRunner.run(conf, gridmix, args);
    return exitCode;
  }
}
