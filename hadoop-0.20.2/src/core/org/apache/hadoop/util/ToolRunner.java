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
package org.apache.hadoop.util;

import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;

/**
 * A utility to help run {@link Tool}s.
 * 
 * <p><code>ToolRunner</code> can be used to run classes implementing 
 * <code>Tool</code> interface. It works in conjunction with 
 * {@link GenericOptionsParser} to parse the 
 * <a href="{@docRoot}/org/apache/hadoop/util/GenericOptionsParser.html#GenericOptions">
 * generic hadoop command line arguments</a> and modifies the 
 * <code>Configuration</code> of the <code>Tool</code>. The 
 * application-specific options are passed along without being modified.
 * </p>
 * 
 * @see Tool
 * @see GenericOptionsParser
 */
public class ToolRunner {
 
  /**
   * Runs the given <code>Tool</code> by {@link Tool#run(String[])}, after 
   * parsing with the given generic arguments. Uses the given 
   * <code>Configuration</code>, or builds one if null.
   * 
   * Sets the <code>Tool</code>'s configuration with the possibly modified 
   * version of the <code>conf</code>.  
   * 
   * @param conf <code>Configuration</code> for the <code>Tool</code>.
   * @param tool <code>Tool</code> to run.
   * @param args command-line arguments to the tool.
   * @return exit code of the {@link Tool#run(String[])} method.
   */
  public static int run(Configuration conf, Tool tool, String[] args) 
    throws Exception{
    if(conf == null) {
      conf = new Configuration();
    }
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    //set the configuration back, so that Tool can configure itself
    tool.setConf(conf);
    
    //get the args w/o generic hadoop args
    String[] toolArgs = parser.getRemainingArgs();
    return tool.run(toolArgs);
  }
  
  /**
   * Runs the <code>Tool</code> with its <code>Configuration</code>.
   * 
   * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
   * 
   * @param tool <code>Tool</code> to run.
   * @param args command-line arguments to the tool.
   * @return exit code of the {@link Tool#run(String[])} method.
   */
  public static int run(Tool tool, String[] args) 
    throws Exception{
    return run(tool.getConf(), tool, args);
  }
  
  /**
   * Prints generic command-line argurments and usage information.
   * 
   *  @param out stream to write usage information to.
   */
  public static void printGenericCommandUsage(PrintStream out) {
    GenericOptionsParser.printGenericCommandUsage(out);
  }
  
}
