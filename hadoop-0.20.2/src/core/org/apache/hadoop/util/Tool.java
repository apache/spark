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

import org.apache.hadoop.conf.Configurable;

/**
 * A tool interface that supports handling of generic command-line options.
 * 
 * <p><code>Tool</code>, is the standard for any Map-Reduce tool/application. 
 * The tool/application should delegate the handling of 
 * <a href="{@docRoot}/org/apache/hadoop/util/GenericOptionsParser.html#GenericOptions">
 * standard command-line options</a> to {@link ToolRunner#run(Tool, String[])} 
 * and only handle its custom arguments.</p>
 * 
 * <p>Here is how a typical <code>Tool</code> is implemented:</p>
 * <p><blockquote><pre>
 *     public class MyApp extends Configured implements Tool {
 *     
 *       public int run(String[] args) throws Exception {
 *         // <code>Configuration</code> processed by <code>ToolRunner</code>
 *         Configuration conf = getConf();
 *         
 *         // Create a JobConf using the processed <code>conf</code>
 *         JobConf job = new JobConf(conf, MyApp.class);
 *         
 *         // Process custom command-line options
 *         Path in = new Path(args[1]);
 *         Path out = new Path(args[2]);
 *         
 *         // Specify various job-specific parameters     
 *         job.setJobName("my-app");
 *         job.setInputPath(in);
 *         job.setOutputPath(out);
 *         job.setMapperClass(MyApp.MyMapper.class);
 *         job.setReducerClass(MyApp.MyReducer.class);
 *
 *         // Submit the job, then poll for progress until the job is complete
 *         JobClient.runJob(job);
 *       }
 *       
 *       public static void main(String[] args) throws Exception {
 *         // Let <code>ToolRunner</code> handle generic command-line options 
 *         int res = ToolRunner.run(new Configuration(), new Sort(), args);
 *         
 *         System.exit(res);
 *       }
 *     }
 * </pre></blockquote></p>
 * 
 * @see GenericOptionsParser
 * @see ToolRunner
 */
public interface Tool extends Configurable {
  /**
   * Execute the command with the given arguments.
   * 
   * @param args command specific arguments.
   * @return exit code.
   * @throws Exception
   */
  int run(String [] args) throws Exception;
}
