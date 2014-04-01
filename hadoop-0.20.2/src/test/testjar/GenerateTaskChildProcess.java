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

package testjar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import java.util.Random;
import java.io.IOException;
import java.io.DataOutputStream;
import java.io.File;

/**
 * It uses for defining a various types of mapperes with child processes. 
 */
public class GenerateTaskChildProcess {
  private static final Log LOG = LogFactory
          .getLog(GenerateTaskChildProcess.class);
  /**
   * It uses for defining the string appending mapper with 
   * child processes.It's keep appending the string and increases
   * the memory continuously. 
   */
  public static class StrAppendMapper extends MapReduceBase implements 
          Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private JobConf conf;

    public void configure(JobConf conf) {
          this.conf = conf;
    }

    public void map(LongWritable key, Text value,
            OutputCollector<NullWritable, NullWritable> out,
                    Reporter reporter) throws IOException {
      int counter = 0;
      while (counter < 30) {
        try {
          reporter.progress();
          synchronized (this) {
            this.wait(1000);
          }          
        } catch (InterruptedException iexp) {
          iexp.printStackTrace();
          LOG.warn("Interrupted while the map was waiting.");
        }
        counter ++;
      }
      try {
          createChildProcess(conf, "AppendStr");
        } catch (Exception exp) {
          exp.printStackTrace();
          LOG.warn("Exception thrown while creating the child processes");
      }
    }

    public void close() {
    }
  }
  
  /**
   * It uses for defining the String display mapper with child processes.
   */
  public static class StrDisplayMapper extends MapReduceBase implements
          Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private JobConf conf;
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    public void map(LongWritable key, Text value,
            OutputCollector<NullWritable, NullWritable> out,
                    Reporter reporter) throws IOException {
      int counter = 0;
      while (counter < 30) {
        try {
          reporter.progress();
          synchronized (this) {
            this.wait(1000);
          }
        } catch (InterruptedException iexp) {
          iexp.printStackTrace();
          LOG.warn("Interrupted while the map was waiting.");
          break;
        }
        counter ++;
      }
      try {
        createChildProcess(conf, "DispStr");
      } catch (Exception exp) {
          exp.printStackTrace();
          LOG.warn("Exception thrown while creating the child processes.");
      }
    }
  }

  /**
   * It uses for defining a failed mapper with child processes.
   *
   */
  public static class FailedMapper extends MapReduceBase implements
          Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private JobConf conf;
    public void configure(JobConf conf) {
      try {
        createChildProcess(conf, "failedmapper");
      } catch (Exception exp) {
        exp.printStackTrace();
        LOG.warn("Exception throw while creating the child processes");
      }
    }

    public void map(LongWritable key, Text value,
          OutputCollector<NullWritable, NullWritable> out,
                  Reporter reporter) throws IOException {
      int counter = 0;
      while (counter < 30) {
        try {
          reporter.progress();
          synchronized (this) {
            this.wait(1000);
          }
        } catch (InterruptedException iexp) {
          iexp.printStackTrace();
          LOG.warn("Interrupted while the map was waiting.");
          break;
        }
        counter ++;
      }
      throw new RuntimeException("Mapper failed.");
    }
  }

 /**
   *  It uses for failing the map tasks.
   *
   */
  public static class FailMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, NullWritable, NullWritable> {

    public void map(LongWritable key, Text value,
          OutputCollector<NullWritable, NullWritable> out,
                  Reporter reporter) throws IOException {
      throw new RuntimeException("failing the map");
    }
  }

  /** 
   * It uses for creating the child processes for a task.
   * @param conf configuration for a job.
   * @param jobName the name of the mapper job.
   * @throws IOException if an I/O error occurs.
   */
  private static void createChildProcess(JobConf conf, String jobName)
          throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    File TMP_ROOT_DIR = new File("/tmp");
    String TEST_ROOT_DIR = TMP_ROOT_DIR.getAbsolutePath() 
            + Path.SEPARATOR + "ChildProc_" + jobName;
    Path scriptDir = new Path(TEST_ROOT_DIR);
    int numOfChildProcesses = 2;

    if (fs.exists(scriptDir)) {
      fs.delete(scriptDir, true);
    }
    fs.mkdirs(scriptDir);
    fs.setPermission(scriptDir, new FsPermission(FsAction.ALL,
            FsAction.ALL, FsAction.ALL));

    String scriptDirName = scriptDir.toUri().getPath();
    Random rm = new Random();
    String scriptName = "ShellScript_" + jobName + "_" 
            + rm.nextInt() + ".sh";
    Path scriptPath = new Path(scriptDirName, scriptName);
    String shellScript = scriptPath.toString();
    String script = null;
    if (jobName.equals("AppendStr")) {
      script =  "#!/bin/sh\n" 
              + "umask 000\n" 
              + "StrVal=\"Hadoop is framework for data intensive "
              + "distributed applications.\"\n"
              + "StrVal=\"${StrVal}Hadoop enables applications to work "
              + "with thousands of nodes.\"\n"
              + "echo $StrVal\n"
              + "if [ \"X$1\" != \"X0\" ]\nthen\n" 
              + "  sh " + shellScript + " $(($1-1))\n"
              + "else\n"
              + "  while(true)\n" 
              + "  do\n"
              + "    StrVal=\"$StrVal Hadoop \"\n"
              + "  done\n"
              + "fi";
    } else if (jobName.equals("DispStr")) {
      script =  "#!/bin/sh\n" 
              + "umask 000\n" 
              + "msg=Welcome\n"
              + "echo $msg\n"
              + " if [ \"X$1\" != \"X0\" ]\nthen\n" 
              + "  sh " + shellScript + " $(($1-1))\n" 
              + "else\n" 
              + "  while(true)\n"
              + "  do\n"
              + "    sleep 2 \n" 
              + "  done\n" 
              + "fi";
    }else {
     script =  "#!/bin/sh\n" 
             + "umask 000\n" 
             + "msg=Welcome\n"
             + "echo $msg\n"
             + " if [ \"X$1\" != \"X0\" ]\nthen\n" 
             + "  sh " + shellScript + " $(($1-1))\n" 
             + "else\n"
             + "  for count in {1..1000}\n"
             + "  do\n"
             + "    echo \"$msg_$count\" \n" 
             + "  done\n" 
             + "fi";
    }
    DataOutputStream file = fs.create(scriptPath);
    file.writeBytes(script);
    file.close();
    File scriptFile = new File(scriptDirName,scriptName);
    scriptFile.setExecutable(true);
    LOG.info("script absolute path:" + scriptFile.getAbsolutePath());
    String [] cmd = new String[]{scriptFile.getAbsolutePath(), 
            String.valueOf(numOfChildProcesses)};
    ShellCommandExecutor shellExec = new ShellCommandExecutor(cmd);
    shellExec.execute();
  }

}
