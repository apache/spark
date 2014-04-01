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

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.Iterator;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A JUnit test to test Kill Job that has tasks with children and checks if the
 * children(subprocesses of java task) are also killed when a task is killed.
 */
public class TestKillSubProcesses extends TestCase {

  private static volatile Log LOG = LogFactory
            .getLog(TestKillSubProcesses.class);

  private static String BASE_TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();
  private static String TEST_ROOT_DIR = BASE_TEST_ROOT_DIR + Path.SEPARATOR
      + "killSubProcesses"; 

  private static Path scriptDir = new Path(TEST_ROOT_DIR, "script");
  private static String scriptDirName = scriptDir.toUri().getPath();
  private static Path signalFile = new Path(TEST_ROOT_DIR
      + "/script/signalFile");

  private static JobClient jobClient = null;

  static MiniMRCluster mr = null;

  private static String pid = null;

  // number of levels in the subtree of subprocesses of map task
  private static int numLevelsOfSubProcesses = 4;

  /**
   * Runs a job, kills the job and verifies if the map task and its
   * subprocesses are also killed properly or not.
   */
  private static void runKillingJobAndValidate(JobTracker jt, JobConf conf) throws IOException {

    conf.setJobName("testkilljobsubprocesses");
    conf.setMapperClass(KillingMapperWithChildren.class);
    
    RunningJob job = runJobAndSetProcessHandle(jt, conf);

    // kill the job now
    job.killJob();

    while (job.cleanupProgress() == 0.0f) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        LOG.warn("sleep is interrupted:" + ie);
        break;
      }
    }

    validateKillingSubprocesses(job, conf);
    // Checking the Job status
    assertEquals(job.getJobState(), JobStatus.KILLED);
  }

  /**
   * Runs a job that will fail and verifies if the subprocesses of failed map
   * task are killed properly or not.
   */
  private static void runFailingJobAndValidate(JobTracker jt, JobConf conf) throws IOException {

    conf.setJobName("testfailjobsubprocesses");
    conf.setMapperClass(FailingMapperWithChildren.class);
    
    // We don't want to run the failing map task 4 times. So we run it once and
    // check if all the subprocesses are killed properly.
    conf.setMaxMapAttempts(1);
    
    RunningJob job = runJobAndSetProcessHandle(jt, conf);
    signalTask(signalFile.toString(), conf);
    validateKillingSubprocesses(job, conf);
    // Checking the Job status
    assertEquals(job.getJobState(), JobStatus.FAILED);
  }
  
  /**
   * Runs a job that will succeed and verifies if the subprocesses of succeeded
   * map task are killed properly or not.
   */
  private static void runSuccessfulJobAndValidate(JobTracker jt, JobConf conf)
               throws IOException {

    conf.setJobName("testsucceedjobsubprocesses");
    conf.setMapperClass(MapperWithChildren.class);

    RunningJob job = runJobAndSetProcessHandle(jt, conf);
    signalTask(signalFile.toString(), conf);
    validateKillingSubprocesses(job, conf);
    // Checking the Job status
    assertEquals(job.getJobState(), JobStatus.SUCCEEDED);
  }

  /**
   * Runs the given job and saves the pid of map task.
   * Also checks if the subprocesses of map task are alive.
   */
  private static RunningJob runJobAndSetProcessHandle(JobTracker jt, JobConf conf)
                     throws IOException {
    RunningJob job = runJob(conf);
    while (job.getJobState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }

    pid = null;
    jobClient = new JobClient(conf);
    
    // get the taskAttemptID of the map task and use it to get the pid
    // of map task
    TaskReport[] mapReports = jobClient.getMapTaskReports(job.getID());

    JobInProgress jip = jt.getJob(job.getID());
    for(TaskReport tr : mapReports) {
      TaskInProgress tip = jip.getTaskInProgress(tr.getTaskID());

      // for this tip, get active tasks of all attempts
      while(tip.getActiveTasks().size() == 0) {
        //wait till the activeTasks Tree is built
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          LOG.warn("sleep is interrupted:" + ie);
          break;
        }
      }

      for (Iterator<TaskAttemptID> it = 
        tip.getActiveTasks().keySet().iterator(); it.hasNext();) {
        TaskAttemptID id = it.next();
        LOG.info("taskAttemptID of map task is " + id);
        
        while(pid == null) {
          pid = mr.getTaskTrackerRunner(0).getTaskTracker().getPid(id);
          if (pid == null) {
            try {
              Thread.sleep(500);
            } catch(InterruptedException e) {}
          }
        }
        LOG.info("pid of map task is " + pid);
        //Checking if the map task is alive
        assertTrue("Map is no more alive", isAlive(pid));
        LOG.info("The map task is alive before Job completion, as expected.");
      }
    }

    // Checking if the descendant processes of map task are alive
    if(ProcessTree.isSetsidAvailable) {
      String childPid = UtilsForTests.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + 0);
      while(childPid == null) {
        LOG.warn(scriptDirName + "/childPidFile" + 0 + " is null; Sleeping...");
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          LOG.warn("sleep is interrupted:" + ie);
          break;
        }
        childPid = UtilsForTests.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + 0);
      }

      // As childPidFile0(leaf process in the subtree of processes with
      // map task as root) is created, all other child pid files should
      // have been created already(See the script for details).
      // Now check if the descendants of map task are alive.
      for(int i=0; i <= numLevelsOfSubProcesses; i++) {
        childPid = UtilsForTests.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + i);
        LOG.info("pid of the descendant process at level " + i +
                 "in the subtree of processes(with the map task as the root)" +
                 " is " + childPid);
        assertTrue("Unexpected: The subprocess at level " + i +
                   " in the subtree is not alive before Job completion",
                   isAlive(childPid));
      }
    }
    return job;
  }
  
  /**
   * Verifies if the subprocesses of the map task are killed properly.
   */
  private static void validateKillingSubprocesses(RunningJob job, JobConf conf)
                   throws IOException {
    // wait till the the job finishes
    while (!job.isComplete()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        break;
      }
    }

    // Checking if the map task got killed or not
    assertTrue(!ProcessTree.isAlive(pid));
    LOG.info("The map task is not alive after Job is completed, as expected.");

    // Checking if the descendant processes of map task are killed properly
    if(ProcessTree.isSetsidAvailable) {
      for(int i=0; i <= numLevelsOfSubProcesses; i++) {
        String childPid = UtilsForTests.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + i);
        LOG.info("pid of the descendant process at level " + i +
                 "in the subtree of processes(with the map task as the root)" +
                 " is " + childPid);
        assertTrue("Unexpected: The subprocess at level " + i +
                   " in the subtree is alive after Job completion",
                   !isAlive(childPid));
      }
    }
    FileSystem fs = FileSystem.getLocal(mr.createJobConf());
    if(fs.exists(scriptDir)) {
      fs.delete(scriptDir, true);
    }
  }
  
  private static RunningJob runJob(JobConf conf) throws IOException {

    final Path inDir;
    final Path outDir;
    FileSystem fs = FileSystem.getLocal(conf);
    FileSystem tempFs = FileSystem.get(conf);
    //Check if test is run with hdfs or local file system.
    //if local filesystem then prepend TEST_ROOT_DIR, otherwise
    //killjob folder would be created in workspace root.
    if (!tempFs.getUri().toASCIIString().equals(
        fs.getUri().toASCIIString())) {
      inDir = new Path("killjob/input");
      outDir = new Path("killjob/output");
    } else {
      inDir = new Path(TEST_ROOT_DIR, "input");
      outDir = new Path(TEST_ROOT_DIR, "output");
    }

    
    if(fs.exists(scriptDir)) {
      fs.delete(scriptDir, true);
    }

    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);

    conf.set(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, 
             conf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, 
                      conf.get(JobConf.MAPRED_TASK_JAVA_OPTS)) +
             " -Dtest.build.data=" + BASE_TEST_ROOT_DIR);
    conf.set(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, 
             conf.get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, 
                      conf.get(JobConf.MAPRED_TASK_JAVA_OPTS)) +
             " -Dtest.build.data=" + BASE_TEST_ROOT_DIR);

    return UtilsForTests.runJob(conf, inDir, outDir);
  }

  public void testJobKillFailAndSucceed() throws IOException {
    if (Shell.WINDOWS) {
      System.out.println(
             "setsid doesn't work on WINDOWS as expected. Not testing");
      return;
    }
    
    try {
      JobConf conf = new JobConf();
      conf.setLong(JvmManager.JvmManagerForType.DELAY_BEFORE_KILL_KEY, 0L);
      mr = new MiniMRCluster(1, "file:///", 1, null, null, conf);

      // run the TCs
      conf = mr.createJobConf();
      JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
      runTests(conf, jt);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  void runTests(JobConf conf, JobTracker jt) throws IOException {
    FileSystem fs = FileSystem.getLocal(mr.createJobConf());
    Path rootDir = new Path(TEST_ROOT_DIR);
    if(!fs.exists(rootDir)) {
      fs.mkdirs(rootDir);
    }
    fs.setPermission(rootDir, 
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    runKillingJobAndValidate(jt, conf);
    runFailingJobAndValidate(jt, conf);
    runSuccessfulJobAndValidate(jt, conf);
  }

  /**
   * Creates signal file
   */
  private static void signalTask(String signalFile, JobConf conf) {
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      fs.createNewFile(new Path(signalFile));
    } catch(IOException e) {
      LOG.warn("Unable to create signal file. " + e);
    }
  }
  
  /**
   * Runs a recursive shell script to create a chain of subprocesses
   */
  private static void runChildren(JobConf conf) throws IOException {
    if (ProcessTree.isSetsidAvailable) {
      FileSystem fs = FileSystem.getLocal(conf);

      if (fs.exists(scriptDir)) {
        fs.delete(scriptDir, true);
      }

      // Create the directory and set open permissions so that the TT can
      // access.
      fs.mkdirs(scriptDir);
      fs.setPermission(scriptDir, new FsPermission(FsAction.ALL, FsAction.ALL,
          FsAction.ALL));

     // create shell script
     Random rm = new Random();
      Path scriptPath = new Path(scriptDirName, "_shellScript_" + rm.nextInt()
        + ".sh");
      String shellScript = scriptPath.toString();

      // Construct the script. Set umask to 0000 so that TT can access all the
      // files.
      String script =
        "umask 000\n" + 
        "echo $$ > " + scriptDirName + "/childPidFile" + "$1\n" +
        "echo hello\n" +
        "trap 'echo got SIGTERM' 15 \n" +
        "if [ $1 != 0 ]\nthen\n" +
        " sh " + shellScript + " $(($1-1))\n" +
        "else\n" +
        " while true\n do\n" +
        "  sleep 2\n" +
        " done\n" +
        "fi";
      DataOutputStream file = fs.create(scriptPath);
      file.writeBytes(script);
      file.close();

      // Set executable permissions on the script.
      new File(scriptPath.toUri().getPath()).setExecutable(true);

      LOG.info("Calling script from map task : " + shellScript);
      Runtime.getRuntime()
          .exec(shellScript + " " + numLevelsOfSubProcesses);
    
      String childPid = UtilsForTests.getPidFromPidFile(scriptDirName
          + "/childPidFile" + 0);
      while (childPid == null) {
        LOG.warn(scriptDirName + "/childPidFile" + 0 + " is null; Sleeping...");
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          LOG.warn("sleep is interrupted:" + ie);
          break;
        }
        childPid = UtilsForTests.getPidFromPidFile(scriptDirName
            + "/childPidFile" + 0);
      }
    }
  }
  
  /**
   * Mapper that starts children
   */
  static class MapperWithChildren extends MapReduceBase implements
  Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    FileSystem fs = null;
    public void configure(JobConf conf) {
      try {
        fs = FileSystem.getLocal(conf);
        runChildren(conf);
      } catch (Exception e) {
        LOG.warn("Exception in configure: " +
                 StringUtils.stringifyException(e));
      }
    }
    
    // Mapper waits for the signal(signal is the existence of a file)
    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {
      while (!fs.exists(signalFile)) {// wait for signal file creation
        try {
          reporter.progress();
          synchronized (this) {
            this.wait(1000);
          }
        } catch (InterruptedException ie) {
          System.out.println("Interrupted while the map was waiting for "
              + " the signal.");
          break;
        }
      }
    }
  }
  
  /**
   * Mapper that waits till it gets killed.
   */
  static class KillingMapperWithChildren extends MapperWithChildren {
    public void configure(JobConf conf) {
      super.configure(conf);
    }
    
    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      try {
        while(true) {//just wait till kill happens
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        LOG.warn("Exception in KillMapperWithChild.map:" + e);
      }
    }
  }
  
  /**
   * Mapper that fails when recieves a signal. Signal is existence of a file.
   */
  static class FailingMapperWithChildren extends MapperWithChildren {
    public void configure(JobConf conf) {
      super.configure(conf);
    }

    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {
      while (!fs.exists(signalFile)) {// wait for signal file creation
        try {
          reporter.progress();
          synchronized (this) {
            this.wait(1000);
          }
        } catch (InterruptedException ie) {
          System.out.println("Interrupted while the map was waiting for "
              + " the signal.");
          break;
        }
      }
      throw new RuntimeException("failing map");
    }
  }
  
  /**
   * Check for presence of the process with the pid passed is alive or not
   * currently.
   * 
   * @param pid pid of the process
   * @return if a process is alive or not.
   */
  private static boolean isAlive(String pid) throws IOException {
    String commandString ="ps -o pid,command -e";
    String args[] = new String[] {"bash", "-c" , commandString};
    ShellCommandExecutor shExec = new ShellCommandExecutor(args);
    try {
      shExec.execute(); 
    }catch(ExitCodeException e) {
      return false;
    } catch (IOException e) {
      LOG.warn("IOExecption thrown while checking if process is alive" + 
          StringUtils.stringifyException(e));
      throw e;
    }

    String output = shExec.getOutput();

    //Parse the command output and check for pid, ignore the commands
    //which has ps or grep in it.
    StringTokenizer strTok = new StringTokenizer(output, "\n");
    boolean found = false;
    while(strTok.hasMoreTokens()) {
      StringTokenizer pidToken = new StringTokenizer(strTok.nextToken(), 
          " ");
      String pidStr = pidToken.nextToken();
      String commandStr = pidToken.nextToken();
      if(pid.equals(pidStr) && !(commandStr.contains("ps") 
          || commandStr.contains("grep"))) {
        found = true;
        break;
      }
    }
    return found; 
  }
  
}
