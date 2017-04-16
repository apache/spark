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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ProcessTree.Signal;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import junit.framework.TestCase;

/**
 * A JUnit test to test ProcfsBasedProcessTree.
 */
public class TestProcfsBasedProcessTree extends TestCase {

  private static final Log LOG = LogFactory
      .getLog(TestProcfsBasedProcessTree.class);
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString().replace(' ', '+');

  private ShellCommandExecutor shexec = null;
  private String pidFile, lowestDescendant;
  private String shellScript;
  private static final int N = 6; // Controls the RogueTask


  private class RogueTaskThread extends Thread {
    public void run() {
      try {
        Vector<String> args = new Vector<String>();
        if(ProcessTree.isSetsidAvailable) {
          args.add("setsid");
        }
        args.add("bash");
        args.add("-c");
        args.add(" echo $$ > " + pidFile + "; sh " +
            shellScript + " " + N + ";") ;
        shexec = new ShellCommandExecutor(args.toArray(new String[0]));
        shexec.execute();
      } catch (ExitCodeException ee) {
        LOG.info("Shell Command exit with a non-zero exit code. This is" +
            " expected as we are killing the subprocesses of the" +
            " task intentionally. " + ee);
      } catch (IOException ioe) {
        LOG.info("Error executing shell command " + ioe);
      } finally {
        LOG.info("Exit code: " + shexec.getExitCode());
      }
    }
  }

  private String getRogueTaskPID() {
    File f = new File(pidFile);
    while (!f.exists()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
        break;
      }
    }

    // read from pidFile
    return UtilsForTests.getPidFromPidFile(pidFile);
  }

  public void testProcessTree() {

    try {
      if (!ProcfsBasedProcessTree.isAvailable()) {
        System.out
            .println("ProcfsBasedProcessTree is not available on this system. Not testing");
        return;
      }
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
      return;
    }
    // create shell script
    Random rm = new Random();
    File tempFile = new File(TEST_ROOT_DIR, this.getName() + "_shellScript_"
        + rm.nextInt() + ".sh");
    tempFile.deleteOnExit();
    shellScript = TEST_ROOT_DIR + File.separator + tempFile.getName();

    // create pid file
    tempFile = new File(TEST_ROOT_DIR,  this.getName() + "_pidFile_" +
                        rm.nextInt() + ".pid");
    tempFile.deleteOnExit();
    pidFile = TEST_ROOT_DIR + File.separator + tempFile.getName();

    lowestDescendant = TEST_ROOT_DIR + File.separator + "lowestDescendantPidFile";

    // write to shell-script
    try {
      FileWriter fWriter = new FileWriter(shellScript);
      fWriter.write(
          "# rogue task\n" +
          "sleep 1\n" +
          "echo hello\n" +
          "if [ $1 -ne 0 ]\n" +
          "then\n" +
          " sh " + shellScript + " $(($1-1))\n" +
          "else\n" +
          " echo $$ > " + lowestDescendant + "\n" +
          " while true\n do\n" +
          "  sleep 5\n" +
          " done\n" +
          "fi");
      fWriter.close();
    } catch (IOException ioe) {
      LOG.info("Error: " + ioe);
      return;
    }

    Thread t = new RogueTaskThread();
    t.start();
    String pid = getRogueTaskPID();
    LOG.info("Root process pid: " + pid);
    ProcfsBasedProcessTree p = new ProcfsBasedProcessTree(pid,
        ProcessTree.isSetsidAvailable);
    p = p.getProcessTree(); // initialize
    LOG.info("ProcessTree: " + p.toString());
    File leaf = new File(lowestDescendant);
    //wait till lowest descendant process of Rougue Task starts execution
    while (!leaf.exists()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
        break;
      }
    }
    
    p = p.getProcessTree(); // reconstruct
    LOG.info("ProcessTree: " + p.toString());

    // Get the process-tree dump
    String processTreeDump = p.getProcessTreeDump();

    // destroy the map task and all its subprocesses
    if (ProcessTree.isSetsidAvailable) {
      ProcessTree.killProcessGroup(pid, Signal.KILL);
    } else {
      ProcessTree.killProcess(pid, Signal.KILL);
    }
    if(ProcessTree.isSetsidAvailable) {// whole processtree should be gone
      assertEquals(false, p.isAnyProcessInTreeAlive());
    }
    else {// process should be gone
      assertEquals(false, p.isAlive());
    }

    LOG.info("Process-tree dump follows: \n" + processTreeDump);
    assertTrue("Process-tree dump doesn't start with a proper header",
        processTreeDump.startsWith("\t|- PID PPID PGRPID SESSID CMD_NAME " +
        "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) " +
        "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
    for (int i = N; i >= 0; i--) {
      String cmdLineDump = "\\|- [0-9]+ [0-9]+ [0-9]+ [0-9]+ \\(sh\\)" +
          " [0-9]+ [0-9]+ [0-9]+ [0-9]+ sh " + shellScript + " " + i;
      Pattern pat = Pattern.compile(cmdLineDump);
      Matcher mat = pat.matcher(processTreeDump);
      assertTrue("Process-tree dump doesn't contain the cmdLineDump of " + i
          + "th process!", mat.find());
    }

    // Not able to join thread sometimes when forking with large N.
    try {
      t.join(2000);
      LOG.info("RogueTaskThread successfully joined.");
    } catch (InterruptedException ie) {
      LOG.info("Interrupted while joining RogueTaskThread.");
    }

    // ProcessTree is gone now. Any further calls should be sane.
    p = p.getProcessTree();
    assertFalse("ProcessTree must have been gone", p.isAlive());
    assertTrue("Cumulative vmem for the gone-process is "
        + p.getCumulativeVmem() + " . It should be zero.", p
        .getCumulativeVmem() == 0);
    assertTrue(p.toString().equals("[ ]"));
  }
  
  public static class ProcessStatInfo {
    // sample stat in a single line : 3910 (gpm) S 1 3910 3910 0 -1 4194624 
    // 83 0 0 0 0 0 0 0 16 0 1 0 7852 2408448 88 4294967295 134512640 
    // 134590050 3220521392 3220520036 10975138 0 0 4096 134234626 
    // 4294967295 0 0 17 1 0 0
    String pid;
    String name;
    String ppid;
    String pgrpId;
    String session;
    String vmem = "0";
    String rssmemPage = "0";
    String utime = "0";
    String stime = "0";
    
    public ProcessStatInfo(String[] statEntries) {
      pid = statEntries[0];
      name = statEntries[1];
      ppid = statEntries[2];
      pgrpId = statEntries[3];
      session = statEntries[4];
      vmem = statEntries[5];
      if (statEntries.length > 6) {
        rssmemPage = statEntries[6];
      }
      if (statEntries.length > 7) {
        utime = statEntries[7];
        stime = statEntries[8];
      }
    }
    
    // construct a line that mimics the procfs stat file.
    // all unused numerical entries are set to 0.
    public String getStatLine() {
      return String.format("%s (%s) S %s %s %s 0 0 0" +
                      " 0 0 0 0 %s %s 0 0 0 0 0 0 0 %s %s 0 0" +
                      " 0 0 0 0 0 0 0 0" +
                      " 0 0 0 0 0", 
                      pid, name, ppid, pgrpId, session,
                      utime, stime, vmem, rssmemPage);
    }
  }
  
  /**
   * A basic test that creates a few process directories and writes
   * stat files. Verifies that the cpu time and memory is correctly
   * computed.
   * @throws IOException if there was a problem setting up the
   *                      fake procfs directories or files.
   */
  public void testCpuAndMemoryForProcessTree() throws IOException {

    // test processes
    String[] pids = { "100", "200", "300", "400" };
    // create the fake procfs root directory. 
    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);
      
      // create stat objects.
      // assuming processes 100, 200, 300 are in tree and 400 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[4];
      procInfos[0] = new ProcessStatInfo(new String[] 
          {"100", "proc1", "1", "100", "100", "100000", "100", "1000", "200"});
      procInfos[1] = new ProcessStatInfo(new String[] 
          {"200", "proc2", "100", "100", "100", "200000", "200", "2000", "400"});
      procInfos[2] = new ProcessStatInfo(new String[] 
          {"300", "proc3", "200", "100", "100", "300000", "300", "3000", "600"});
      procInfos[3] = new ProcessStatInfo(new String[] 
          {"400", "proc4", "1", "400", "400", "400000", "400", "4000", "800"});
      
      writeStatFiles(procfsRootDir, pids, procInfos);
      
      // crank up the process tree class.
      ProcfsBasedProcessTree processTree = 
          new ProcfsBasedProcessTree("100", procfsRootDir.getAbsolutePath());
      // build the process tree.
      processTree.getProcessTree();
      
      // verify cumulative memory
      assertEquals("Cumulative virtual memory does not match", 600000L,
                   processTree.getCumulativeVmem());

      // verify rss memory
      long cumuRssMem = ProcfsBasedProcessTree.PAGE_SIZE > 0 ?
                        600L * ProcfsBasedProcessTree.PAGE_SIZE : 0L;
      assertEquals("Cumulative rss memory does not match",
                   cumuRssMem, processTree.getCumulativeRssmem());

      // verify cumulative cpu time
      long cumuCpuTime = ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS > 0 ?
             7200L * ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS : 0L;
      assertEquals("Cumulative cpu time does not match",
                   cumuCpuTime, processTree.getCumulativeCpuTime());

      // test the cpu time again to see if it cumulates
      procInfos[0] = new ProcessStatInfo(new String[]
          {"100", "proc1", "1", "100", "100", "100000", "100", "2000", "300"});
      procInfos[1] = new ProcessStatInfo(new String[]
          {"200", "proc2", "100", "100", "100", "200000", "200", "3000", "500"});
      writeStatFiles(procfsRootDir, pids, procInfos);

      // build the process tree.
      processTree.getProcessTree();

      // verify cumulative cpu time again
      cumuCpuTime = ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS > 0 ?
             9400L * ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS : 0L;
      assertEquals("Cumulative cpu time does not match",
                   cumuCpuTime, processTree.getCumulativeCpuTime());
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }
  
  /**
   * Tests that cumulative memory is computed only for
   * processes older than a given age.
   * @throws IOException if there was a problem setting up the
   *                      fake procfs directories or files.
   */
  public void testMemForOlderProcesses() throws IOException {
    // initial list of processes
    String[] pids = { "100", "200", "300", "400" };
    // create the fake procfs root directory. 
    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);
      
      // create stat objects.
      // assuming 100, 200 and 400 are in tree, 300 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[4];
      procInfos[0] = new ProcessStatInfo(new String[] 
                        {"100", "proc1", "1", "100", "100", "100000", "100"});
      procInfos[1] = new ProcessStatInfo(new String[] 
                        {"200", "proc2", "100", "100", "100", "200000", "200"});
      procInfos[2] = new ProcessStatInfo(new String[] 
                        {"300", "proc3", "1", "300", "300", "300000", "300"});
      procInfos[3] = new ProcessStatInfo(new String[] 
                        {"400", "proc4", "100", "100", "100", "400000", "400"});
      
      writeStatFiles(procfsRootDir, pids, procInfos);
      
      // crank up the process tree class.
      ProcfsBasedProcessTree processTree = 
          new ProcfsBasedProcessTree("100", procfsRootDir.getAbsolutePath());
      // build the process tree.
      processTree.getProcessTree();
      
      // verify cumulative memory
      assertEquals("Cumulative memory does not match",
                   700000L, processTree.getCumulativeVmem());

      // write one more process as child of 100.
      String[] newPids = { "500" };
      setupPidDirs(procfsRootDir, newPids);
      
      ProcessStatInfo[] newProcInfos = new ProcessStatInfo[1];
      newProcInfos[0] = new ProcessStatInfo(new String[]
                      {"500", "proc5", "100", "100", "100", "500000", "500"});
      writeStatFiles(procfsRootDir, newPids, newProcInfos);
      
      // check memory includes the new process.
      processTree.getProcessTree();
      assertEquals("Cumulative vmem does not include new process",
                   1200000L, processTree.getCumulativeVmem());
      long cumuRssMem = ProcfsBasedProcessTree.PAGE_SIZE > 0 ?
                        1200L * ProcfsBasedProcessTree.PAGE_SIZE : 0L;
      assertEquals("Cumulative rssmem does not include new process",
                   cumuRssMem, processTree.getCumulativeRssmem());
      
      // however processes older than 1 iteration will retain the older value
      assertEquals("Cumulative vmem shouldn't have included new process",
                   700000L, processTree.getCumulativeVmem(1));
      cumuRssMem = ProcfsBasedProcessTree.PAGE_SIZE > 0 ?
                   700L * ProcfsBasedProcessTree.PAGE_SIZE : 0L;
      assertEquals("Cumulative rssmem shouldn't have included new process",
                   cumuRssMem, processTree.getCumulativeRssmem(1));

      // one more process
      newPids = new String[]{ "600" };
      setupPidDirs(procfsRootDir, newPids);
      
      newProcInfos = new ProcessStatInfo[1];
      newProcInfos[0] = new ProcessStatInfo(new String[]
                      {"600", "proc6", "100", "100", "100", "600000", "600"});
      writeStatFiles(procfsRootDir, newPids, newProcInfos);

      // refresh process tree
      processTree.getProcessTree();
      
      // processes older than 2 iterations should be same as before.
      assertEquals("Cumulative vmem shouldn't have included new processes",
                   700000L, processTree.getCumulativeVmem(2));
      cumuRssMem = ProcfsBasedProcessTree.PAGE_SIZE > 0 ?
                   700L * ProcfsBasedProcessTree.PAGE_SIZE : 0L;
      assertEquals("Cumulative rssmem shouldn't have included new processes",
                   cumuRssMem, processTree.getCumulativeRssmem(2));

      // processes older than 1 iteration should not include new process,
      // but include process 500
      assertEquals("Cumulative vmem shouldn't have included new processes",
                   1200000L, processTree.getCumulativeVmem(1));
      cumuRssMem = ProcfsBasedProcessTree.PAGE_SIZE > 0 ?
                   1200L * ProcfsBasedProcessTree.PAGE_SIZE : 0L;
      assertEquals("Cumulative rssmem shouldn't have included new processes",
                   cumuRssMem, processTree.getCumulativeRssmem(1));

      // no processes older than 3 iterations, this should be 0
      assertEquals("Getting non-zero vmem for processes older than 3 iterations",
                    0L, processTree.getCumulativeVmem(3));
      assertEquals("Getting non-zero rssmem for processes older than 3 iterations",
                    0L, processTree.getCumulativeRssmem(3));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  /**
   * Test the correctness of process-tree dump.
   * 
   * @throws IOException
   */
  public void testProcessTreeDump()
      throws IOException {

    String[] pids = { "100", "200", "300", "400", "500", "600" };

    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);

      int numProcesses = pids.length;
      // Processes 200, 300, 400 and 500 are descendants of 100. 600 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[numProcesses];
      procInfos[0] = new ProcessStatInfo(new String[] {
          "100", "proc1", "1", "100", "100", "100000", "100", "1000", "200"});
      procInfos[1] = new ProcessStatInfo(new String[] {
          "200", "proc2", "100", "100", "100", "200000", "200", "2000", "400"});
      procInfos[2] = new ProcessStatInfo(new String[] {
          "300", "proc3", "200", "100", "100", "300000", "300", "3000", "600"});
      procInfos[3] = new ProcessStatInfo(new String[] {
          "400", "proc4", "200", "100", "100", "400000", "400", "4000", "800"});
      procInfos[4] = new ProcessStatInfo(new String[] {
          "500", "proc5", "400", "100", "100", "400000", "400", "4000", "800"});
      procInfos[5] = new ProcessStatInfo(new String[] {
          "600", "proc6", "1", "1", "1", "400000", "400", "4000", "800"});

      String[] cmdLines = new String[numProcesses];
      cmdLines[0] = "proc1 arg1 arg2";
      cmdLines[1] = "proc2 arg3 arg4";
      cmdLines[2] = "proc3 arg5 arg6";
      cmdLines[3] = "proc4 arg7 arg8";
      cmdLines[4] = "proc5 arg9 arg10";
      cmdLines[5] = "proc6 arg11 arg12";

      writeStatFiles(procfsRootDir, pids, procInfos);
      writeCmdLineFiles(procfsRootDir, pids, cmdLines);

      ProcfsBasedProcessTree processTree =
          new ProcfsBasedProcessTree("100", procfsRootDir.getAbsolutePath());
      // build the process tree.
      processTree.getProcessTree();

      // Get the process-tree dump
      String processTreeDump = processTree.getProcessTreeDump();

      LOG.info("Process-tree dump follows: \n" + processTreeDump);
      assertTrue("Process-tree dump doesn't start with a proper header",
          processTreeDump.startsWith("\t|- PID PPID PGRPID SESSID CMD_NAME " +
          "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) " +
          "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
      for (int i = 0; i < 5; i++) {
        ProcessStatInfo p = procInfos[i];
        assertTrue(
            "Process-tree dump doesn't contain the cmdLineDump of process "
                + p.pid, processTreeDump.contains("\t|- " + p.pid + " "
                + p.ppid + " " + p.pgrpId + " " + p.session + " (" + p.name
                + ") " + p.utime + " " + p.stime + " " + p.vmem + " "
                + p.rssmemPage + " " + cmdLines[i]));
      }

      // 600 should not be in the dump
      ProcessStatInfo p = procInfos[5];
      assertFalse(
          "Process-tree dump shouldn't contain the cmdLineDump of process "
              + p.pid, processTreeDump.contains("\t|- " + p.pid + " " + p.ppid
              + " " + p.pgrpId + " " + p.session + " (" + p.name + ") "
              + p.utime + " " + p.stime + " " + p.vmem + " " + cmdLines[5]));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  /**
   * Create a directory to mimic the procfs file system's root.
   * @param procfsRootDir root directory to create.
   * @throws IOException if could not delete the procfs root directory
   */
  public static void setupProcfsRootDir(File procfsRootDir) 
                                        throws IOException { 
    // cleanup any existing process root dir.
    if (procfsRootDir.exists()) {
      assertTrue(FileUtil.fullyDelete(procfsRootDir));  
    }

    // create afresh
    assertTrue(procfsRootDir.mkdirs());
  }

  /**
   * Create PID directories under the specified procfs root directory
   * @param procfsRootDir root directory of procfs file system
   * @param pids the PID directories to create.
   * @throws IOException If PID dirs could not be created
   */
  public static void setupPidDirs(File procfsRootDir, String[] pids) 
                      throws IOException {
    for (String pid : pids) {
      File pidDir = new File(procfsRootDir, pid);
      pidDir.mkdir();
      if (!pidDir.exists()) {
        throw new IOException ("couldn't make process directory under " +
            "fake procfs");
      } else {
        LOG.info("created pid dir");
      }
    }
  }
  
  /**
   * Write stat files under the specified pid directories with data
   * setup in the corresponding ProcessStatInfo objects
   * @param procfsRootDir root directory of procfs file system
   * @param pids the PID directories under which to create the stat file
   * @param procs corresponding ProcessStatInfo objects whose data should be
   *              written to the stat files.
   * @throws IOException if stat files could not be written
   */
  public static void writeStatFiles(File procfsRootDir, String[] pids, 
                              ProcessStatInfo[] procs) throws IOException {
    for (int i=0; i<pids.length; i++) {
      File statFile =
          new File(new File(procfsRootDir, pids[i]),
              ProcfsBasedProcessTree.PROCFS_STAT_FILE);
      BufferedWriter bw = null;
      try {
        FileWriter fw = new FileWriter(statFile);
        bw = new BufferedWriter(fw);
        bw.write(procs[i].getStatLine());
        LOG.info("wrote stat file for " + pids[i] + 
                  " with contents: " + procs[i].getStatLine());
      } finally {
        // not handling exception - will throw an error and fail the test.
        if (bw != null) {
          bw.close();
        }
      }
    }
  }

  private static void writeCmdLineFiles(File procfsRootDir, String[] pids,
      String[] cmdLines)
      throws IOException {
    for (int i = 0; i < pids.length; i++) {
      File statFile =
          new File(new File(procfsRootDir, pids[i]),
              ProcfsBasedProcessTree.PROCFS_CMDLINE_FILE);
      BufferedWriter bw = null;
      try {
        bw = new BufferedWriter(new FileWriter(statFile));
        bw.write(cmdLines[i]);
        LOG.info("wrote command-line file for " + pids[i] + " with contents: "
            + cmdLines[i]);
      } finally {
        // not handling exception - will throw an error and fail the test.
        if (bw != null) {
          bw.close();
        }
      }
    }
  }
}
