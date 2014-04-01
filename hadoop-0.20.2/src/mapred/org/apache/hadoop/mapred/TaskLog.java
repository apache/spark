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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.Shell;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 * This class is for Map/Reduce internal use only.
 * 
 */
public class TaskLog {
  private static final Log LOG =
    LogFactory.getLog(TaskLog.class);

  static final String USERLOGS_DIR_NAME = "userlogs";

  private static final File LOG_DIR = 
    new File(getBaseLogDir(), USERLOGS_DIR_NAME).getAbsoluteFile();
  
  // localFS is set in (and used by) writeToIndexFile()
  static LocalFileSystem localFS = null;
  static {
    if (!LOG_DIR.exists()) {
      LOG_DIR.mkdirs();
    }
  }
  
  static AtomicInteger rotor = new AtomicInteger(0);

  /**
   * Create log directory for the given attempt. This involves creating the
   * following and setting proper permissions for the new directories
   * <br>{hadoop.log.dir}/userlogs/<jobid>
   * <br>{hadoop.log.dir}/userlogs/<jobid>/<attempt-id-as-symlink>
   * <br>{one of the mapred-local-dirs}/userlogs/<jobid>
   * <br>{one of the mapred-local-dirs}/userlogs/<jobid>/<attempt-id>
   *
   * @param taskID attempt-id for which log dir is to be created
   * @param isCleanup Is this attempt a cleanup attempt ?
   * @param localDirs mapred local directories
   * @throws IOException
   */
  public static void createTaskAttemptLogDir(TaskAttemptID taskID,
      boolean isCleanup, String[] localDirs) throws IOException {
    String cleanupSuffix = isCleanup ? ".cleanup" : "";
    String strAttemptLogDir = getTaskAttemptLogDir(taskID, 
        cleanupSuffix, localDirs);
    File attemptLogDir = new File(strAttemptLogDir);
    if (!attemptLogDir.mkdirs()) {
      throw new IOException("Creation of " + attemptLogDir + " failed.");
    }
    String strLinkAttemptLogDir = getJobDir(
        taskID.getJobID()).getAbsolutePath() + File.separatorChar + 
        taskID.toString() + cleanupSuffix;
    if (FileUtil.symLink(strAttemptLogDir, strLinkAttemptLogDir) != 0) {
      throw new IOException("Creation of symlink from " + 
                            strLinkAttemptLogDir + " to " + strAttemptLogDir +
                            " failed.");
    }

    FileSystem localFs = FileSystem.getLocal(new Configuration());
    localFs.setPermission(new Path(attemptLogDir.getPath()),
                          new FsPermission((short)0700));
  }

  /**
   * Get one of the mapred local directory in a round-robin-way.
   * @param localDirs mapred local directories
   * @return the next chosen mapred local directory
   * @throws IOException
   */
  private static String getNextLocalDir(String[] localDirs) throws IOException {
    if (localDirs.length == 0) {
      throw new IOException ("Not enough mapred.local.dirs ("
                             + localDirs.length + ")");
    }
    return localDirs[Math.abs(rotor.getAndIncrement()) % localDirs.length];  
  }

  /**
   * Get attempt log directory path for the given attempt-id under randomly
   * selected mapred local directory.
   * @param taskID attempt-id for which log dir path is needed
   * @param cleanupSuffix ".cleanup" if this attempt is a cleanup attempt 
   * @param localDirs mapred local directories
   * @return target task attempt log directory
   * @throws IOException
   */
  public static String getTaskAttemptLogDir(TaskAttemptID taskID, 
      String cleanupSuffix, String[] localDirs) throws IOException {
    StringBuilder taskLogDirLocation = new StringBuilder();
    taskLogDirLocation.append(getNextLocalDir(localDirs));
    taskLogDirLocation.append(File.separatorChar);
    taskLogDirLocation.append(USERLOGS_DIR_NAME);
    taskLogDirLocation.append(File.separatorChar);
    taskLogDirLocation.append(taskID.getJobID().toString());
    taskLogDirLocation.append(File.separatorChar);
    taskLogDirLocation.append(taskID.toString()+cleanupSuffix);
    return taskLogDirLocation.toString();
  }
  
  public static File getTaskLogFile(TaskAttemptID taskid, boolean isCleanup,
      LogName filter) {
    return new File(getAttemptDir(taskid, isCleanup), filter.toString());
  }

  /**
   * Get the real task-log file-path
   * 
   * @param location Location of the log-file. This should point to an
   *          attempt-directory.
   * @param filter
   * @return
   * @throws IOException
   */
  static String getRealTaskLogFilePath(String location, LogName filter)
      throws IOException {
    return FileUtil.makeShellPath(new File(location, filter.toString()));
  }

  static class LogFileDetail {
    final static String LOCATION = "LOG_DIR:";
    String location;
    long start;
    long length;
  }

  static Map<LogName, LogFileDetail> getAllLogsFileDetails(
      TaskAttemptID taskid, boolean isCleanup) throws IOException {

    Map<LogName, LogFileDetail> allLogsFileDetails =
        new HashMap<LogName, LogFileDetail>();

    File indexFile = getIndexFile(taskid, isCleanup);
    BufferedReader fis = new BufferedReader(new FileReader(indexFile));
    //the format of the index file is
    //LOG_DIR: <the dir where the task logs are really stored>
    //stdout:<start-offset in the stdout file> <length>
    //stderr:<start-offset in the stderr file> <length>
    //syslog:<start-offset in the syslog file> <length>
    String str = fis.readLine();
    if (str == null) { //the file doesn't have anything
      throw new IOException ("Index file for the log of " + taskid+" doesn't exist.");
    }
    String loc = str.substring(str.indexOf(LogFileDetail.LOCATION)+
        LogFileDetail.LOCATION.length());
    //special cases are the debugout and profile.out files. They are guaranteed
    //to be associated with each task attempt since jvm reuse is disabled
    //when profiling/debugging is enabled
    for (LogName filter : new LogName[] { LogName.DEBUGOUT, LogName.PROFILE }) {
      LogFileDetail l = new LogFileDetail();
      l.location = loc;
      l.length = new File(l.location, filter.toString()).length();
      l.start = 0;
      allLogsFileDetails.put(filter, l);
    }
    str = fis.readLine();
    while (str != null) {
      LogFileDetail l = new LogFileDetail();
      l.location = loc;
      int idx = str.indexOf(':');
      LogName filter = LogName.valueOf(str.substring(0, idx).toUpperCase());
      str = str.substring(idx + 1);
      String[] startAndLen = str.split(" ");
      l.start = Long.parseLong(startAndLen[0]);

      l.length = Long.parseLong(startAndLen[1]);
      if (l.length == -1L) {
        l.length = new File(l.location, filter.toString()).length();
      }

      allLogsFileDetails.put(filter, l);
      str = fis.readLine();
    }
    fis.close();
    return allLogsFileDetails;
  }
  
  private static File getTmpIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.tmp");
  }

  static File getIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.index");
  }

  /**
   * Obtain the owner of the log dir. This is 
   * determined by checking the job's log directory.
   */
  static String obtainLogDirOwner(TaskAttemptID taskid) throws IOException {
    Configuration conf = new Configuration();
    FileSystem raw = FileSystem.getLocal(conf).getRaw();
    Path jobLogDir = new Path(getJobDir(taskid.getJobID()).getAbsolutePath());
    FileStatus jobStat = raw.getFileStatus(jobLogDir);
    return jobStat.getOwner();
  }

  static String getBaseLogDir() {
    return System.getProperty("hadoop.log.dir");
  }

  static File getAttemptDir(TaskAttemptID taskid, boolean isCleanup) {
    String cleanupSuffix = isCleanup ? ".cleanup" : "";
    return getAttemptDir(taskid.getJobID().toString(), 
        taskid.toString() + cleanupSuffix);
  }
  
  static File getAttemptDir(String jobid, String taskid) {
    // taskid should be fully formed and it should have the optional 
    // .cleanup suffix
    // TODO(todd) should this have cleanup suffix?
    return new File(getJobDir(jobid), taskid);
  }

  static final List<LogName> LOGS_TRACKED_BY_INDEX_FILES =
      Arrays.asList(LogName.STDOUT, LogName.STDERR, LogName.SYSLOG);

  private static TaskAttemptID currentTaskid;

  /**
   * Map to store previous and current lengths.
   */
  private static Map<LogName, Long[]> logLengths =
      new HashMap<LogName, Long[]>();
  static {
    for (LogName logName : LOGS_TRACKED_BY_INDEX_FILES) {
      logLengths.put(logName, new Long[] { Long.valueOf(0L),
          Long.valueOf(0L) });
    }
  }
  
  static void writeToIndexFile(String logLocation,
      TaskAttemptID currentTaskid, boolean isCleanup,
      Map<LogName, Long[]> lengths) throws IOException {
    // To ensure atomicity of updates to index file, write to temporary index
    // file first and then rename.
    File tmpIndexFile = getTmpIndexFile(currentTaskid, isCleanup);
    
    BufferedOutputStream bos = 
      new BufferedOutputStream(
        SecureIOUtils.createForWrite(tmpIndexFile, 0644));
    DataOutputStream dos = new DataOutputStream(bos);
    //the format of the index file is
    //LOG_DIR: <the dir where the task logs are really stored>
    //STDOUT: <start-offset in the stdout file> <length>
    //STDERR: <start-offset in the stderr file> <length>
    //SYSLOG: <start-offset in the syslog file> <length>    
    dos.writeBytes(LogFileDetail.LOCATION
        + logLocation
        + "\n");
    for (LogName logName : LOGS_TRACKED_BY_INDEX_FILES) {
      Long[] lens = lengths.get(logName);
      dos.writeBytes(logName.toString() + ":"
          + lens[0].toString() + " "
          + Long.toString(lens[1].longValue() - lens[0].longValue())
          + "\n");}
    dos.close();

    File indexFile = getIndexFile(currentTaskid, isCleanup);
    Path indexFilePath = new Path(indexFile.getAbsolutePath());
    Path tmpIndexFilePath = new Path(tmpIndexFile.getAbsolutePath());

    if (localFS == null) {// set localFS once
      localFS = FileSystem.getLocal(new Configuration());
    }
    localFS.rename (tmpIndexFilePath, indexFilePath);
  }

  @SuppressWarnings("unchecked")
  public synchronized static void syncLogs(String logLocation, 
                                           TaskAttemptID taskid,
                                           boolean isCleanup,
                                           boolean segmented) 
  throws IOException {
    System.out.flush();
    System.err.flush();
    Enumeration<Logger> allLoggers = LogManager.getCurrentLoggers();
    while (allLoggers.hasMoreElements()) {
      Logger l = allLoggers.nextElement();
      Enumeration<Appender> allAppenders = l.getAllAppenders();
      while (allAppenders.hasMoreElements()) {
        Appender a = allAppenders.nextElement();
        if (a instanceof TaskLogAppender) {
          ((TaskLogAppender)a).flush();
        }
      }
    }
    if (currentTaskid == null) {
      currentTaskid = taskid;
    }
    // set start and end
    for (LogName logName : LOGS_TRACKED_BY_INDEX_FILES) {
      if (currentTaskid != taskid) {
        // Set start = current-end
        logLengths.get(logName)[0] = Long.valueOf(new File(
            logLocation, logName.toString()).length());
      }
      // Set current end
      logLengths.get(logName)[1]
        = (segmented
           ? (Long.valueOf
              (new File(logLocation, logName.toString()).length()))
           : -1);
    }
    if (currentTaskid != taskid) {
      if (currentTaskid != null) {
        LOG.info("Starting logging for a new task " + taskid
            + " in the same JVM as that of the first task " + logLocation);
      }
      currentTaskid = taskid;
    }
    writeToIndexFile(logLocation, taskid, isCleanup, logLengths);
  }
  
  /**
   * The filter for userlogs.
   */
  public static enum LogName {
    /** Log on the stdout of the task. */
    STDOUT ("stdout"),

    /** Log on the stderr of the task. */
    STDERR ("stderr"),
    
    /** Log on the map-reduce system logs of the task. */
    SYSLOG ("syslog"),
    
    /** The java profiler information. */
    PROFILE ("profile.out"),
    
    /** Log the debug script's stdout  */
    DEBUGOUT ("debugout");
        
    private String prefix;
    
    private LogName(String prefix) {
      this.prefix = prefix;
    }
    
    @Override
    public String toString() {
      return prefix;
    }
  }

  static class Reader extends InputStream {
    private long bytesRemaining;
    private FileInputStream file;

    /**
     * Read a log file from start to end positions. The offsets may be negative,
     * in which case they are relative to the end of the file. For example,
     * Reader(taskid, kind, 0, -1) is the entire file and 
     * Reader(taskid, kind, -4197, -1) is the last 4196 bytes. 
     * @param taskid the id of the task to read the log file for
     * @param kind the kind of log to read
     * @param start the offset to read from (negative is relative to tail)
     * @param end the offset to read upto (negative is relative to tail)
     * @param isCleanup whether the attempt is cleanup attempt or not
     * @throws IOException
     */
    public Reader(TaskAttemptID taskid, LogName kind, 
                  long start, long end, boolean isCleanup) throws IOException {
      // find the right log file
      Map<LogName, LogFileDetail> allFilesDetails =
          getAllLogsFileDetails(taskid, isCleanup);
      LogFileDetail fileDetail = allFilesDetails.get(kind);
      // calculate the start and stop
      long size = fileDetail.length;
      if (start < 0) {
        start += size + 1;
      }
      if (end < 0) {
        end += size + 1;
      }
      start = Math.max(0, Math.min(start, size));
      end = Math.max(0, Math.min(end, size));
      start += fileDetail.start;
      end += fileDetail.start;
      bytesRemaining = end - start;
      String owner = obtainLogDirOwner(taskid);
      file = SecureIOUtils.openForRead(new File(fileDetail.location, kind.toString()), 
          owner);
      // skip upto start
      long pos = 0;
      while (pos < start) {
        long result = file.skip(start - pos);
        if (result < 0) {
          bytesRemaining = 0;
          break;
        }
        pos += result;
      }
    }
    
    @Override
    public int read() throws IOException {
      int result = -1;
      if (bytesRemaining > 0) {
        bytesRemaining -= 1;
        result = file.read();
      }
      return result;
    }
    
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      length = (int) Math.min(length, bytesRemaining);
      int bytes = file.read(buffer, offset, length);
      if (bytes > 0) {
        bytesRemaining -= bytes;
      }
      return bytes;
    }
    
    @Override
    public int available() throws IOException {
      return (int) Math.min(bytesRemaining, file.available());
    }

    @Override
    public void close() throws IOException {
      file.close();
    }
  }

  private static final String bashCommand = "bash";
  private static final String tailCommand = "tail";
  
  /**
   * Get the desired maximum length of task's logs.
   * @param conf the job to look in
   * @return the number of bytes to cap the log files at
   */
  public static long getTaskLogLength(JobConf conf) {
    return conf.getLong("mapred.userlog.limit.kb", 100) * 1024;
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * If the tailLength is 0, the entire output will be saved.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength
                                               ) throws IOException {
    return captureOutAndError(null, cmd, stdoutFilename,
                              stderrFilename, tailLength, false);
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> setup,
                                                List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength
                                               ) throws IOException {
    return captureOutAndError(setup, cmd, stdoutFilename, stderrFilename,
        tailLength, false);
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @deprecated pidFiles are no more used. Instead pid is exported to
   *             env variable JVM_PID.
   * @return the modified command that should be run
   */
  @Deprecated
  public static List<String> captureOutAndError(List<String> setup,
                                                List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength,
                                                String pidFileName
                                               ) throws IOException {
    return captureOutAndError(setup, cmd, stdoutFilename, stderrFilename,
        tailLength, false, pidFileName);
  }
  
  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @param useSetsid Should setsid be used in the command or not.
   * @deprecated pidFiles are no more used. Instead pid is exported to
   *             env variable JVM_PID.
   * @return the modified command that should be run
   * 
   */
  @Deprecated
  public static List<String> captureOutAndError(List<String> setup,
      List<String> cmd, 
      File stdoutFilename,
      File stderrFilename,
      long tailLength,
      boolean useSetsid,
      String pidFileName
     ) throws IOException {
    return captureOutAndError(setup,cmd, stdoutFilename, stderrFilename, tailLength,
        useSetsid);
  }

  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @param useSetsid Should setsid be used in the command or not.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> setup,
      List<String> cmd, 
      File stdoutFilename,
      File stderrFilename,
      long tailLength,
      boolean useSetsid
     ) throws IOException {
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    String mergedCmd = buildCommandLine(setup,
        cmd,
        stdoutFilename,
        stderrFilename, tailLength,
        useSetsid);
    result.add(mergedCmd.toString());
    return result; 
  }
  
  
  static String buildCommandLine(List<String> setup,
      List<String> cmd, 
      File stdoutFilename,
      File stderrFilename,
      long tailLength,
      boolean useSetSid) throws IOException {
    
    String stdout = FileUtil.makeShellPath(stdoutFilename);
    String stderr = FileUtil.makeShellPath(stderrFilename);
    StringBuilder mergedCmd = new StringBuilder();
    
    if (!Shell.WINDOWS) {
      mergedCmd.append("export JVM_PID=`echo $$`\n");
    }

    if (setup != null) {
      for (String s : setup) {
        mergedCmd.append(s);
        mergedCmd.append("\n");
      }
    }
    if (tailLength > 0) {
      mergedCmd.append("(");
    } else if (ProcessTree.isSetsidAvailable && useSetSid 
        && !Shell.WINDOWS) {
      mergedCmd.append("exec setsid ");
    } else {
      mergedCmd.append("exec ");
    }
    mergedCmd.append(addCommand(cmd, true));
    mergedCmd.append(" < /dev/null ");
    if (tailLength > 0) {
      mergedCmd.append(" | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      mergedCmd.append(" >> ");
      mergedCmd.append(stdout);
      mergedCmd.append(" ; exit $PIPESTATUS ) 2>&1 | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      mergedCmd.append(" >> ");
      mergedCmd.append(stderr);
      mergedCmd.append(" ; exit $PIPESTATUS");
    } else {
      mergedCmd.append(" 1>> ");
      mergedCmd.append(stdout);
      mergedCmd.append(" 2>> ");
      mergedCmd.append(stderr);
    }
    return mergedCmd.toString();
  }

  /**
   * Add quotes to each of the command strings and
   * return as a single string 
   * @param cmd The command to be quoted
   * @param isExecutable makes shell path if the first 
   * argument is executable
   * @return returns The quoted string. 
   * @throws IOException
   */
  public static String addCommand(List<String> cmd, boolean isExecutable) 
  throws IOException {
    StringBuffer command = new StringBuffer();
    for(String s: cmd) {
    	command.append('\'');
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
    	  command.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
    	  command.append(s);
      }
      command.append('\'');
      command.append(" ");
    }
    return command.toString();
  }
  
  /**
   * Wrap a command in a shell to capture debug script's 
   * stdout and stderr to debugout.
   * @param cmd The command and the arguments that should be run
   * @param debugoutFilename The filename that stdout and stderr
   *  should be saved to.
   * @return the modified command that should be run
   * @throws IOException
   */
  public static List<String> captureDebugOut(List<String> cmd, 
                                             File debugoutFilename
                                            ) throws IOException {
    String debugout = FileUtil.makeShellPath(debugoutFilename);
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    StringBuffer mergedCmd = new StringBuffer();
    mergedCmd.append("exec ");
    boolean isExecutable = true;
    for(String s: cmd) {
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
        mergedCmd.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
        mergedCmd.append(s);
      }
      mergedCmd.append(" ");
    }
    mergedCmd.append(" < /dev/null ");
    mergedCmd.append(" >");
    mergedCmd.append(debugout);
    mergedCmd.append(" 2>&1 ");
    result.add(mergedCmd.toString());
    return result;
  }
  
  public static File getUserLogDir() {  
    return LOG_DIR;
  }
  
  /**
   * Get the user log directory for the job jobid.
   * 
   * @param jobid string representation of the jobid
   * @return user log directory for the job
   */
  public static File getJobDir(String jobid) {
    return new File(getUserLogDir(), jobid);
  }
  
  /**
   * Get the user log directory for the job jobid.
   * 
   * @param jobid the jobid object
   * @return user log directory for the job
   */
  public static File getJobDir(JobID jobid) {
    return getJobDir(jobid.toString());
  }

} // TaskLog
