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
package org.apache.hadoop.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Map-reduce program to recursively change files properties
 * such as owner, group and permission.
 */
public class DistCh extends DistTool {
  static final String NAME = "distch";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String OP_LIST_LABEL = NAME + ".op.list";
  static final String OP_COUNT_LABEL = NAME + ".op.count";

  static final String USAGE = "java " + DistCh.class.getName() 
      + " [OPTIONS] <path:owner:group:permission>+ "

      + "\n\nThe values of owner, group and permission can be empty."
      + "\nPermission is a octal number."

      + "\n\nOPTIONS:"
      + "\n-f <urilist_uri>       Use list at <urilist_uri> as src list"
      + "\n-i                     Ignore failures"
      + "\n-log <logdir>          Write logs to <logdir>"
      ;

  private static final long OP_PER_MAP =  1000;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final int SYNC_FILE_MAX = 10;

  static enum Counter { SUCCEED, FAIL }

  static enum Option {
    IGNORE_FAILURES("-i", NAME + ".ignore.failures");

    final String cmd, propertyname;

    private Option(String cmd, String propertyname) {
      this.cmd = cmd;
      this.propertyname = propertyname;
    }
  }

  DistCh(Configuration conf) {
    super(createJobConf(conf));
  }

  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, DistCh.class);
    jobconf.setJobName(NAME);
    jobconf.setMapSpeculativeExecution(false);

    jobconf.setInputFormat(ChangeInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(ChangeFilesMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  /** File operations. */
  static class FileOperation implements Writable {
    private Path src;
    private String owner;
    private String group;
    private FsPermission permission;

    FileOperation() {}

    FileOperation(Path src, FileOperation that) {
      this.src = src;
      this.owner = that.owner;
      this.group = that.group;
      this.permission = that.permission;
      checkState();
    }

    /**
     * path:owner:group:permission
     * e.g.
     * /user/foo:foo:bar:700 
     */
    FileOperation(String line) {
      try {
        String[] t = line.split(":", 4);
        for(int i = 0; i < t.length; i++) {
          if ("".equals(t[i])) {
            t[i] = null;
          }
        }

        src = new Path(t[0]);
        owner = t[1];
        group = t[2];
        permission = t[3] == null? null:
          new FsPermission(Short.parseShort(t[3], 8));

        checkState();
      }
      catch(Exception e) {
        throw (IllegalArgumentException)new IllegalArgumentException(
            "line=" + line).initCause(e);
      }
    }

    private void checkState() throws IllegalStateException {
      if (owner == null && group == null && permission == null) {
        throw new IllegalStateException(
            "owner == null && group == null && permission == null");
      }
    }

    static final FsPermission FILE_UMASK
        = FsPermission.createImmutable((short)0111);

    private boolean isDifferent(FileStatus original) {
      if (owner != null && !owner.equals(original.getOwner())) {
        return true;
      }
      if (group != null && !group.equals(original.getGroup())) {
        return true;
      }
      if (permission != null) {
        FsPermission orig = original.getPermission();
        return original.isDir()? !permission.equals(orig):
          !permission.applyUMask(FILE_UMASK).equals(orig);
      }
      return false;
    }

    void run(Configuration conf) throws IOException {
      FileSystem fs = src.getFileSystem(conf);
      if (permission != null) {
        fs.setPermission(src, permission);
      }
      if (owner != null || group != null) {
        fs.setOwner(src, owner, group);
      }
    }

    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException {
      this.src = new Path(Text.readString(in));
      owner = DistTool.readString(in);
      group = DistTool.readString(in);
      permission = in.readBoolean()? FsPermission.read(in): null;
    }

    /** {@inheritDoc} */
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, src.toString());
      DistTool.writeString(out, owner);
      DistTool.writeString(out, group);

      boolean b = permission != null;
      out.writeBoolean(b);
      if (b) {permission.write(out);}
    }

    /** {@inheritDoc} */
    public String toString() {
      return src + ":" + owner + ":" + group + ":" + permission; 
    }
  }

  /** Responsible for generating splits of the src file list. */
  static class ChangeInputFormat implements InputFormat<Text, FileOperation> {
    /** Do nothing. */
    public void validateInput(JobConf job) {}

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * @param job The handle to the JobConf object
     * @param numSplits Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits
        ) throws IOException {
      final int srcCount = job.getInt(OP_COUNT_LABEL, -1);
      final int targetcount = srcCount / numSplits;
      String srclist = job.get(OP_LIST_LABEL, "");
      if (srcCount < 0 || "".equals(srclist)) {
        throw new RuntimeException("Invalid metadata: #files(" + srcCount +
                                   ") listuri(" + srclist + ")");
      }
      Path srcs = new Path(srclist);
      FileSystem fs = srcs.getFileSystem(job);

      List<FileSplit> splits = new ArrayList<FileSplit>(numSplits);

      Text key = new Text();
      FileOperation value = new FileOperation();
      SequenceFile.Reader in = null;
      long prev = 0L;
      int count = 0; //count src
      try {
        for(in = new SequenceFile.Reader(fs, srcs, job); in.next(key, value); ) {
          long curr = in.getPosition();
          long delta = curr - prev;
          if (++count > targetcount) {
            count = 0;
            splits.add(new FileSplit(srcs, prev, delta, (String[])null));
            prev = curr;
          }
        }
      }
      finally {
        in.close();
      }
      long remaining = fs.getFileStatus(srcs).getLen() - prev;
      if (remaining != 0) {
        splits.add(new FileSplit(srcs, prev, remaining, (String[])null));
      }
      LOG.info("numSplits="  + numSplits + ", splits.size()=" + splits.size());
      return splits.toArray(new FileSplit[splits.size()]);
    }

    /** {@inheritDoc} */
    public RecordReader<Text, FileOperation> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, FileOperation>(job,
          (FileSplit)split);
    }
  }

  /** The mapper for changing files. */
  static class ChangeFilesMapper 
      implements Mapper<Text, FileOperation, WritableComparable<?>, Text> {
    private JobConf jobconf;
    private boolean ignoreFailures;

    private int failcount = 0;
    private int succeedcount = 0;

    private String getCountString() {
      return "Succeeded: " + succeedcount + " Failed: " + failcount;
    }

    /** {@inheritDoc} */
    public void configure(JobConf job) {
      this.jobconf = job;
      ignoreFailures=job.getBoolean(Option.IGNORE_FAILURES.propertyname,false);
    }

    /** Run a FileOperation */
    public void map(Text key, FileOperation value,
        OutputCollector<WritableComparable<?>, Text> out, Reporter reporter
        ) throws IOException {
      try {
        value.run(jobconf);
        ++succeedcount;
        reporter.incrCounter(Counter.SUCCEED, 1);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FAIL, 1);

        String s = "FAIL: " + value + ", " + StringUtils.stringifyException(e);
        out.collect(null, new Text(s));
        LOG.info(s);
      } finally {
        reporter.setStatus(getCountString());
      }
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
      if (failcount == 0 || ignoreFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }

  private static void check(Configuration conf, List<FileOperation> ops
      ) throws InvalidInputException {
    List<Path> srcs = new ArrayList<Path>();
    for(FileOperation op : ops) {
      srcs.add(op.src);
    }
    DistTool.checkSource(conf, srcs);
  }

  private static List<FileOperation> fetchList(Configuration conf, Path inputfile
      ) throws IOException {
    List<FileOperation> result = new ArrayList<FileOperation>();
    for(String line : readFile(conf, inputfile)) {
      result.add(new FileOperation(line));
    }
    return result;
  }

  /** This is the main driver for recursively changing files properties. */
  public int run(String[] args) throws Exception {
    List<FileOperation> ops = new ArrayList<FileOperation>();
    Path logpath = null;
    boolean isIgnoreFailures = false;

    try {
      for (int idx = 0; idx < args.length; idx++) {
        if ("-f".equals(args[idx])) {
          if (++idx ==  args.length) {
            System.out.println("urilist_uri not specified");
            System.out.println(USAGE);
            return -1;
          }
          ops.addAll(fetchList(jobconf, new Path(args[idx])));
        } else if (Option.IGNORE_FAILURES.cmd.equals(args[idx])) {
          isIgnoreFailures = true;
        } else if ("-log".equals(args[idx])) {
          if (++idx ==  args.length) {
            System.out.println("logdir not specified");
            System.out.println(USAGE);
            return -1;
          }
          logpath = new Path(args[idx]);
        } else if ('-' == args[idx].codePointAt(0)) {
          System.out.println("Invalid switch " + args[idx]);
          System.out.println(USAGE);
          ToolRunner.printGenericCommandUsage(System.out);
          return -1;
        } else {
          ops.add(new FileOperation(args[idx]));
        }
      }
      // mandatory command-line parameters
      if (ops.isEmpty()) {
        throw new IllegalStateException("Operation is empty");
      }
      LOG.info("ops=" + ops);
      LOG.info("isIgnoreFailures=" + isIgnoreFailures);
      jobconf.setBoolean(Option.IGNORE_FAILURES.propertyname, isIgnoreFailures);
      check(jobconf, ops);

      try {
        if (setup(ops, logpath)) {
          JobClient.runJob(jobconf);
        }
      } finally {
        try {
          if (logpath == null) {
            //delete log directory
            final Path logdir = FileOutputFormat.getOutputPath(jobconf);
            if (logdir != null) {
              logdir.getFileSystem(jobconf).delete(logdir, true);
            }
          }
        }
        finally {
          //delete job directory
          final String jobdir = jobconf.get(JOB_DIR_LABEL);
          if (jobdir != null) {
            final Path jobpath = new Path(jobdir);
            jobpath.getFileSystem(jobconf).delete(jobpath, true);
          }
        }
      }
    } catch(DuplicationException e) {
      LOG.error("Input error:", e);
      return DuplicationException.ERROR_CODE;
    } catch(Exception e) {
      LOG.error(NAME + " failed: ", e);
      System.out.println(USAGE);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    return 0;
  }

  /** Calculate how many maps to run. */
  private static int getMapCount(int srcCount, int numNodes) {
    int numMaps = (int)(srcCount / OP_PER_MAP);
    numMaps = Math.min(numMaps, numNodes * MAX_MAPS_PER_NODE);
    return Math.max(numMaps, 1);
  }

  private boolean setup(List<FileOperation> ops, Path log) throws IOException {
    final String randomId = getRandomId();
    JobClient jClient = new JobClient(jobconf);
    Path stagingArea;
    try {
      stagingArea = JobSubmissionFiles.getStagingDir(
                       jClient, jobconf);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    Path jobdir = new Path(stagingArea + NAME + "_" + randomId);
    FsPermission mapredSysPerms =
      new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jClient.getFs(), jobdir, mapredSysPerms);
    LOG.info(JOB_DIR_LABEL + "=" + jobdir);

    if (log == null) {
      log = new Path(jobdir, "_logs");
    }
    FileOutputFormat.setOutputPath(jobconf, log);
    LOG.info("log=" + log);

    //create operation list
    FileSystem fs = jobdir.getFileSystem(jobconf);
    Path opList = new Path(jobdir, "_" + OP_LIST_LABEL);
    jobconf.set(OP_LIST_LABEL, opList.toString());
    int opCount = 0, synCount = 0;
    SequenceFile.Writer opWriter = null;
    try {
      opWriter = SequenceFile.createWriter(fs, jobconf, opList, Text.class,
          FileOperation.class, SequenceFile.CompressionType.NONE);
      for(FileOperation op : ops) {
        FileStatus srcstat = fs.getFileStatus(op.src); 
        if (srcstat.isDir() && op.isDifferent(srcstat)) {
          ++opCount;
          opWriter.append(new Text(op.src.toString()), op);
        }

        Stack<Path> pathstack = new Stack<Path>();
        for(pathstack.push(op.src); !pathstack.empty(); ) {
          for(FileStatus stat : fs.listStatus(pathstack.pop())) {
            if (stat.isDir()) {
              pathstack.push(stat.getPath());
            }

            if (op.isDifferent(stat)) {              
              ++opCount;
              if (++synCount > SYNC_FILE_MAX) {
                opWriter.sync();
                synCount = 0;
              }
              Path f = stat.getPath();
              opWriter.append(new Text(f.toString()), new FileOperation(f, op));
            }
          }
        }
      }
    } finally {
      opWriter.close();
    }

    checkDuplication(fs, opList, new Path(jobdir, "_sorted"), jobconf);
    jobconf.setInt(OP_COUNT_LABEL, opCount);
    LOG.info(OP_COUNT_LABEL + "=" + opCount);
    jobconf.setNumMapTasks(getMapCount(opCount,
        new JobClient(jobconf).getClusterStatus().getTaskTrackers()));
    return opCount != 0;    
  }

  private static void checkDuplication(FileSystem fs, Path file, Path sorted,
    Configuration conf) throws IOException {
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
        new Text.Comparator(), Text.class, FileOperation.class, conf);
    sorter.sort(file, sorted);
    SequenceFile.Reader in = null;
    try {
      in = new SequenceFile.Reader(fs, sorted, conf);
      FileOperation curop = new FileOperation();
      Text prevsrc = null, cursrc = new Text(); 
      for(; in.next(cursrc, curop); ) {
        if (prevsrc != null && cursrc.equals(prevsrc)) {
          throw new DuplicationException(
            "Invalid input, there are duplicated files in the sources: "
            + prevsrc + ", " + cursrc);
        }
        prevsrc = cursrc;
        cursrc = new Text();
        curop = new FileOperation();
      }
    }
    finally {
      in.close();
    }
  } 

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new DistCh(new Configuration()), args));
  }
}