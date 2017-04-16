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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A Map-reduce program to recursively copy directories between
 * different file-systems.
 */
public class DistCp implements Tool {
  public static final Log LOG = LogFactory.getLog(DistCp.class);

  private static final String NAME = "distcp";

  private static final String usage = NAME
    + " [OPTIONS] <srcurl>* <desturl>" +
    "\n\nOPTIONS:" +
    "\n-p[rbugp]              Preserve status" +
    "\n                       r: replication number" +
    "\n                       b: block size" +
    "\n                       u: user" + 
    "\n                       g: group" +
    "\n                       p: permission" +
    "\n                       -p alone is equivalent to -prbugp" +
    "\n-i                     Ignore failures" +
    "\n-log <logdir>          Write logs to <logdir>" +
    "\n-m <num_maps>          Maximum number of simultaneous copies" +
    "\n-overwrite             Overwrite destination" +
    "\n-update                Overwrite if src size different from dst size" +
    "\n-skipcrccheck          Do not use CRC check to determine if src is " +
    "\n                       different from dest. Relevant only if -update" +
    "\n                       is specified" +
    "\n-f <urilist_uri>       Use list at <urilist_uri> as src list" +
    "\n-filelimit <n>         Limit the total number of files to be <= n" +
    "\n-sizelimit <n>         Limit the total size to be <= n bytes" +
    "\n-delete                Delete the files existing in the dst but not in src" +
    "\n-mapredSslConf <f>     Filename of SSL configuration for mapper task" +
    
    "\n\nNOTE 1: if -overwrite or -update are set, each source URI is " +
    "\n      interpreted as an isomorphic update to an existing directory." +
    "\nFor example:" +
    "\nhadoop " + NAME + " -p -update \"hdfs://A:8020/user/foo/bar\" " +
    "\"hdfs://B:8020/user/foo/baz\"\n" +
    "\n     would update all descendants of 'baz' also in 'bar'; it would " +
    "\n     *not* update /user/foo/baz/bar" + 

    "\n\nNOTE 2: The parameter <n> in -filelimit and -sizelimit can be " +
    "\n     specified with symbolic representation.  For examples," +
    "\n       1230k = 1230 * 1024 = 1259520" +
    "\n       891g = 891 * 1024^3 = 956703965184" +
    
    "\n";
  
  private static final long BYTES_PER_MAP =  256 * 1024 * 1024;
  private static final int MAX_MAPS_PER_NODE = 20;
  private static final int SYNC_FILE_MAX = 10;

  static enum Counter { COPY, SKIP, FAIL, BYTESCOPIED, BYTESEXPECTED }
  static enum Options {
    DELETE("-delete", NAME + ".delete"),
    FILE_LIMIT("-filelimit", NAME + ".limit.file"),
    SIZE_LIMIT("-sizelimit", NAME + ".limit.size"),
    IGNORE_READ_FAILURES("-i", NAME + ".ignore.read.failures"),
    PRESERVE_STATUS("-p", NAME + ".preserve.status"),
    OVERWRITE("-overwrite", NAME + ".overwrite.always"),
    UPDATE("-update", NAME + ".overwrite.ifnewer"),
    SKIPCRC("-skipcrccheck", NAME + ".skip.crc.check");

    final String cmd, propertyname;

    private Options(String cmd, String propertyname) {
      this.cmd = cmd;
      this.propertyname = propertyname;
    }
    
    private long parseLong(String[] args, int offset) {
      if (offset ==  args.length) {
        throw new IllegalArgumentException("<n> not specified in " + cmd);
      }
      long n = StringUtils.TraditionalBinaryPrefix.string2long(args[offset]);
      if (n <= 0) {
        throw new IllegalArgumentException("n = " + n + " <= 0 in " + cmd);
      }
      return n;
    }
  }
  static enum FileAttribute {
    BLOCK_SIZE, REPLICATION, USER, GROUP, PERMISSION;

    final char symbol;

    private FileAttribute() {symbol = toString().toLowerCase().charAt(0);}
    
    static EnumSet<FileAttribute> parse(String s) {
      if (s == null || s.length() == 0) {
        return EnumSet.allOf(FileAttribute.class);
      }

      EnumSet<FileAttribute> set = EnumSet.noneOf(FileAttribute.class);
      FileAttribute[] attributes = values();
      for(char c : s.toCharArray()) {
        int i = 0;
        for(; i < attributes.length && c != attributes[i].symbol; i++);
        if (i < attributes.length) {
          if (!set.contains(attributes[i])) {
            set.add(attributes[i]);
          } else {
            throw new IllegalArgumentException("There are more than one '"
                + attributes[i].symbol + "' in " + s); 
          }
        } else {
          throw new IllegalArgumentException("'" + c + "' in " + s
              + " is undefined.");
        }
      }
      return set;
    }
  }

  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String DST_DIR_LABEL = NAME + ".dest.path";
  static final String JOB_DIR_LABEL = NAME + ".job.dir";
  static final String MAX_MAPS_LABEL = NAME + ".max.map.tasks";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String SRC_COUNT_LABEL = NAME + ".src.count";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
  static final String DST_DIR_LIST_LABEL = NAME + ".dst.dir.list";
  static final String BYTES_PER_MAP_LABEL = NAME + ".bytes.per.map";
  static final String PRESERVE_STATUS_LABEL
      = Options.PRESERVE_STATUS.propertyname + ".value";

  private JobConf conf;

  public void setConf(Configuration conf) {
    if (conf instanceof JobConf) {
      this.conf = (JobConf) conf;
    } else {
      this.conf = new JobConf(conf);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public DistCp(Configuration conf) {
    setConf(conf);
  }

  /**
   * An input/output pair of filenames.
   */
  static class FilePair implements Writable {
    FileStatus input = new FileStatus();
    String output;
    FilePair() { }
    FilePair(FileStatus input, String output) {
      this.input = input;
      this.output = output;
    }
    public void readFields(DataInput in) throws IOException {
      input.readFields(in);
      output = Text.readString(in);
    }
    public void write(DataOutput out) throws IOException {
      input.write(out);
      Text.writeString(out, output);
    }
    public String toString() {
      return input + " : " + output;
    }
  }

  /**
   * InputFormat of a distcp job responsible for generating splits of the src
   * file list.
   */
  static class CopyInputFormat implements InputFormat<Text, Text> {

    /**
     * Produce splits such that each is no greater than the quotient of the
     * total size and the number of splits requested.
     * @param job The handle to the JobConf object
     * @param numSplits Number of splits requested
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      int cnfiles = job.getInt(SRC_COUNT_LABEL, -1);
      long cbsize = job.getLong(TOTAL_SIZE_LABEL, -1);
      String srcfilelist = job.get(SRC_LIST_LABEL, "");
      if (cnfiles < 0 || cbsize < 0 || "".equals(srcfilelist)) {
        throw new RuntimeException("Invalid metadata: #files(" + cnfiles +
                                   ") total_size(" + cbsize + ") listuri(" +
                                   srcfilelist + ")");
      }
      Path src = new Path(srcfilelist);
      FileSystem fs = src.getFileSystem(job);
      FileStatus srcst = fs.getFileStatus(src);

      ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
      LongWritable key = new LongWritable();
      FilePair value = new FilePair();
      final long targetsize = cbsize / numSplits;
      long pos = 0L;
      long last = 0L;
      long acc = 0L;
      long cbrem = srcst.getLen();
      SequenceFile.Reader sl = null;
      try {
        sl = new SequenceFile.Reader(fs, src, job);
        for (; sl.next(key, value); last = sl.getPosition()) {
          // if adding this split would put this split past the target size,
          // cut the last split and put this next file in the next split.
          if (acc + key.get() > targetsize && acc != 0) {
            long splitsize = last - pos;
            splits.add(new FileSplit(src, pos, splitsize, (String[])null));
            cbrem -= splitsize;
            pos = last;
            acc = 0L;
          }
          acc += key.get();
        }
      }
      finally {
        checkAndClose(sl);
      }
      if (cbrem != 0) {
        splits.add(new FileSplit(src, pos, cbrem, (String[])null));
      }

      return splits.toArray(new FileSplit[splits.size()]);
    }

    /**
     * Returns a reader for this split of the src file list.
     */
    public RecordReader<Text, Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      return new SequenceFileRecordReader<Text, Text>(job, (FileSplit)split);
    }
  }

  /**
   * FSCopyFilesMapper: The mapper for copying files between FileSystems.
   */
  static class CopyFilesMapper
      implements Mapper<LongWritable, FilePair, WritableComparable<?>, Text> {
    // config
    private int sizeBuf = 128 * 1024;
    private FileSystem destFileSys = null;
    private boolean ignoreReadFailures;
    private boolean preserve_status;
    private EnumSet<FileAttribute> preseved;
    private boolean overwrite;
    private boolean update;
    private Path destPath = null;
    private byte[] buffer = null;
    private JobConf job;
    private boolean skipCRCCheck = false;

    // stats
    private int failcount = 0;
    private int skipcount = 0;
    private int copycount = 0;

    private String getCountString() {
      return "Copied: " + copycount + " Skipped: " + skipcount
          + " Failed: " + failcount;
    }
    private void updateStatus(Reporter reporter) {
      reporter.setStatus(getCountString());
    }

    /**
     * Return true if dst should be replaced by src and the update flag is set.
     * Right now, this merely checks that the src and dst len are not equal. 
     * This should be improved on once modification times, CRCs, etc. can
     * be meaningful in this context.
     * @throws IOException 
     */
    private boolean needsUpdate(FileStatus srcstatus,
        FileSystem dstfs, Path dstpath) throws IOException {
      return update && !sameFile(srcstatus.getPath().getFileSystem(job),
          srcstatus, dstfs, dstpath, skipCRCCheck);
    }
    
    private FSDataOutputStream create(Path f, Reporter reporter,
        FileStatus srcstat) throws IOException {
      if (destFileSys.exists(f)) {
        destFileSys.delete(f, false);
      }
      if (!preserve_status) {
        return destFileSys.create(f, true, sizeBuf, reporter);
      }

      FsPermission permission = preseved.contains(FileAttribute.PERMISSION)?
          srcstat.getPermission(): null;
      short replication = preseved.contains(FileAttribute.REPLICATION)?
          srcstat.getReplication(): destFileSys.getDefaultReplication();
      long blockSize = preseved.contains(FileAttribute.BLOCK_SIZE)?
          srcstat.getBlockSize(): destFileSys.getDefaultBlockSize();
      return destFileSys.create(f, permission, true, sizeBuf, replication,
          blockSize, reporter);
    }

    /**
     * Copy a file to a destination.
     * @param srcstat src path and metadata
     * @param dstpath dst path
     * @param reporter
     */
    private void copy(FileStatus srcstat, Path relativedst,
        OutputCollector<WritableComparable<?>, Text> outc, Reporter reporter)
        throws IOException {
      Path absdst = new Path(destPath, relativedst);
      int totfiles = job.getInt(SRC_COUNT_LABEL, -1);
      assert totfiles >= 0 : "Invalid file count " + totfiles;

      // if a directory, ensure created even if empty
      if (srcstat.isDir()) {
        if (destFileSys.exists(absdst)) {
          if (!destFileSys.getFileStatus(absdst).isDir()) {
            throw new IOException("Failed to mkdirs: " + absdst+" is a file.");
          }
        }
        else if (!destFileSys.mkdirs(absdst)) {
          throw new IOException("Failed to mkdirs " + absdst);
        }
        // TODO: when modification times can be set, directories should be
        // emitted to reducers so they might be preserved. Also, mkdirs does
        // not currently return an error when the directory already exists;
        // if this changes, all directory work might as well be done in reduce
        return;
      }

      if (destFileSys.exists(absdst) && !overwrite
          && !needsUpdate(srcstat, destFileSys, absdst)) {
        outc.collect(null, new Text("SKIP: " + srcstat.getPath()));
        ++skipcount;
        reporter.incrCounter(Counter.SKIP, 1);
        updateStatus(reporter);
        return;
      }

      Path tmpfile = new Path(job.get(TMP_DIR_LABEL), relativedst);
      long cbcopied = 0L;
      FSDataInputStream in = null;
      FSDataOutputStream out = null;
      try {
        // open src file
        in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
        reporter.incrCounter(Counter.BYTESEXPECTED, srcstat.getLen());
        // open tmp file
        out = create(tmpfile, reporter, srcstat);
        // copy file
        for(int cbread; (cbread = in.read(buffer)) >= 0; ) {
          out.write(buffer, 0, cbread);
          cbcopied += cbread;
          reporter.setStatus(
              String.format("%.2f ", cbcopied*100.0/srcstat.getLen())
              + absdst + " [ " +
              StringUtils.humanReadableInt(cbcopied) + " / " +
              StringUtils.humanReadableInt(srcstat.getLen()) + " ]");
        }
      } finally {
        checkAndClose(in);
        checkAndClose(out);
      }

      if (cbcopied != srcstat.getLen()) {
        throw new IOException("File size not matched: copied "
            + bytesString(cbcopied) + " to tmpfile (=" + tmpfile
            + ") but expected " + bytesString(srcstat.getLen()) 
            + " from " + srcstat.getPath());        
      }
      else {
        if (totfiles == 1) {
          // Copying a single file; use dst path provided by user as destination
          // rather than destination directory, if a file
          Path dstparent = absdst.getParent();
          if (!(destFileSys.exists(dstparent) &&
                destFileSys.getFileStatus(dstparent).isDir())) {
            absdst = dstparent;
          }
        }
        if (destFileSys.exists(absdst) &&
            destFileSys.getFileStatus(absdst).isDir()) {
          throw new IOException(absdst + " is a directory");
        }
        if (!destFileSys.mkdirs(absdst.getParent())) {
          throw new IOException("Failed to create parent dir: " + absdst.getParent());
        }
        rename(tmpfile, absdst);

        FileStatus dststat = destFileSys.getFileStatus(absdst);
        if (dststat.getLen() != srcstat.getLen()) {
          destFileSys.delete(absdst, false);
          throw new IOException("File size not matched: copied "
              + bytesString(dststat.getLen()) + " to dst (=" + absdst
              + ") but expected " + bytesString(srcstat.getLen()) 
              + " from " + srcstat.getPath());        
        } 
        updatePermissions(srcstat, dststat);
      }

      // report at least once for each file
      ++copycount;
      reporter.incrCounter(Counter.BYTESCOPIED, cbcopied);
      reporter.incrCounter(Counter.COPY, 1);
      updateStatus(reporter);
    }
    
    /** rename tmp to dst, delete dst if already exists */
    private void rename(Path tmp, Path dst) throws IOException {
      try {
        if (destFileSys.exists(dst)) {
          destFileSys.delete(dst, true);
        }
        if (!destFileSys.rename(tmp, dst)) {
          throw new IOException();
        }
      }
      catch(IOException cause) {
        throw (IOException)new IOException("Fail to rename tmp file (=" + tmp 
            + ") to destination file (=" + dst + ")").initCause(cause);
      }
    }

    private void updatePermissions(FileStatus src, FileStatus dst
        ) throws IOException {
      if (preserve_status) {
        DistCp.updatePermissions(src, dst, preseved, destFileSys);
      }
    }

    static String bytesString(long b) {
      return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

    /** Mapper configuration.
     * Extracts source and destination file system, as well as
     * top-level paths on source and destination directories.
     * Gets the named file systems, to be used later in map.
     */
    public void configure(JobConf job)
    {
      destPath = new Path(job.get(DST_DIR_LABEL, "/"));
      try {
        destFileSys = destPath.getFileSystem(job);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
      sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
      buffer = new byte[sizeBuf];
      ignoreReadFailures = job.getBoolean(Options.IGNORE_READ_FAILURES.propertyname, false);
      preserve_status = job.getBoolean(Options.PRESERVE_STATUS.propertyname, false);
      if (preserve_status) {
        preseved = FileAttribute.parse(job.get(PRESERVE_STATUS_LABEL));
      }
      update = job.getBoolean(Options.UPDATE.propertyname, false);
      overwrite = !update && job.getBoolean(Options.OVERWRITE.propertyname, false);
      skipCRCCheck = job.getBoolean(Options.SKIPCRC.propertyname, false);
      this.job = job;
    }

    /** Map method. Copies one file from source file system to destination.
     * @param key src len
     * @param value FilePair (FileStatus src, Path dst)
     * @param out Log of failed copies
     * @param reporter
     */
    public void map(LongWritable key,
                    FilePair value,
                    OutputCollector<WritableComparable<?>, Text> out,
                    Reporter reporter) throws IOException {
      final FileStatus srcstat = value.input;
      final Path relativedst = new Path(value.output);
      try {
        copy(srcstat, relativedst, out, reporter);
      } catch (IOException e) {
        ++failcount;
        reporter.incrCounter(Counter.FAIL, 1);
        updateStatus(reporter);
        final String sfailure = "FAIL " + relativedst + " : " +
                          StringUtils.stringifyException(e);
        out.collect(null, new Text(sfailure));
        LOG.info(sfailure);
        try {
          for (int i = 0; i < 3; ++i) {
            try {
              final Path tmp = new Path(job.get(TMP_DIR_LABEL), relativedst);
              if (destFileSys.delete(tmp, true))
                break;
            } catch (Throwable ex) {
              // ignore, we are just cleaning up
              LOG.debug("Ignoring cleanup exception", ex);
            }
            // update status, so we don't get timed out
            updateStatus(reporter);
            Thread.sleep(3 * 1000);
          }
        } catch (InterruptedException inte) {
          throw (IOException)new IOException().initCause(inte);
        }
      } finally {
        updateStatus(reporter);
      }
    }

    public void close() throws IOException {
      if (0 == failcount || ignoreReadFailures) {
        return;
      }
      throw new IOException(getCountString());
    }
  }

  private static List<Path> fetchFileList(Configuration conf, Path srcList)
      throws IOException {
    List<Path> result = new ArrayList<Path>();
    FileSystem fs = srcList.getFileSystem(conf);
    BufferedReader input = null;
    try {
      input = new BufferedReader(new InputStreamReader(fs.open(srcList)));
      String line = input.readLine();
      while (line != null) {
        result.add(new Path(line));
        line = input.readLine();
      }
    } finally {
      checkAndClose(input);
    }
    return result;
  }

  @Deprecated
  public static void copy(Configuration conf, String srcPath,
                          String destPath, Path logPath,
                          boolean srcAsList, boolean ignoreReadFailures)
      throws IOException {
    final Path src = new Path(srcPath);
    List<Path> tmp = new ArrayList<Path>();
    if (srcAsList) {
      tmp.addAll(fetchFileList(conf, src));
    } else {
      tmp.add(src);
    }
    EnumSet<Options> flags = ignoreReadFailures
      ? EnumSet.of(Options.IGNORE_READ_FAILURES)
      : EnumSet.noneOf(Options.class);

    final Path dst = new Path(destPath);
    copy(conf, new Arguments(tmp, dst, logPath, flags, null,
        Long.MAX_VALUE, Long.MAX_VALUE, null));
  }

  /** Sanity check for srcPath */
  private static void checkSrcPath(JobConf jobConf, List<Path> srcPaths)
  throws IOException {
    List<IOException> rslt = new ArrayList<IOException>();
    
    Path[] ps = new Path[srcPaths.size()];
    ps = srcPaths.toArray(ps);
    TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(), ps, jobConf);

    for (Path p : srcPaths) {
      FileSystem fs = p.getFileSystem(jobConf);
      if (!fs.exists(p)) {
        rslt.add(new IOException("Input source " + p + " does not exist."));
      }
    }
    if (!rslt.isEmpty()) {
      throw new InvalidInputException(rslt);
    }
  }

  /**
   * Driver to copy srcPath to destPath depending on required protocol.
   * @param args arguments
   */
  static void copy(final Configuration conf, final Arguments args
      ) throws IOException {
    LOG.info("srcPaths=" + args.srcs);
    LOG.info("destPath=" + args.dst);

    JobConf job = createJobConf(conf);
    
    checkSrcPath(job, args.srcs);
    if (args.preservedAttributes != null) {
      job.set(PRESERVE_STATUS_LABEL, args.preservedAttributes);
    }
    if (args.mapredSslConf != null) {
      job.set("dfs.https.client.keystore.resource", args.mapredSslConf);
    }
    
    //Initialize the mapper
    try {
      if (setup(conf, job, args)) {
        JobClient.runJob(job);
      }
      finalize(conf, job, args.dst, args.preservedAttributes);
    } finally {
      //delete tmp
      fullyDelete(job.get(TMP_DIR_LABEL), job);
      //delete jobDirectory
      fullyDelete(job.get(JOB_DIR_LABEL), job);
    }
  }

  private static void updatePermissions(FileStatus src, FileStatus dst,
      EnumSet<FileAttribute> preseved, FileSystem destFileSys
      ) throws IOException {
    String owner = null;
    String group = null;
    if (preseved.contains(FileAttribute.USER)
        && !src.getOwner().equals(dst.getOwner())) {
      owner = src.getOwner();
    }
    if (preseved.contains(FileAttribute.GROUP)
        && !src.getGroup().equals(dst.getGroup())) {
      group = src.getGroup();
    }
    if (owner != null || group != null) {
      destFileSys.setOwner(dst.getPath(), owner, group);
    }
    if (preseved.contains(FileAttribute.PERMISSION)
        && !src.getPermission().equals(dst.getPermission())) {
      destFileSys.setPermission(dst.getPath(), src.getPermission());
    }
  }

  static private void finalize(Configuration conf, JobConf jobconf,
      final Path destPath, String presevedAttributes) throws IOException {
    if (presevedAttributes == null) {
      return;
    }
    EnumSet<FileAttribute> preseved = FileAttribute.parse(presevedAttributes);
    if (!preseved.contains(FileAttribute.USER)
        && !preseved.contains(FileAttribute.GROUP)
        && !preseved.contains(FileAttribute.PERMISSION)) {
      return;
    }

    FileSystem dstfs = destPath.getFileSystem(conf);
    Path dstdirlist = new Path(jobconf.get(DST_DIR_LIST_LABEL));
    SequenceFile.Reader in = null;
    try {
      in = new SequenceFile.Reader(dstdirlist.getFileSystem(jobconf),
          dstdirlist, jobconf);
      Text dsttext = new Text();
      FilePair pair = new FilePair(); 
      for(; in.next(dsttext, pair); ) {
        Path absdst = new Path(destPath, pair.output);
        updatePermissions(pair.input, dstfs.getFileStatus(absdst),
            preseved, dstfs);
      }
    } finally {
      checkAndClose(in);
    }
  }

  static private class Arguments {
    final List<Path> srcs;
    final Path dst;
    final Path log;
    final EnumSet<Options> flags;
    final String preservedAttributes;
    final long filelimit;
    final long sizelimit;
    final String mapredSslConf;
    
    /**
     * Arguments for distcp
     * @param srcs List of source paths
     * @param dst Destination path
     * @param log Log output directory
     * @param flags Command-line flags
     * @param preservedAttributes Preserved attributes 
     * @param filelimit File limit
     * @param sizelimit Size limit
     */
    Arguments(List<Path> srcs, Path dst, Path log,
        EnumSet<Options> flags, String preservedAttributes,
        long filelimit, long sizelimit, String mapredSslConf) {
      this.srcs = srcs;
      this.dst = dst;
      this.log = log;
      this.flags = flags;
      this.preservedAttributes = preservedAttributes;
      this.filelimit = filelimit;
      this.sizelimit = sizelimit;
      this.mapredSslConf = mapredSslConf;
      
      if (LOG.isTraceEnabled()) {
        LOG.trace("this = " + this);
      }
    }

    static Arguments valueOf(String[] args, Configuration conf
        ) throws IOException {
      List<Path> srcs = new ArrayList<Path>();
      Path dst = null;
      Path log = null;
      EnumSet<Options> flags = EnumSet.noneOf(Options.class);
      String presevedAttributes = null;
      String mapredSslConf = null;
      long filelimit = Long.MAX_VALUE;
      long sizelimit = Long.MAX_VALUE;

      for (int idx = 0; idx < args.length; idx++) {
        Options[] opt = Options.values();
        int i = 0;
        for(; i < opt.length && !args[idx].startsWith(opt[i].cmd); i++);

        if (i < opt.length) {
          flags.add(opt[i]);
          if (opt[i] == Options.PRESERVE_STATUS) {
            presevedAttributes =  args[idx].substring(2);         
            FileAttribute.parse(presevedAttributes); //validation
          }
          else if (opt[i] == Options.FILE_LIMIT) {
            filelimit = Options.FILE_LIMIT.parseLong(args, ++idx);
          }
          else if (opt[i] == Options.SIZE_LIMIT) {
            sizelimit = Options.SIZE_LIMIT.parseLong(args, ++idx);
          }
        } else if ("-f".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("urilist_uri not specified in -f");
          }
          srcs.addAll(fetchFileList(conf, new Path(args[idx])));
        } else if ("-log".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("logdir not specified in -log");
          }
          log = new Path(args[idx]);
        } else if ("-mapredSslConf".equals(args[idx])) {
          if (++idx ==  args.length) {
            throw new IllegalArgumentException("ssl conf file not specified in -mapredSslConf");
          }
          mapredSslConf = args[idx];
        } else if ("-m".equals(args[idx])) {
          if (++idx == args.length) {
            throw new IllegalArgumentException("num_maps not specified in -m");
          }
          try {
            conf.setInt(MAX_MAPS_LABEL, Integer.valueOf(args[idx]));
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid argument to -m: " +
                                               args[idx]);
          }
        } else if ('-' == args[idx].codePointAt(0)) {
          throw new IllegalArgumentException("Invalid switch " + args[idx]);
        } else if (idx == args.length -1) {
          dst = new Path(args[idx]);
        } else {
          srcs.add(new Path(args[idx]));
        }
      }
      // mandatory command-line parameters
      if (srcs.isEmpty() || dst == null) {
        throw new IllegalArgumentException("Missing "
            + (dst == null ? "dst path" : "src"));
      }
      // incompatible command-line flags
      final boolean isOverwrite = flags.contains(Options.OVERWRITE);
      final boolean isUpdate = flags.contains(Options.UPDATE);
      final boolean isDelete = flags.contains(Options.DELETE);
      final boolean skipCRC = flags.contains(Options.SKIPCRC);
      
      if (isOverwrite && isUpdate) {
        throw new IllegalArgumentException("Conflicting overwrite policies");
      }
      if (isDelete && !isOverwrite && !isUpdate) {
        throw new IllegalArgumentException(Options.DELETE.cmd
            + " must be specified with " + Options.OVERWRITE + " or "
            + Options.UPDATE + ".");
      }
      if (!isUpdate && skipCRC) {
        throw new IllegalArgumentException(
            Options.SKIPCRC.cmd + " is relevant only with the " +
            Options.UPDATE.cmd + " option");
      }
      return new Arguments(srcs, dst, log, flags, presevedAttributes,
          filelimit, sizelimit, mapredSslConf);
    }
    
    /** {@inheritDoc} */
    public String toString() {
      return getClass().getName() + "{"
          + "\n  srcs = " + srcs 
          + "\n  dst = " + dst 
          + "\n  log = " + log 
          + "\n  flags = " + flags
          + "\n  preservedAttributes = " + preservedAttributes 
          + "\n  filelimit = " + filelimit 
          + "\n  sizelimit = " + sizelimit
          + "\n  mapredSslConf = " + mapredSslConf
          + "\n}"; 
    }
  }

  /**
   * This is the main driver for recursively copying directories
   * across file systems. It takes at least two cmdline parameters. A source
   * URL and a destination URL. It then essentially does an "ls -lR" on the
   * source URL, and writes the output in a round-robin manner to all the map
   * input files. The mapper actually copies the files allotted to it. The
   * reduce is empty.
   */
  public int run(String[] args) {
    try {
      copy(conf, Arguments.valueOf(args, conf));
      return 0;
    } catch (IllegalArgumentException e) {
      System.err.println(StringUtils.stringifyException(e) + "\n" + usage);
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    } catch (DuplicationException e) {
      System.err.println(StringUtils.stringifyException(e));
      return DuplicationException.ERROR_CODE;
    } catch (RemoteException e) {
      final IOException unwrapped = e.unwrapRemoteException(
          FileNotFoundException.class, 
          AccessControlException.class,
          QuotaExceededException.class);
      System.err.println(StringUtils.stringifyException(unwrapped));
      return -3;
    } catch (Exception e) {
      System.err.println("With failures, global counters are inaccurate; " +
          "consider running with -i");
      System.err.println("Copy failed: " + StringUtils.stringifyException(e));
      return -999;
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf job = new JobConf(DistCp.class);
    DistCp distcp = new DistCp(job);
    int res = ToolRunner.run(distcp, args);
    System.exit(res);
  }

  /**
   * Make a path relative with respect to a root path.
   * absPath is always assumed to descend from root.
   * Otherwise returned path is null.
   */
  static String makeRelative(Path root, Path absPath) {
    if (!absPath.isAbsolute()) {
      throw new IllegalArgumentException("!absPath.isAbsolute(), absPath="
          + absPath);
    }
    String p = absPath.toUri().getPath();

    StringTokenizer pathTokens = new StringTokenizer(p, "/");
    for(StringTokenizer rootTokens = new StringTokenizer(
        root.toUri().getPath(), "/"); rootTokens.hasMoreTokens(); ) {
      if (!rootTokens.nextToken().equals(pathTokens.nextToken())) {
        return null;
      }
    }
    StringBuilder sb = new StringBuilder();
    for(; pathTokens.hasMoreTokens(); ) {
      sb.append(pathTokens.nextToken());
      if (pathTokens.hasMoreTokens()) { sb.append(Path.SEPARATOR); }
    }
    return sb.length() == 0? ".": sb.toString();
  }

  /**
   * Calculate how many maps to run.
   * Number of maps is bounded by a minimum of the cumulative size of the
   * copy / (distcp.bytes.per.map, default BYTES_PER_MAP or -m on the
   * command line) and at most (distcp.max.map.tasks, default
   * MAX_MAPS_PER_NODE * nodes in the cluster).
   * @param totalBytes Count of total bytes for job
   * @param job The job to configure
   * @return Count of maps to run.
   */
  private static void setMapCount(long totalBytes, JobConf job) 
      throws IOException {
    int numMaps =
      (int)(totalBytes / job.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP));
    numMaps = Math.min(numMaps, 
        job.getInt(MAX_MAPS_LABEL, MAX_MAPS_PER_NODE *
          new JobClient(job).getClusterStatus().getTaskTrackers()));
    job.setNumMapTasks(Math.max(numMaps, 1));
  }

  /** Fully delete dir */
  static void fullyDelete(String dir, Configuration conf) throws IOException {
    if (dir != null) {
      Path tmp = new Path(dir);
      boolean success = tmp.getFileSystem(conf).delete(tmp, true);
      if (!success) {
        LOG.warn("Could not fully delete " + tmp);
      }
    }
  }

  //Job configuration
  private static JobConf createJobConf(Configuration conf) {
    JobConf jobconf = new JobConf(conf, DistCp.class);
    jobconf.setJobName(NAME);

    // turn off speculative execution, because DFS doesn't handle
    // multiple writers to the same file.
    jobconf.setMapSpeculativeExecution(false);

    jobconf.setInputFormat(CopyInputFormat.class);
    jobconf.setOutputKeyClass(Text.class);
    jobconf.setOutputValueClass(Text.class);

    jobconf.setMapperClass(CopyFilesMapper.class);
    jobconf.setNumReduceTasks(0);
    return jobconf;
  }

  private static final Random RANDOM = new Random();
  public static String getRandomId() {
    return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
  }

  /**
   * Initialize DFSCopyFileMapper specific job-configuration.
   * @param conf : The dfs/mapred configuration.
   * @param jobConf : The handle to the jobConf object to be initialized.
   * @param args Arguments
   * @return true if it is necessary to launch a job.
   */
  private static boolean setup(Configuration conf, JobConf jobConf,
                            final Arguments args)
      throws IOException {
    jobConf.set(DST_DIR_LABEL, args.dst.toUri().toString());

    //set boolean values
    final boolean update = args.flags.contains(Options.UPDATE);
    final boolean skipCRCCheck = args.flags.contains(Options.SKIPCRC);
    final boolean overwrite = !update && args.flags.contains(Options.OVERWRITE);
    jobConf.setBoolean(Options.UPDATE.propertyname, update);
    jobConf.setBoolean(Options.SKIPCRC.propertyname, skipCRCCheck);
    jobConf.setBoolean(Options.OVERWRITE.propertyname, overwrite);
    jobConf.setBoolean(Options.IGNORE_READ_FAILURES.propertyname,
        args.flags.contains(Options.IGNORE_READ_FAILURES));
    jobConf.setBoolean(Options.PRESERVE_STATUS.propertyname,
        args.flags.contains(Options.PRESERVE_STATUS));

    final String randomId = getRandomId();
    JobClient jClient = new JobClient(jobConf);
    Path stagingArea;
    try {
      stagingArea = JobSubmissionFiles.getStagingDir(jClient, conf);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    
    Path jobDirectory = new Path(stagingArea + NAME + "_" + randomId);
    FsPermission mapredSysPerms =
      new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
    FileSystem.mkdirs(jClient.getFs(), jobDirectory, mapredSysPerms);
    jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());

    long maxBytesPerMap = conf.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP);

    FileSystem dstfs = args.dst.getFileSystem(conf);
    
    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(), 
                                        new Path[] {args.dst}, conf);
    
    boolean dstExists = dstfs.exists(args.dst);
    boolean dstIsDir = false;
    if (dstExists) {
      dstIsDir = dstfs.getFileStatus(args.dst).isDir();
    }

    // default logPath
    Path logPath = args.log; 
    if (logPath == null) {
      String filename = "_distcp_logs_" + randomId;
      if (!dstExists || !dstIsDir) {
        Path parent = args.dst.getParent();
        if (null == parent) {
          // If dst is '/' on S3, it might not exist yet, but dst.getParent()
          // will return null. In this case, use '/' as its own parent to prevent
          // NPE errors below.
          parent = args.dst;
        }
        if (!dstfs.exists(parent)) {
          dstfs.mkdirs(parent);
        }
        logPath = new Path(parent, filename);
      } else {
        logPath = new Path(args.dst, filename);
      }
    }
    FileOutputFormat.setOutputPath(jobConf, logPath);

    // create src list, dst list
    FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

    Path srcfilelist = new Path(jobDirectory, "_distcp_src_files");
    jobConf.set(SRC_LIST_LABEL, srcfilelist.toString());
    SequenceFile.Writer src_writer = SequenceFile.createWriter(jobfs, jobConf,
        srcfilelist, LongWritable.class, FilePair.class,
        SequenceFile.CompressionType.NONE);

    Path dstfilelist = new Path(jobDirectory, "_distcp_dst_files");
    SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstfilelist, Text.class, Text.class,
        SequenceFile.CompressionType.NONE);

    Path dstdirlist = new Path(jobDirectory, "_distcp_dst_dirs");
    jobConf.set(DST_DIR_LIST_LABEL, dstdirlist.toString());
    SequenceFile.Writer dir_writer = SequenceFile.createWriter(jobfs, jobConf,
        dstdirlist, Text.class, FilePair.class,
        SequenceFile.CompressionType.NONE);

    // handle the case where the destination directory doesn't exist
    // and we've only a single src directory OR we're updating/overwriting
    // the contents of the destination directory.
    final boolean special =
      (args.srcs.size() == 1 && !dstExists) || update || overwrite;
    int srcCount = 0, cnsyncf = 0, dirsyn = 0;
    long fileCount = 0L, byteCount = 0L, cbsyncs = 0L;
    try {
      for(Iterator<Path> srcItr = args.srcs.iterator(); srcItr.hasNext(); ) {
        final Path src = srcItr.next();
        FileSystem srcfs = src.getFileSystem(conf);
        FileStatus srcfilestat = srcfs.getFileStatus(src);
        Path root = special && srcfilestat.isDir()? src: src.getParent();
        if (srcfilestat.isDir()) {
          ++srcCount;
        }

        Stack<FileStatus> pathstack = new Stack<FileStatus>();
        for(pathstack.push(srcfilestat); !pathstack.empty(); ) {
          FileStatus cur = pathstack.pop();
          FileStatus[] children = srcfs.listStatus(cur.getPath());
          for(int i = 0; i < children.length; i++) {
            boolean skipfile = false;
            final FileStatus child = children[i]; 
            final String dst = makeRelative(root, child.getPath());
            ++srcCount;

            if (child.isDir()) {
              pathstack.push(child);
            }
            else {
              //skip file if the src and the dst files are the same.
              skipfile = update && 
                sameFile(srcfs, child, dstfs, 
                  new Path(args.dst, dst), skipCRCCheck);
              //skip file if it exceed file limit or size limit
              skipfile |= fileCount == args.filelimit
                          || byteCount + child.getLen() > args.sizelimit; 

              if (!skipfile) {
                ++fileCount;
                byteCount += child.getLen();

                if (LOG.isTraceEnabled()) {
                  LOG.trace("adding file " + child.getPath());
                }

                ++cnsyncf;
                cbsyncs += child.getLen();
                if (cnsyncf > SYNC_FILE_MAX || cbsyncs > maxBytesPerMap) {
                  src_writer.sync();
                  dst_writer.sync();
                  cnsyncf = 0;
                  cbsyncs = 0L;
                }
              }
            }

            if (!skipfile) {
              src_writer.append(new LongWritable(child.isDir()? 0: child.getLen()),
                  new FilePair(child, dst));
            }

            dst_writer.append(new Text(dst),
                new Text(child.getPath().toString()));
          }

          if (cur.isDir()) {
            String dst = makeRelative(root, cur.getPath());
            dir_writer.append(new Text(dst), new FilePair(cur, dst));
            if (++dirsyn > SYNC_FILE_MAX) {
              dirsyn = 0;
              dir_writer.sync();                
            }
          }
        }
      }
    } finally {
      checkAndClose(src_writer);
      checkAndClose(dst_writer);
      checkAndClose(dir_writer);
    }

    FileStatus dststatus = null;
    try {
      dststatus = dstfs.getFileStatus(args.dst);
    } catch(FileNotFoundException fnfe) {
      LOG.info(args.dst + " does not exist.");
    }

    // create dest path dir if copying > 1 file
    if (dststatus == null) {
      if (srcCount > 1 && !dstfs.mkdirs(args.dst)) {
        throw new IOException("Failed to create" + args.dst);
      }
    }
    
    final Path sorted = new Path(jobDirectory, "_distcp_sorted"); 
    checkDuplication(jobfs, dstfilelist, sorted, conf);

    if (dststatus != null && args.flags.contains(Options.DELETE)) {
      deleteNonexisting(dstfs, dststatus, sorted,
          jobfs, jobDirectory, jobConf, conf);
    }

    Path tmpDir = new Path(
        (dstExists && !dstIsDir) || (!dstExists && srcCount == 1)?
        args.dst.getParent(): args.dst, "_distcp_tmp_" + randomId);
    jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());

    // Explicitly create the tmpDir to ensure that it can be cleaned
    // up by fullyDelete() later.
    tmpDir.getFileSystem(conf).mkdirs(tmpDir);

    LOG.info("sourcePathsCount=" + srcCount);
    LOG.info("filesToCopyCount=" + fileCount);
    LOG.info("bytesToCopyCount=" + StringUtils.humanReadableInt(byteCount));
    jobConf.setInt(SRC_COUNT_LABEL, srcCount);
    jobConf.setLong(TOTAL_SIZE_LABEL, byteCount);
    setMapCount(byteCount, jobConf);
    return fileCount > 0;
  }

  /**
   * Check whether the contents of src and dst are the same.
   * 
   * Return false if dstpath does not exist
   * 
   * If the files have different sizes, return false.
   * 
   * If the files have the same sizes, the file checksums will be compared.
   * 
   * When file checksum is not supported in any of file systems,
   * two files are considered as the same if they have the same size.
   */
  static private boolean sameFile(FileSystem srcfs, FileStatus srcstatus,
      FileSystem dstfs, Path dstpath, boolean skipCRCCheck) throws IOException {
    FileStatus dststatus;
    try {
      dststatus = dstfs.getFileStatus(dstpath);
    } catch(FileNotFoundException fnfe) {
      return false;
    }

    //same length?
    if (srcstatus.getLen() != dststatus.getLen()) {
      return false;
    }

    if (skipCRCCheck) {
      LOG.debug("Skipping CRC Check");
      return true;
    }

    //get src checksum
    final FileChecksum srccs;
    try {
      srccs = srcfs.getFileChecksum(srcstatus.getPath());
    } catch(FileNotFoundException fnfe) {
      /*
       * Two possible cases:
       * (1) src existed once but was deleted between the time period that
       *     srcstatus was obtained and the try block above.
       * (2) srcfs does not support file checksum and (incorrectly) throws
       *     FNFE, e.g. some previous versions of HftpFileSystem.
       * For case (1), it is okay to return true since src was already deleted.
       * For case (2), true should be returned.  
       */
      return true;
    }

    //compare checksums
    try {
      final FileChecksum dstcs = dstfs.getFileChecksum(dststatus.getPath());
      //return true if checksum is not supported
      //(i.e. some of the checksums is null)
      return srccs == null || dstcs == null || srccs.equals(dstcs);
    } catch(FileNotFoundException fnfe) {
      return false;
    }
  }
  
  /** Delete the dst files/dirs which do not exist in src */
  static private void deleteNonexisting(
      FileSystem dstfs, FileStatus dstroot, Path dstsorted,
      FileSystem jobfs, Path jobdir, JobConf jobconf, Configuration conf
      ) throws IOException {
    if (!dstroot.isDir()) {
      throw new IOException("dst must be a directory when option "
          + Options.DELETE.cmd + " is set, but dst (= " + dstroot.getPath()
          + ") is not a directory.");
    }

    //write dst lsr results
    final Path dstlsr = new Path(jobdir, "_distcp_dst_lsr");
    final SequenceFile.Writer writer = SequenceFile.createWriter(jobfs, jobconf,
        dstlsr, Text.class, dstroot.getClass(),
        SequenceFile.CompressionType.NONE);
    try {
      //do lsr to get all file statuses in dstroot
      final Stack<FileStatus> lsrstack = new Stack<FileStatus>();
      for(lsrstack.push(dstroot); !lsrstack.isEmpty(); ) {
        final FileStatus status = lsrstack.pop();
        if (status.isDir()) {
          for(FileStatus child : dstfs.listStatus(status.getPath())) {
            String relative = makeRelative(dstroot.getPath(), child.getPath());
            writer.append(new Text(relative), child);
            lsrstack.push(child);
          }
        }
      }
    } finally {
      checkAndClose(writer);
    }

    //sort lsr results
    final Path sortedlsr = new Path(jobdir, "_distcp_dst_lsr_sorted");
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(jobfs,
        new Text.Comparator(), Text.class, FileStatus.class, jobconf);
    sorter.sort(dstlsr, sortedlsr);

    //compare lsr list and dst list  
    SequenceFile.Reader lsrin = null;
    SequenceFile.Reader dstin = null;
    try {
      lsrin = new SequenceFile.Reader(jobfs, sortedlsr, jobconf);
      dstin = new SequenceFile.Reader(jobfs, dstsorted, jobconf);

      //compare sorted lsr list and sorted dst list
      final Text lsrpath = new Text();
      final FileStatus lsrstatus = new FileStatus();
      final Text dstpath = new Text();
      final Text dstfrom = new Text();
      final FsShell shell = new FsShell(conf);
      final String[] shellargs = {"-rmr", null};

      boolean hasnext = dstin.next(dstpath, dstfrom);
      for(; lsrin.next(lsrpath, lsrstatus); ) {
        int dst_cmp_lsr = dstpath.compareTo(lsrpath);
        for(; hasnext && dst_cmp_lsr < 0; ) {
          hasnext = dstin.next(dstpath, dstfrom);
          dst_cmp_lsr = dstpath.compareTo(lsrpath);
        }
        
        if (dst_cmp_lsr == 0) {
          //lsrpath exists in dst, skip it
          hasnext = dstin.next(dstpath, dstfrom);
        }
        else {
          //lsrpath does not exist, delete it
          String s = new Path(dstroot.getPath(), lsrpath.toString()).toString();
          if (shellargs[1] == null || !isAncestorPath(shellargs[1], s)) {
            shellargs[1] = s;
            int r = 0;
            try {
               r = shell.run(shellargs);
            } catch(Exception e) {
              throw new IOException("Exception from shell.", e);
            }
            if (r != 0) {
              throw new IOException("\"" + shellargs[0] + " " + shellargs[1]
                  + "\" returns non-zero value " + r);
            }
          }
        }
      }
    } finally {
      checkAndClose(lsrin);
      checkAndClose(dstin);
    }
  }

  //is x an ancestor path of y?
  static private boolean isAncestorPath(String x, String y) {
    if (!y.startsWith(x)) {
      return false;
    }
    final int len = x.length();
    return y.length() == len || y.charAt(len) == Path.SEPARATOR_CHAR;  
  }
  
  /** Check whether the file list have duplication. */
  static private void checkDuplication(FileSystem fs, Path file, Path sorted,
    Configuration conf) throws IOException {
    SequenceFile.Reader in = null;
    try {
      SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
        new Text.Comparator(), Text.class, Text.class, conf);
      sorter.sort(file, sorted);
      in = new SequenceFile.Reader(fs, sorted, conf);

      Text prevdst = null, curdst = new Text();
      Text prevsrc = null, cursrc = new Text(); 
      for(; in.next(curdst, cursrc); ) {
        if (prevdst != null && curdst.equals(prevdst)) {
          throw new DuplicationException(
            "Invalid input, there are duplicated files in the sources: "
            + prevsrc + ", " + cursrc);
        }
        prevdst = curdst;
        curdst = new Text();
        prevsrc = cursrc;
        cursrc = new Text();
      }
    }
    finally {
      checkAndClose(in);
    }
  } 

  static boolean checkAndClose(java.io.Closeable io) {
    if (io != null) {
      try {
        io.close();
      }
      catch(IOException ioe) {
        LOG.warn(StringUtils.stringifyException(ioe));
        return false;
      }
    }
    return true;
  }

  /** An exception class for duplicated source files. */
  public static class DuplicationException extends IOException {
    private static final long serialVersionUID = 1L;
    /** Error code for this exception */
    public static final int ERROR_CODE = -2;
    DuplicationException(String message) {super(message);}
  }
}
