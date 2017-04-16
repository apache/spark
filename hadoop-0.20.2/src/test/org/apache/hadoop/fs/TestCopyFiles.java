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

package org.apache.hadoop.fs;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;


/**
 * A JUnit test for copying files recursively.
 */
public class TestCopyFiles extends TestCase {
  {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.StateChange")
        ).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DistCp.LOG).getLogger().setLevel(Level.ALL);
  }
  
  static final URI LOCAL_FS = URI.create("file:///");
  
  private static final Random RAN = new Random();
  private static final int NFILES = 20;
  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private static class MyFile {
    private static Random gen = new Random();
    private static final int MAX_LEVELS = 3;
    private static final int MAX_SIZE = 8*1024;
    private static String[] dirNames = {
      "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
    };
    private final String name;
    private int size = 0;
    private long seed = 0L;

    MyFile() {
      this(gen.nextInt(MAX_LEVELS));
    }
    MyFile(int nLevels) {
      String xname = "";
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        xname = sb.toString();
      }
      long fidx = gen.nextLong() & Long.MAX_VALUE;
      name = xname + Long.toString(fidx);
      reset();
    }
    void reset() {
      final int oldsize = size;
      do { size = gen.nextInt(MAX_SIZE); } while (oldsize == size);
      final long oldseed = seed;
      do { seed = gen.nextLong() & Long.MAX_VALUE; } while (oldseed == seed);
    }
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  private static MyFile[] createFiles(URI fsname, String topdir)
    throws IOException {
    return createFiles(FileSystem.get(fsname, new Configuration()), topdir);
  }

  /** create NFILES with random names and directory hierarchies
   * with random (but reproducible) data in them.
   */
  private static MyFile[] createFiles(FileSystem fs, String topdir)
    throws IOException {
    Path root = new Path(topdir);
    MyFile[] files = new MyFile[NFILES];
    for (int i = 0; i < NFILES; i++) {
      files[i] = createFile(root, fs);
    }
    return files;
  }

  static MyFile createFile(Path root, FileSystem fs, int levels)
      throws IOException {
    MyFile f = levels < 0 ? new MyFile() : new MyFile(levels);
    Path p = new Path(root, f.getName());
    FSDataOutputStream out = fs.create(p);
    byte[] toWrite = new byte[f.getSize()];
    new Random(f.getSeed()).nextBytes(toWrite);
    out.write(toWrite);
    out.close();
    FileSystem.LOG.info("created: " + p + ", size=" + f.getSize());
    return f;
  }

  static MyFile createFile(Path root, FileSystem fs) throws IOException {
    return createFile(root, fs, -1);
  }

  private static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files
      ) throws IOException {
    return checkFiles(fs, topdir, files, false);    
  }

  private static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files,
      boolean existingOnly) throws IOException {
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < files.length; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      try {
        fs.getFileStatus(fPath);
        FSDataInputStream in = fs.open(fPath);
        byte[] toRead = new byte[files[idx].getSize()];
        byte[] toCompare = new byte[files[idx].getSize()];
        Random rb = new Random(files[idx].getSeed());
        rb.nextBytes(toCompare);
        assertEquals("Cannnot read file.", toRead.length, in.read(toRead));
        in.close();
        for (int i = 0; i < toRead.length; i++) {
          if (toRead[i] != toCompare[i]) {
            return false;
          }
        }
        toRead = null;
        toCompare = null;
      }
      catch(FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
    }
    
    return true;
  }

  private static void updateFiles(FileSystem fs, String topdir, MyFile[] files,
        int nupdate) throws IOException {
    assert nupdate <= NFILES;

    Path root = new Path(topdir);

    for (int idx = 0; idx < nupdate; ++idx) {
      Path fPath = new Path(root, files[idx].getName());
      // overwrite file
      assertTrue(fPath.toString() + " does not exist", fs.exists(fPath));
      FSDataOutputStream out = fs.create(fPath);
      files[idx].reset();
      byte[] toWrite = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toWrite);
      out.write(toWrite);
      out.close();
    }
  }

  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, MyFile[] files) throws IOException {
    return getFileStatus(fs, topdir, files, false);
  }
  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, MyFile[] files, boolean existingOnly) throws IOException {
    Path root = new Path(topdir);
    List<FileStatus> statuses = new ArrayList<FileStatus>();
    for (int idx = 0; idx < NFILES; ++idx) {
      try {
        statuses.add(fs.getFileStatus(new Path(root, files[idx].getName())));
      } catch(FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
    }
    return statuses.toArray(new FileStatus[statuses.size()]);
  }

  private static boolean checkUpdate(FileSystem fs, FileStatus[] old,
      String topdir, MyFile[] upd, final int nupdate) throws IOException {
    Path root = new Path(topdir);

    // overwrote updated files
    for (int idx = 0; idx < nupdate; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() <= old[idx].getModificationTime()) {
        return false;
      }
    }
    // did not overwrite files not updated
    for (int idx = nupdate; idx < NFILES; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() != old[idx].getModificationTime()) {
        return false;
      }
    }
    return true;
  }

  /** delete directory and everything underneath it.*/
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }
  
  /** copy files from local file system to local file system */
  public void testCopyFromLocalToLocal() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
    MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
    ToolRunner.run(new DistCp(new Configuration()),
                           new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
    assertTrue("Source and destination directories do not match.",
               checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));
    deldir(localfs, TEST_ROOT_DIR+"/destdat");
    deldir(localfs, TEST_ROOT_DIR+"/srcdat");
  }
  
  /** copy files from dfs file system to dfs file system */
  public void testCopyFromDfsToDfs() throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                   fs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  /** copy files from local file system to dfs file system */
  public void testCopyFromLocalToDfs() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(LOCAL_FS, TEST_ROOT_DIR+"/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         "file:///"+TEST_ROOT_DIR+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(cluster.getFileSystem(), "/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/logs");
        deldir(FileSystem.get(LOCAL_FS, conf), TEST_ROOT_DIR+"/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** copy files from dfs file system to local file system */
  public void testCopyFromDfsToLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      final FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         "/logs",
                                         namenode+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path("/logs")));
        deldir(localfs, TEST_ROOT_DIR+"/destdat");
        deldir(hdfs, "/logs");
        deldir(hdfs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /**
   * verify that -delete option works for other {@link FileSystem}
   * implementations. See MAPREDUCE-1285 */
  public void testDeleteLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      final FileSystem localfs = FileSystem.get(LOCAL_FS, conf);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        String destdir = TEST_ROOT_DIR + "/destdat";
        MyFile[] localFiles = createFiles(localfs, destdir);
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-delete",
                                         "-update",
                                         "-log",
                                         "/logs",
                                         namenode+"/srcdat",
                                         "file:///"+TEST_ROOT_DIR+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(localfs, destdir, files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path("/logs")));
        deldir(localfs, destdir);
        deldir(hdfs, "/logs");
        deldir(hdfs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCopyDfsToDfsUpdateOverwrite() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode+"/logs"), conf);
        assertTrue("Log directory does not exist.",
                    fs.exists(new Path(namenode+"/logs")));

        FileStatus[] dchkpoint = getFileStatus(hdfs, "/destdat", files);
        final int nupdate = NFILES>>2;
        updateFiles(cluster.getFileSystem(), "/srcdat", files, nupdate);
        deldir(hdfs, "/logs");

        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-update",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        assertTrue("Update failed to replicate all changes in src",
                 checkUpdate(hdfs, dchkpoint, "/destdat", files, nupdate));

        deldir(hdfs, "/logs");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-p",
                                         "-overwrite",
                                         "-log",
                                         namenode+"/logs",
                                         namenode+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(hdfs, "/destdat", files));
        assertTrue("-overwrite didn't.",
                 checkUpdate(hdfs, dchkpoint, "/destdat", files, NFILES));

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

 public void testCopyDfsToDfsUpdateWithSkipCRC() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      
      FileSystem fs = FileSystem.get(URI.create(namenode), new Configuration());
      // Create two files of the same name, same length but different
      // contents
      final String testfilename = "test";
      final String srcData = "act act act";
      final String destData = "cat cat cat";
      
      if (namenode.startsWith("hdfs://")) {
        deldir(hdfs,"/logs");
        
        Path srcPath = new Path("/srcdat", testfilename);
        Path destPath = new Path("/destdat", testfilename);
        FSDataOutputStream out = fs.create(srcPath, true);
        out.writeUTF(srcData);
        out.close();

        out = fs.create(destPath, true);
        out.writeUTF(destData);
        out.close();
        
        // Run with -skipcrccheck option
        ToolRunner.run(new DistCp(conf), new String[] {
          "-p",
          "-update",
          "-skipcrccheck",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});
        
        // File should not be overwritten
        FSDataInputStream in = hdfs.open(destPath);
        String s = in.readUTF();
        System.out.println("Dest had: " + s);
        assertTrue("Dest got over written even with skip crc",
            s.equalsIgnoreCase(destData));
        in.close();
        
        deldir(hdfs, "/logs");

        // Run without the option        
        ToolRunner.run(new DistCp(conf), new String[] {
          "-p",
          "-update",
          "-log",
          namenode+"/logs",
          namenode+"/srcdat",
          namenode+"/destdat"});
        
        // File should be overwritten
        in = hdfs.open(destPath);
        s = in.readUTF();
        System.out.println("Dest had: " + s);

        assertTrue("Dest did not get overwritten without skip crc",
            s.equalsIgnoreCase(srcData));
        in.close();

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
       }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testCopyDuplication() throws Exception {
    final FileSystem localfs = FileSystem.get(LOCAL_FS, new Configuration());
    try {    
      MyFile[] files = createFiles(localfs, TEST_ROOT_DIR+"/srcdat");
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/src2/srcdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(localfs, TEST_ROOT_DIR+"/src2/srcdat", files));
  
      assertEquals(DistCp.DuplicationException.ERROR_CODE,
          ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/src2/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/destdat",}));
    }
    finally {
      deldir(localfs, TEST_ROOT_DIR+"/destdat");
      deldir(localfs, TEST_ROOT_DIR+"/srcdat");
      deldir(localfs, TEST_ROOT_DIR+"/src2");
    }
  }

  public void testCopySingleFile() throws Exception {
    FileSystem fs = FileSystem.get(LOCAL_FS, new Configuration());
    Path root = new Path(TEST_ROOT_DIR+"/srcdat");
    try {    
      MyFile[] files = {createFile(root, fs)};
      //copy a dir with a single file
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat",
                        "file:///"+TEST_ROOT_DIR+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(fs, TEST_ROOT_DIR+"/destdat", files));
      
      //copy a single file
      String fname = files[0].getName();
      Path p = new Path(root, fname);
      FileSystem.LOG.info("fname=" + fname + ", exists? " + fs.exists(p));
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"file:///"+TEST_ROOT_DIR+"/srcdat/"+fname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"+fname});
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR+"/dest2", files));     
      //copy single file to existing dir
      deldir(fs, TEST_ROOT_DIR+"/dest2");
      fs.mkdirs(new Path(TEST_ROOT_DIR+"/dest2"));
      MyFile[] files2 = {createFile(root, fs, 0)};
      String sname = files2[0].getName();
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"-update",
                        "file:///"+TEST_ROOT_DIR+"/srcdat/"+sname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"});
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR+"/dest2", files2));     
      updateFiles(fs, TEST_ROOT_DIR+"/srcdat", files2, 1);
      //copy single file to existing dir w/ dst name conflict
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {"-update",
                        "file:///"+TEST_ROOT_DIR+"/srcdat/"+sname,
                        "file:///"+TEST_ROOT_DIR+"/dest2/"});
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR+"/dest2", files2));     
    }
    finally {
      deldir(fs, TEST_ROOT_DIR+"/destdat");
      deldir(fs, TEST_ROOT_DIR+"/dest2");
      deldir(fs, TEST_ROOT_DIR+"/srcdat");
    }
  }

  public void testPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      {//test preserving user
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), "u" + i, null);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pu", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving group
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), null, "g" + i);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pg", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "g" + i, dststat[i].getGroup());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving mode
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          permissions[i] = new FsPermission((short)(i & 0666));
          fs.setPermission(srcstat[i].getPath(), permissions[i]);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pp", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
  
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, permissions[i], dststat[i].getPermission());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testMapCount() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      final FsShell shell = new FsShell(conf);
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = createFiles(fs.getUri(), "/srcdat");
      long totsize = 0;
      for (MyFile f : files) {
        totsize += f.getSize();
      }
      Configuration job = mr.createJobConf();
      job.setLong("distcp.bytes.per.map", totsize / 3);
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "100",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(fs, "/destdat", files));

      String logdir = namenode + "/logs";
      System.out.println(execCmd(shell, "-lsr", logdir));
      FileStatus[] logs = fs.listStatus(new Path(logdir));
      // rare case where splits are exact, logs.length can be 4
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 5 || logs.length == 4);

      deldir(fs, "/destdat");
      deldir(fs, "/logs");
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "1",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});

      System.out.println(execCmd(shell, "-lsr", logdir));
      logs = fs.listStatus(new Path(namenode+"/logs"));
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 2);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  public void testLimits() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final String nnUri = FileSystem.getDefaultUri(conf).toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir =  "/src_root";
      final Path srcrootpath = new Path(srcrootdir); 
      final String dstrootdir =  "/dst_root";
      final Path dstrootpath = new Path(dstrootdir); 

      {//test -filelimit
        MyFile[] files = createFiles(URI.create(nnUri), srcrootdir);
        int filelimit = files.length / 2;
        System.out.println("filelimit=" + filelimit);

        ToolRunner.run(distcp,
            new String[]{"-filelimit", ""+filelimit, nnUri+srcrootdir, nnUri+dstrootdir});
        String results = execCmd(shell, "-lsr", dstrootdir);
        results = removePrefix(results, dstrootdir);
        System.out.println("results=" +  results);

        FileStatus[] dststat = getFileStatus(fs, dstrootdir, files, true);
        assertEquals(filelimit, dststat.length);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test -sizelimit
        createFiles(URI.create(nnUri), srcrootdir);
        long sizelimit = fs.getContentSummary(srcrootpath).getLength()/2;
        System.out.println("sizelimit=" + sizelimit);

        ToolRunner.run(distcp,
            new String[]{"-sizelimit", ""+sizelimit, nnUri+srcrootdir, nnUri+dstrootdir});
        
        ContentSummary summary = fs.getContentSummary(dstrootpath);
        System.out.println("summary=" + summary);
        assertTrue(summary.getLength() <= sizelimit);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test update
        final MyFile[] srcs = createFiles(URI.create(nnUri), srcrootdir);
        final long totalsize = fs.getContentSummary(srcrootpath).getLength();
        System.out.println("src.length=" + srcs.length);
        System.out.println("totalsize =" + totalsize);
        fs.mkdirs(dstrootpath);
        final int parts = RAN.nextInt(NFILES/3 - 1) + 2;
        final int filelimit = srcs.length/parts;
        final long sizelimit = totalsize/parts;
        System.out.println("filelimit=" + filelimit);
        System.out.println("sizelimit=" + sizelimit);
        System.out.println("parts    =" + parts);
        final String[] args = {"-filelimit", ""+filelimit, "-sizelimit", ""+sizelimit,
            "-update", nnUri+srcrootdir, nnUri+dstrootdir};

        int dstfilecount = 0;
        long dstsize = 0;
        for(int i = 0; i <= parts; i++) {
          ToolRunner.run(distcp, args);
        
          FileStatus[] dststat = getFileStatus(fs, dstrootdir, srcs, true);
          System.out.println(i + ") dststat.length=" + dststat.length);
          assertTrue(dststat.length - dstfilecount <= filelimit);
          ContentSummary summary = fs.getContentSummary(dstrootpath);
          System.out.println(i + ") summary.getLength()=" + summary.getLength());
          assertTrue(summary.getLength() - dstsize <= sizelimit);
          assertTrue(checkFiles(fs, dstrootdir, srcs, true));
          dstfilecount = dststat.length;
          dstsize = summary.getLength();
        }

        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  static final long now = System.currentTimeMillis();

  static UserGroupInformation createUGI(String name, boolean issuper) {
    String username = name + now;
    String group = issuper? "supergroup": username;
    return UserGroupInformation.createUserForTesting(username, 
        new String[]{group});
  }

  static Path createHomeDirectory(FileSystem fs, UserGroupInformation ugi
      ) throws IOException {
    final Path home = new Path("/user/" + ugi.getUserName());
    fs.mkdirs(home);
    fs.setOwner(home, ugi.getUserName(), ugi.getGroupNames()[0]);
    fs.setPermission(home, new FsPermission((short)0700));
    return home;
  }

  public void testHftpAccessControl() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      final UserGroupInformation DFS_UGI = createUGI("dfs", true); 
      final UserGroupInformation USER_UGI = createUGI("user", false); 

      //start cluster by DFS_UGI
      final Configuration dfsConf = new Configuration();
      cluster = new MiniDFSCluster(dfsConf, 2, true, null);
      cluster.waitActive();

      final String httpAdd = dfsConf.get("dfs.http.address");
      final URI nnURI = FileSystem.getDefaultUri(dfsConf);
      final String nnUri = nnURI.toString();
      FileSystem fs1 = DFS_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return FileSystem.get(nnURI, dfsConf);
        }
      });
      final Path home = 
        createHomeDirectory(fs1, USER_UGI);
      
      //now, login as USER_UGI
      final Configuration userConf = new Configuration();
      final FileSystem fs = 
        USER_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return FileSystem.get(nnURI, userConf);
        }
      });
      
      final Path srcrootpath = new Path(home, "src_root"); 
      final String srcrootdir =  srcrootpath.toString();
      final Path dstrootpath = new Path(home, "dst_root"); 
      final String dstrootdir =  dstrootpath.toString();
      final DistCp distcp = USER_UGI.doAs(new PrivilegedExceptionAction<DistCp>() {
        public DistCp run() {
          return new DistCp(userConf);
        }
      });

      FileSystem.mkdirs(fs, srcrootpath, new FsPermission((short)0700));
      final String[] args = {"hftp://"+httpAdd+srcrootdir, nnUri+dstrootdir};

      { //copy with permission 000, should fail
        fs.setPermission(srcrootpath, new FsPermission((short)0));
        USER_UGI.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws Exception {
            assertEquals(-3, ToolRunner.run(distcp, args));
            return null;
          }
        });
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** test -delete */
  public void testDelete() throws Exception {
    final Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final URI nnURI = FileSystem.getDefaultUri(conf);
      final String nnUri = nnURI.toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir = "/src_root";
      final String dstrootdir = "/dst_root";

      {
        //create source files
        createFiles(nnURI, srcrootdir);
        String srcresults = execCmd(shell, "-lsr", srcrootdir);
        srcresults = removePrefix(srcresults, srcrootdir);
        System.out.println("srcresults=" +  srcresults);

        //create some files in dst
        createFiles(nnURI, dstrootdir);
        System.out.println("dstrootdir=" +  dstrootdir);
        shell.run(new String[]{"-lsr", dstrootdir});

        //run distcp
        ToolRunner.run(distcp,
            new String[]{"-delete", "-update", "-log", "/log",
                         nnUri+srcrootdir, nnUri+dstrootdir});

        //make sure src and dst contains the same files
        String dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("first dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //create additional file in dst
        create(fs, new Path(dstrootdir, "foo"));
        create(fs, new Path(dstrootdir, "foobar"));

        //run distcp again
        ToolRunner.run(distcp,
            new String[]{"-delete", "-update", "-log", "/log2",
                         nnUri+srcrootdir, nnUri+dstrootdir});
        
        //make sure src and dst contains the same files
        dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("second dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //cleanup
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  static void create(FileSystem fs, Path f) throws IOException {
    FSDataOutputStream out = fs.create(f);
    try {
      byte[] b = new byte[1024 + RAN.nextInt(1024)];
      RAN.nextBytes(b);
      out.write(b);
    } finally {
      if (out != null) out.close();
    }
  }
  
  static String execCmd(FsShell shell, String... args) throws Exception {
    ByteArrayOutputStream baout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baout, true);
    PrintStream old = System.out;
    System.setOut(out);
    shell.run(args);
    out.close();
    System.setOut(old);
    return baout.toString();
  }
  
  private static String removePrefix(String lines, String prefix) {
    final int prefixlen = prefix.length();
    final StringTokenizer t = new StringTokenizer(lines, "\n");
    final StringBuffer results = new StringBuffer(); 
    for(; t.hasMoreTokens(); ) {
      String s = t.nextToken();
      results.append(s.substring(s.indexOf(prefix) + prefixlen) + "\n");
    }
    return results.toString();
  }
}
