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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.log4j.Level;

public class TestDistCh extends junit.framework.TestCase {
  {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.StateChange")
        ).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)TaskTracker.LOG).getLogger().setLevel(Level.OFF);
  }

  static final Long RANDOM_NUMBER_GENERATOR_SEED = null;

  private static final Random RANDOM = new Random();
  static {
    final long seed = RANDOM_NUMBER_GENERATOR_SEED == null?
        RANDOM.nextLong(): RANDOM_NUMBER_GENERATOR_SEED;
    System.out.println("seed=" + seed);
    RANDOM.setSeed(seed);
  }

  static final String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp")
        ).toString().replace(' ', '+');

  static final int NUN_SUBS = 5;

  static class FileTree {
    private final FileSystem fs;
    private final String root;
    private final Path rootdir;
    private int fcount = 0;

    Path createSmallFile(Path dir) throws IOException {
      final Path f = new Path(dir, "f" + ++fcount);
      assertTrue(!fs.exists(f));
      final DataOutputStream out = fs.create(f);
      try {
        out.writeBytes("createSmallFile: f=" + f);
      } finally {
        out.close();
      }
      assertTrue(fs.exists(f));
      return f;
    }

    Path mkdir(Path dir) throws IOException {
      assertTrue(fs.mkdirs(dir));
      assertTrue(fs.getFileStatus(dir).isDir());
      return dir;
    }
    
    FileTree(FileSystem fs, String name) throws IOException {
      this.fs = fs;
      this.root = "/test/" + name;
      this.rootdir = mkdir(new Path(root));
  
      for(int i = 0; i < 3; i++) {
        createSmallFile(rootdir);
      }
      
      for(int i = 0; i < NUN_SUBS; i++) {
        final Path sub = mkdir(new Path(root, "sub" + i));
        int num_files = RANDOM.nextInt(3);
        for(int j = 0; j < num_files; j++) {
          createSmallFile(sub);
        }
      }
      
      System.out.println("rootdir = " + rootdir);
    }
  }

  static class ChPermissionStatus extends PermissionStatus {
    ChPermissionStatus(FileStatus filestatus) {
      this(filestatus, "", "", "");
    }

    ChPermissionStatus(FileStatus filestatus, String owner, String group, String permission) {
      super("".equals(owner)? filestatus.getOwner(): owner, 
          "".equals(group)? filestatus.getGroup(): group,
          "".equals(permission)? filestatus.getPermission(): new FsPermission(Short.parseShort(permission, 8)));
    }
  }
  
  public void testDistCh() throws Exception {
    final Configuration conf = new Configuration();
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem fs = cluster.getFileSystem();
    final MiniMRCluster mr = new MiniMRCluster(2, fs.getUri().toString(), 1);
    final FsShell shell = new FsShell(conf);
    
    try {
      final FileTree tree = new FileTree(fs, "testDistCh");
      final FileStatus rootstatus = fs.getFileStatus(tree.rootdir);

      runLsr(shell, tree.root, 0);

      //generate random arguments
      final String[] args = new String[RANDOM.nextInt(NUN_SUBS-1) + 1];
      final PermissionStatus[] newstatus = new PermissionStatus[NUN_SUBS];
      final List<Integer> indices = new LinkedList<Integer>();
      for(int i = 0; i < NUN_SUBS; i++) {
        indices.add(i);
      }
      for(int i = 0; i < args.length; i++) {
        final int index = indices.remove(RANDOM.nextInt(indices.size()));
        final String sub = "sub" + index;
        final boolean changeOwner = RANDOM.nextBoolean();
        final boolean changeGroup = RANDOM.nextBoolean();
        final boolean changeMode = !changeOwner && !changeGroup? true: RANDOM.nextBoolean();
        
        final String owner = changeOwner? sub: "";
        final String group = changeGroup? sub: "";
        final String permission = changeMode? RANDOM.nextInt(8) + "" + RANDOM.nextInt(8) + "" + RANDOM.nextInt(8): "";

        args[i] = tree.root + "/" + sub + ":" + owner + ":" + group + ":" + permission;
        newstatus[index] = new ChPermissionStatus(rootstatus, owner, group, permission);
      }
      for(int i = 0; i < NUN_SUBS; i++) {
        if (newstatus[i] == null) {
          newstatus[i] = new ChPermissionStatus(rootstatus);
        }
      }
      System.out.println("args=" + Arrays.asList(args).toString().replace(",", ",\n  "));
      System.out.println("newstatus=" + Arrays.asList(newstatus).toString().replace(",", ",\n  "));

      //run DistCh
      new DistCh(mr.createJobConf()).run(args);
      runLsr(shell, tree.root, 0);

      //check results
      for(int i = 0; i < NUN_SUBS; i++) {
        Path sub = new Path(tree.root + "/sub" + i);
        checkFileStatus(newstatus[i], fs.getFileStatus(sub));
        for(FileStatus status : fs.listStatus(sub)) {
          checkFileStatus(newstatus[i], status);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

  static void checkFileStatus(PermissionStatus expected, FileStatus actual) {
    assertEquals(expected.getUserName(), actual.getOwner());
    assertEquals(expected.getGroupName(), actual.getGroup());
    FsPermission perm = expected.getPermission(); 
    if (!actual.isDir()) {
      perm = perm.applyUMask(UMASK);
    }
    assertEquals(perm, actual.getPermission());
  }

  private static String runLsr(final FsShell shell, String root, int returnvalue
      ) throws Exception {
    System.out.println("root=" + root + ", returnvalue=" + returnvalue);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream(); 
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      assertEquals(returnvalue, shell.run(new String[]{"-lsr", root}));
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("results:\n" + results);
    return results;
  }
}