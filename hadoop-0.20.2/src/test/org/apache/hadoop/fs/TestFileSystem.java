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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.net.InetSocketAddress;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import static org.mockito.Mockito.mock;

public class TestFileSystem extends TestCase {
  private static final Log LOG = FileSystem.LOG;

  private static Configuration conf = new Configuration();
  private static int BUFFER_SIZE = conf.getInt("io.file.buffer.size", 4096);

  private static final long MEGA = 1024 * 1024;
  private static final int SEEKS_PER_FILE = 4;

  private static String ROOT = System.getProperty("test.build.data","fs_test");
  private static Path CONTROL_DIR = new Path(ROOT, "fs_control");
  private static Path WRITE_DIR = new Path(ROOT, "fs_write");
  private static Path READ_DIR = new Path(ROOT, "fs_read");
  private static Path DATA_DIR = new Path(ROOT, "fs_data");

  public void testFs() throws Exception {
    testFs(10 * MEGA, 100, 0);
  }

  public static void testFs(long megaBytes, int numFiles, long seed)
    throws Exception {

    FileSystem fs = FileSystem.get(conf);

    if (seed == 0)
      seed = new Random().nextLong();

    LOG.info("seed = "+seed);

    createControlFile(fs, megaBytes, numFiles, seed);
    writeTest(fs, false);
    readTest(fs, false);
    seekTest(fs, false);
    fs.delete(CONTROL_DIR, true);
    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
    fs.delete(READ_DIR, true);
  }

  public static void testCommandFormat() throws Exception {
    // This should go to TestFsShell.java when it is added.
    CommandFormat cf;
    cf= new CommandFormat("copyToLocal", 2,2,"crc","ignoreCrc");
    assertEquals(cf.parse(new String[] {"-get","file", "-"}, 1).get(1), "-");
    assertEquals(cf.parse(new String[] {"-get","file","-ignoreCrc","/foo"}, 1).get(1),"/foo");
    cf = new CommandFormat("tail", 1, 1, "f");
    assertEquals(cf.parse(new String[] {"-tail","fileName"}, 1).get(0),"fileName");
    assertEquals(cf.parse(new String[] {"-tail","-f","fileName"}, 1).get(0),"fileName");
    cf = new CommandFormat("setrep", 2, 2, "R", "w");
    assertEquals(cf.parse(new String[] {"-setrep","-R","2","/foo/bar"}, 1).get(1), "/foo/bar");
    cf = new CommandFormat("put", 2, 10000);
    assertEquals(cf.parse(new String[] {"-put", "-", "dest"}, 1).get(1), "dest"); 
  }

  public static void createControlFile(FileSystem fs,
                                       long megaBytes, int numFiles,
                                       long seed) throws Exception {

    LOG.info("creating control file: "+megaBytes+" bytes, "+numFiles+" files");

    Path controlFile = new Path(CONTROL_DIR, "files");
    fs.delete(controlFile, true);
    Random random = new Random(seed);

    SequenceFile.Writer writer =
      SequenceFile.createWriter(fs, conf, controlFile, 
                                UTF8.class, LongWritable.class, CompressionType.NONE);

    long totalSize = 0;
    long maxSize = ((megaBytes / numFiles) * 2) + 1;
    try {
      while (totalSize < megaBytes) {
        UTF8 name = new UTF8(Long.toString(random.nextLong()));

        long size = random.nextLong();
        if (size < 0)
          size = -size;
        size = size % maxSize;

        //LOG.info(" adding: name="+name+" size="+size);

        writer.append(name, new LongWritable(size));

        totalSize += size;
      }
    } finally {
      writer.close();
    }
    LOG.info("created control file for: "+totalSize+" bytes");
  }

  public static class WriteMapper extends Configured
      implements Mapper<UTF8, LongWritable, UTF8, LongWritable> {
    
    private Random random = new Random();
    private byte[] buffer = new byte[BUFFER_SIZE];
    private FileSystem fs;
    private boolean fastCheck;

    // a random suffix per task
    private String suffix = "-"+random.nextLong();
    
    {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public WriteMapper() { super(null); }
    
    public WriteMapper(Configuration conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
      fastCheck = job.getBoolean("fs.test.fastCheck", false);
    }

    public void map(UTF8 key, LongWritable value,
                    OutputCollector<UTF8, LongWritable> collector,
                    Reporter reporter)
      throws IOException {
      
      String name = key.toString();
      long size = value.get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      reporter.setStatus("creating " + name);

      // write to temp file initially to permit parallel execution
      Path tempFile = new Path(DATA_DIR, name+suffix);
      OutputStream out = fs.create(tempFile);

      long written = 0;
      try {
        while (written < size) {
          if (fastCheck) {
            Arrays.fill(buffer, (byte)random.nextInt(Byte.MAX_VALUE));
          } else {
            random.nextBytes(buffer);
          }
          long remains = size - written;
          int length = (remains<=buffer.length) ? (int)remains : buffer.length;
          out.write(buffer, 0, length);
          written += length;
          reporter.setStatus("writing "+name+"@"+written+"/"+size);
        }
      } finally {
        out.close();
      }
      // rename to final location
      fs.rename(tempFile, new Path(DATA_DIR, name));

      collector.collect(new UTF8("bytes"), new LongWritable(written));

      reporter.setStatus("wrote " + name);
    }
    
    public void close() {
    }
    
  }

  public static void writeTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
    
    JobConf job = new JobConf(conf, TestFileSystem.class);
    job.setBoolean("fs.test.fastCheck", fastCheck);

    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(WriteMapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, WRITE_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  public static class ReadMapper extends Configured
      implements Mapper<UTF8, LongWritable, UTF8, LongWritable> {
    
    private Random random = new Random();
    private byte[] buffer = new byte[BUFFER_SIZE];
    private byte[] check  = new byte[BUFFER_SIZE];
    private FileSystem fs;
    private boolean fastCheck;

    {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public ReadMapper() { super(null); }
    
    public ReadMapper(Configuration conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
      fastCheck = job.getBoolean("fs.test.fastCheck", false);
    }

    public void map(UTF8 key, LongWritable value,
                    OutputCollector<UTF8, LongWritable> collector,
                    Reporter reporter)
      throws IOException {
      
      String name = key.toString();
      long size = value.get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      reporter.setStatus("opening " + name);

      DataInputStream in =
        new DataInputStream(fs.open(new Path(DATA_DIR, name)));

      long read = 0;
      try {
        while (read < size) {
          long remains = size - read;
          int n = (remains<=buffer.length) ? (int)remains : buffer.length;
          in.readFully(buffer, 0, n);
          read += n;
          if (fastCheck) {
            Arrays.fill(check, (byte)random.nextInt(Byte.MAX_VALUE));
          } else {
            random.nextBytes(check);
          }
          if (n != buffer.length) {
            Arrays.fill(buffer, n, buffer.length, (byte)0);
            Arrays.fill(check, n, check.length, (byte)0);
          }
          assertTrue(Arrays.equals(buffer, check));

          reporter.setStatus("reading "+name+"@"+read+"/"+size);

        }
      } finally {
        in.close();
      }

      collector.collect(new UTF8("bytes"), new LongWritable(read));

      reporter.setStatus("read " + name);
    }
    
    public void close() {
    }
    
  }

  public static void readTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(READ_DIR, true);

    JobConf job = new JobConf(conf, TestFileSystem.class);
    job.setBoolean("fs.test.fastCheck", fastCheck);


    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(ReadMapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, READ_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }


  public static class SeekMapper<K> extends Configured
    implements Mapper<WritableComparable, LongWritable, K, LongWritable> {
    
    private Random random = new Random();
    private byte[] check  = new byte[BUFFER_SIZE];
    private FileSystem fs;
    private boolean fastCheck;

    {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public SeekMapper() { super(null); }
    
    public SeekMapper(Configuration conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
      fastCheck = job.getBoolean("fs.test.fastCheck", false);
    }

    public void map(WritableComparable key, LongWritable value,
                    OutputCollector<K, LongWritable> collector,
                    Reporter reporter)
      throws IOException {
      String name = key.toString();
      long size = value.get();
      long seed = Long.parseLong(name);

      if (size == 0) return;

      reporter.setStatus("opening " + name);

      FSDataInputStream in = fs.open(new Path(DATA_DIR, name));
        
      try {
        for (int i = 0; i < SEEKS_PER_FILE; i++) {
          // generate a random position
          long position = Math.abs(random.nextLong()) % size;
          
          // seek file to that position
          reporter.setStatus("seeking " + name);
          in.seek(position);
          byte b = in.readByte();
          
          // check that byte matches
          byte checkByte = 0;
          // advance random state to that position
          random.setSeed(seed);
          for (int p = 0; p <= position; p+= check.length) {
            reporter.setStatus("generating data for " + name);
            if (fastCheck) {
              checkByte = (byte)random.nextInt(Byte.MAX_VALUE);
            } else {
              random.nextBytes(check);
              checkByte = check[(int)(position % check.length)];
            }
          }
          assertEquals(b, checkByte);
        }
      } finally {
        in.close();
      }
    }
    
    public void close() {
    }
    
  }

  public static void seekTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(READ_DIR, true);

    JobConf job = new JobConf(conf, TestFileSystem.class);
    job.setBoolean("fs.test.fastCheck", fastCheck);

    FileInputFormat.setInputPaths(job,CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(SeekMapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, READ_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }


  public static void main(String[] args) throws Exception {
    int megaBytes = 10;
    int files = 100;
    boolean noRead = false;
    boolean noWrite = false;
    boolean noSeek = false;
    boolean fastCheck = false;
    long seed = new Random().nextLong();

    String usage = "Usage: TestFileSystem -files N -megaBytes M [-noread] [-nowrite] [-noseek] [-fastcheck]";
    
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].equals("-files")) {
        files = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-megaBytes")) {
        megaBytes = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-noread")) {
        noRead = true;
      } else if (args[i].equals("-nowrite")) {
        noWrite = true;
      } else if (args[i].equals("-noseek")) {
        noSeek = true;
      } else if (args[i].equals("-fastcheck")) {
        fastCheck = true;
      }
    }

    LOG.info("seed = "+seed);
    LOG.info("files = " + files);
    LOG.info("megaBytes = " + megaBytes);
  
    FileSystem fs = FileSystem.get(conf);

    if (!noWrite) {
      createControlFile(fs, megaBytes*MEGA, files, seed);
      writeTest(fs, fastCheck);
    }
    if (!noRead) {
      readTest(fs, fastCheck);
    }
    if (!noSeek) {
      seekTest(fs, fastCheck);
    }
  }

  public void testFsCache() throws Exception {
    {
      long now = System.currentTimeMillis();
      String[] users = new String[]{"foo","bar"};
      final Configuration conf = new Configuration();
      FileSystem[] fs = new FileSystem[users.length];
  
      for(int i = 0; i < users.length; i++) {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(users[i]);
        fs[i] = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException {
            return FileSystem.get(conf);
        }});
        for(int j = 0; j < i; j++) {
          assertFalse(fs[j] == fs[i]);
        }
      }
      FileSystem.closeAll();
    }
    
    {
      try {
        runTestCache(NameNode.DEFAULT_PORT);
      } catch(java.net.BindException be) {
        LOG.warn("Cannot test NameNode.DEFAULT_PORT (="
            + NameNode.DEFAULT_PORT + ")", be);
      }

      runTestCache(0);
    }
  }
  
  static void runTestCache(int port) throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(port, conf, 2, true, true, null, null);
      URI uri = cluster.getFileSystem().getUri();
      LOG.info("uri=" + uri);

      {
        FileSystem fs = FileSystem.get(uri, new Configuration());
        checkPath(cluster, fs);
        for(int i = 0; i < 100; i++) {
          assertTrue(fs == FileSystem.get(uri, new Configuration()));
        }
      }
      
      if (port == NameNode.DEFAULT_PORT) {
        //test explicit default port
        URI uri2 = new URI(uri.getScheme(), uri.getUserInfo(),
            uri.getHost(), NameNode.DEFAULT_PORT, uri.getPath(),
            uri.getQuery(), uri.getFragment());  
        LOG.info("uri2=" + uri2);
        FileSystem fs = FileSystem.get(uri2, conf);
        checkPath(cluster, fs);
        for(int i = 0; i < 100; i++) {
          assertTrue(fs == FileSystem.get(uri2, new Configuration()));
        }
      }
    } finally {
      if (cluster != null) cluster.shutdown(); 
    }
  }
  
  static void checkPath(MiniDFSCluster cluster, FileSystem fileSys) throws IOException {
    InetSocketAddress add = cluster.getNameNode().getNameNodeAddress();
    // Test upper/lower case
    fileSys.checkPath(new Path("hdfs://" + add.getHostName().toUpperCase() + ":" + add.getPort()));
  }

  public void testFsClose() throws Exception {
    {
      Configuration conf = new Configuration();
      new Path("file:///").getFileSystem(conf);
      FileSystem.closeAll();
    }

    {
      Configuration conf = new Configuration();
      new Path("hftp://localhost:12345/").getFileSystem(conf);
      FileSystem.closeAll();
    }

    {
      Configuration conf = new Configuration();
      FileSystem fs = new Path("hftp://localhost:12345/").getFileSystem(conf);
      FileSystem.closeAll();
    }
  }

  public void testFsShutdownHook() throws Exception {
    final Set<FileSystem> closed = Collections.synchronizedSet(new HashSet<FileSystem>());
    Configuration conf = new Configuration();
    Configuration confNoAuto = new Configuration();

    conf.setClass("fs.test.impl", TestShutdownFileSystem.class, FileSystem.class);
    confNoAuto.setClass("fs.test.impl", TestShutdownFileSystem.class, FileSystem.class);
    confNoAuto.setBoolean("fs.automatic.close", false);

    TestShutdownFileSystem fsWithAuto =
      (TestShutdownFileSystem)(new Path("test://a/").getFileSystem(conf));
    TestShutdownFileSystem fsWithoutAuto =
      (TestShutdownFileSystem)(new Path("test://b/").getFileSystem(confNoAuto));

    fsWithAuto.setClosedSet(closed);
    fsWithoutAuto.setClosedSet(closed);

    // Different URIs should result in different FS instances
    assertNotSame(fsWithAuto, fsWithoutAuto);

    FileSystem.CACHE.closeAll(null, true);
    assertEquals(1, closed.size());
    assertTrue(closed.contains(fsWithAuto));

    closed.clear();

    FileSystem.closeAll();
    assertEquals(1, closed.size());
    assertTrue(closed.contains(fsWithoutAuto));
  }


  public void testCacheKeysAreCaseInsensitive()
    throws Exception
  {
    Configuration conf = new Configuration();
    
    // check basic equality
    FileSystem.Cache.Key lowercaseCachekey1 = new FileSystem.Cache.Key(new URI("hftp://localhost:12345/"), conf);
    FileSystem.Cache.Key lowercaseCachekey2 = new FileSystem.Cache.Key(new URI("hftp://localhost:12345/"), conf);
    assertEquals( lowercaseCachekey1, lowercaseCachekey2 );

    // check insensitive equality    
    FileSystem.Cache.Key uppercaseCachekey = new FileSystem.Cache.Key(new URI("HFTP://Localhost:12345/"), conf);
    assertEquals( lowercaseCachekey2, uppercaseCachekey );

    // check behaviour with collections
    List<FileSystem.Cache.Key> list = new ArrayList<FileSystem.Cache.Key>();
    list.add(uppercaseCachekey);
    assertTrue(list.contains(uppercaseCachekey));
    assertTrue(list.contains(lowercaseCachekey2));

    Set<FileSystem.Cache.Key> set = new HashSet<FileSystem.Cache.Key>();
    set.add(uppercaseCachekey);
    assertTrue(set.contains(uppercaseCachekey));
    assertTrue(set.contains(lowercaseCachekey2));

    Map<FileSystem.Cache.Key, String> map = new HashMap<FileSystem.Cache.Key, String>();
    map.put(uppercaseCachekey, "");
    assertTrue(map.containsKey(uppercaseCachekey));
    assertTrue(map.containsKey(lowercaseCachekey2));    

  }

  public static class TestShutdownFileSystem extends RawLocalFileSystem {
    private Set<FileSystem> closedSet;

    public void setClosedSet(Set<FileSystem> closedSet) {
      this.closedSet = closedSet;
    }
    public void close() throws IOException {
      if (closedSet != null) {
        closedSet.add(this);
      }
      super.close();
    }
  }

  public static void testFsUniqueness(long megaBytes, int numFiles, long seed)
    throws Exception {

    // multiple invocations of FileSystem.get return the same object.
    FileSystem fs1 = FileSystem.get(conf);
    FileSystem fs2 = FileSystem.get(conf);
    assertTrue(fs1 == fs2);

    // multiple invocations of FileSystem.newInstance return different objects
    fs1 = FileSystem.newInstance(conf);
    fs2 = FileSystem.newInstance(conf);
    assertTrue(fs1 != fs2 && !fs1.equals(fs2));
    fs1.close();
    fs2.close();
  }

  @SuppressWarnings("unchecked")
  public <T extends TokenIdentifier> void testCacheForUgi() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    UserGroupInformation ugiA = UserGroupInformation.createRemoteUser("foo");
    UserGroupInformation ugiB = UserGroupInformation.createRemoteUser("bar");
    FileSystem fsA = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    FileSystem fsA1 = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Since the UGIs are the same, we should have the same filesystem for both
    assertSame(fsA, fsA1);
    
    FileSystem fsB = ugiB.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Since the UGIs are different, we should end up with different filesystems
    //corresponding to the two UGIs
    assertNotSame(fsA, fsB);
    
    Token<T> t1 = mock(Token.class);
    UserGroupInformation ugiA2 = UserGroupInformation.createRemoteUser("foo");
    
    fsA = ugiA2.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    // Although the users in the UGI are same, they have different subjects
    // and so are different.
    assertNotSame(fsA, fsA1);
    
    ugiA.addToken(t1);
    
    fsA = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    // Make sure that different UGI's with the same subject lead to the same
    // file system.
    assertSame(fsA, fsA1);
  }
  
  public void testCloseAllForUGI() throws Exception {
    final Configuration conf = new Configuration();
    conf.set("fs.cachedfile.impl", conf.get("fs.file.impl"));
    UserGroupInformation ugiA = UserGroupInformation.createRemoteUser("foo");
    FileSystem fsA = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    //Now we should get the cached filesystem
    FileSystem fsA1 = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    assertSame(fsA, fsA1);
    
    FileSystem.closeAllForUGI(ugiA);
    
    //Now we should get a different (newly created) filesystem
    fsA1 = ugiA.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws Exception {
        return FileSystem.get(new URI("cachedfile://a"), conf);
      }
    });
    assertNotSame(fsA, fsA1);
  }
}
