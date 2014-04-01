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
package org.apache.hadoop.filecache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Tests the use of the
 * {@link org.apache.hadoop.mapreduce.filecache.DistributedCache} within the
 * full MR flow as well as the LocalJobRunner. This ought to be part of the
 * filecache package, but that package is not currently in mapred, so cannot
 * depend on MR for testing.
 * 
 * We use the distributed.* namespace for temporary files.
 * 
 * See {@link TestMiniMRLocalFS}, {@link TestMiniMRDFSCaching}, and
 * {@link MRCaching} for other tests that test the distributed cache.
 * 
 * This test is not fast: it uses MiniMRCluster.
 */
public class TestMRWithDistributedCache extends TestCase {
  private static Path TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static final Log LOG =
    LogFactory.getLog(TestMRWithDistributedCache.class);

  public static class DistributedCacheChecker extends
      Mapper<LongWritable, Text, NullWritable, NullWritable> {

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      Path[] files = DistributedCache.getLocalCacheFiles(conf);
      Path[] archives = DistributedCache.getLocalCacheArchives(conf);
      FileSystem fs = LocalFileSystem.get(conf);

      // Check that 2 files and 2 archives are present
      TestCase.assertEquals(2, files.length);
      TestCase.assertEquals(2, archives.length);

      // Check lengths of the files
      TestCase.assertEquals(1, fs.getFileStatus(files[0]).getLen());
      TestCase.assertTrue(fs.getFileStatus(files[1]).getLen() > 1);

      // Check extraction of the archive
      TestCase.assertTrue(fs.exists(new Path(archives[0],
          "distributed.jar.inside3")));
      TestCase.assertTrue(fs.exists(new Path(archives[1],
          "distributed.jar.inside4")));

      // Check the class loaders
      LOG.info("Java Classpath: " + System.getProperty("java.class.path"));
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      // Both the file and the archive were added to classpath, so both
      // should be reachable via the class loader.
      TestCase.assertNotNull(cl.getResource("distributed.jar.inside2"));
      TestCase.assertNotNull(cl.getResource("distributed.jar.inside3"));
      TestCase.assertNull(cl.getResource("distributed.jar.inside4"));


      // Check that the symlink for the renaming was created in the cwd;
      // This only happens for real for non-local jobtrackers.
      // (The symlinks exist in "localRunner/" for local Jobtrackers,
      // but the user has no way to get at them.
      if (!"local".equals(
          context.getConfiguration().get("mapred.job.tracker"))) {
        File symlinkFile = new File("distributed.first.symlink");
        TestCase.assertTrue(symlinkFile.exists());
        TestCase.assertEquals(1, symlinkFile.length());
      }
    }
  }

  private void testWithConf(JobConf conf) throws IOException,
      InterruptedException, ClassNotFoundException, URISyntaxException {
    // Create a temporary file of length 1.
    Path first = createTempFile("distributed.first", "x");
    // Create two jars with a single file inside them.
    Path second =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.second.jar"), 2);
    Path third =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.third.jar"), 3);
    Path fourth =
        makeJar(new Path(TEST_ROOT_DIR, "distributed.fourth.jar"), 4);

    // Creates the Job Configuration
    DistributedCache.addCacheFile(
        new URI(first.toUri().toString() + "#distributed.first.symlink"),
        conf);
    FileSystem fs = FileSystem.get(conf);
    DistributedCache.addFileToClassPath(second, conf, fs);
    DistributedCache.addArchiveToClassPath(third, conf, fs);
    DistributedCache.addCacheArchive(fourth.toUri(), conf);
    DistributedCache.createSymlink(conf);

    conf.setMaxMapAttempts(1); // speed up failures
    Job job = new Job(conf);
    job.setMapperClass(DistributedCacheChecker.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    FileInputFormat.setInputPaths(job, first);

    job.submit();
    assertTrue(job.waitForCompletion(false));
  }

  /** Tests using the local job runner. */
  public void testLocalJobRunner() throws Exception {
    JobConf c = new JobConf();
    c.set("mapred.job.tracker", "local");
    c.set("fs.default.name", "file:///");
    testWithConf(c);
  }

  /** Tests using a full MiniMRCluster. */
  public void testMiniMRJobRunner() throws Exception {
    MiniMRCluster m = new MiniMRCluster(1, "file:///", 1);
    try {
      testWithConf(m.createJobConf());
    } finally {
      m.shutdown();
    }

  }

  private Path createTempFile(String filename, String contents)
      throws IOException {
    Path path = new Path(TEST_ROOT_DIR, filename);
    FSDataOutputStream os = localFs.create(path);
    os.writeBytes(contents);
    os.close();
    return path;
  }

  private Path makeJar(Path p, int index) throws FileNotFoundException,
      IOException {
    FileOutputStream fos = new FileOutputStream(new File(p.toString()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
    jos.putNextEntry(ze);
    jos.write(("inside the jar!" + index).getBytes());
    jos.closeEntry();
    jos.close();
    return p;
  }
}
