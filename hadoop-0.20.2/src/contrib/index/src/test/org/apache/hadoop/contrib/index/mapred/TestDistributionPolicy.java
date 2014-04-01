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

package org.apache.hadoop.contrib.index.mapred;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.index.example.HashingDistributionPolicy;
import org.apache.hadoop.contrib.index.example.RoundRobinDistributionPolicy;
import org.apache.hadoop.contrib.index.lucene.FileSystemDirectory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;

import junit.framework.TestCase;

public class TestDistributionPolicy extends TestCase {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  // however, "we only allow 0 or 1 reducer in local mode" - from
  // LocalJobRunner
  private Configuration conf;
  private Path localInputPath = new Path(System.getProperty("build.test") + "/sample/data.txt");
  private Path localUpdatePath =
      new Path(System.getProperty("build.test") + "/sample/data2.txt");
  private Path inputPath = new Path("/myexample/data.txt");
  private Path updatePath = new Path("/myexample/data2.txt");
  private Path outputPath = new Path("/myoutput");
  private Path indexPath = new Path("/myindex");
  private int numShards = 3;
  private int numMapTasks = 5;

  private int numDataNodes = 3;
  private int numTaskTrackers = 3;

  private int numDocsPerRun = 10; // num of docs in local input path

  private FileSystem fs;
  private MiniDFSCluster dfsCluster;
  private MiniMRCluster mrCluster;

  public TestDistributionPolicy() throws IOException {
    super();
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    conf = new Configuration();
  }

  protected void setUp() throws Exception {
    super.setUp();
    try {
      dfsCluster =
          new MiniDFSCluster(conf, numDataNodes, true, (String[]) null);

      fs = dfsCluster.getFileSystem();
      if (fs.exists(inputPath)) {
        fs.delete(inputPath);
      }
      fs.copyFromLocalFile(localInputPath, inputPath);
      if (fs.exists(updatePath)) {
        fs.delete(updatePath);
      }
      fs.copyFromLocalFile(localUpdatePath, updatePath);

      if (fs.exists(outputPath)) {
        // do not create, mapred will create
        fs.delete(outputPath);
      }

      if (fs.exists(indexPath)) {
        fs.delete(indexPath);
      }

      mrCluster =
          new MiniMRCluster(numTaskTrackers, fs.getUri().toString(), 1);

    } catch (IOException e) {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
        dfsCluster = null;
      }

      if (fs != null) {
        fs.close();
        fs = null;
      }

      if (mrCluster != null) {
        mrCluster.shutdown();
        mrCluster = null;
      }

      throw e;
    }

  }

  protected void tearDown() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }

    if (fs != null) {
      fs.close();
      fs = null;
    }

    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }

    super.tearDown();
  }

  public void testDistributionPolicy() throws IOException {
    IndexUpdateConfiguration iconf = new IndexUpdateConfiguration(conf);

    // test hashing distribution policy
    iconf.setDistributionPolicyClass(HashingDistributionPolicy.class);
    onetest();

    if (fs.exists(indexPath)) {
      fs.delete(indexPath);
    }

    // test round-robin distribution policy
    iconf.setDistributionPolicyClass(RoundRobinDistributionPolicy.class);
    onetest();
  }

  private void onetest() throws IOException {
    long versionNumber = -1;
    long generation = -1;

    Shard[] shards = new Shard[numShards];
    for (int j = 0; j < shards.length; j++) {
      shards[j] =
          new Shard(versionNumber,
              new Path(indexPath, NUMBER_FORMAT.format(j)).toString(),
              generation);
    }

    if (fs.exists(outputPath)) {
      fs.delete(outputPath);
    }

    IIndexUpdater updater = new IndexUpdater();
    updater.run(conf, new Path[] { inputPath }, outputPath, numMapTasks,
        shards);

    if (fs.exists(outputPath)) {
      fs.delete(outputPath);
    }

    // delete docs w/ even docids, update docs w/ odd docids
    updater.run(conf, new Path[] { updatePath }, outputPath, numMapTasks,
        shards);

    verify(shards);
  }

  private void verify(Shard[] shards) throws IOException {
    // verify the index
    IndexReader[] readers = new IndexReader[shards.length];
    for (int i = 0; i < shards.length; i++) {
      Directory dir =
          new FileSystemDirectory(fs, new Path(shards[i].getDirectory()),
              false, conf);
      readers[i] = IndexReader.open(dir);
    }

    IndexReader reader = new MultiReader(readers);
    IndexSearcher searcher = new IndexSearcher(reader);
    Hits hits = searcher.search(new TermQuery(new Term("content", "apache")));
    assertEquals(0, hits.length());

    hits = searcher.search(new TermQuery(new Term("content", "hadoop")));
    assertEquals(numDocsPerRun / 2, hits.length());

    int[] counts = new int[numDocsPerRun];
    for (int i = 0; i < hits.length(); i++) {
      Document doc = hits.doc(i);
      counts[Integer.parseInt(doc.get("id"))]++;
    }

    for (int i = 0; i < numDocsPerRun; i++) {
      if (i % 2 == 0) {
        assertEquals(0, counts[i]);
      } else {
        assertEquals(1, counts[i]);
      }
    }

    searcher.close();
    reader.close();
  }

}
