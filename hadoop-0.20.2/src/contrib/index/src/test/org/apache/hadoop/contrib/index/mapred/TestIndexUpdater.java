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
import org.apache.hadoop.contrib.index.lucene.FileSystemDirectory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;

import junit.framework.TestCase;

public class TestIndexUpdater extends TestCase {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  // however, "we only allow 0 or 1 reducer in local mode" - from
  // LocalJobRunner
  private Configuration conf;
  private Path localInputPath = new Path(System.getProperty("build.test") + "/sample/data.txt");
  private Path inputPath = new Path("/myexample/data.txt");
  private Path outputPath = new Path("/myoutput");
  private Path indexPath = new Path("/myindex");
  private int initNumShards = 3;
  private int numMapTasks = 5;

  private int numDataNodes = 3;
  private int numTaskTrackers = 3;

  private int numRuns = 3;
  private int numDocsPerRun = 10; // num of docs in local input path

  private FileSystem fs;
  private MiniDFSCluster dfsCluster;
  private MiniMRCluster mrCluster;

  public TestIndexUpdater() throws IOException {
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

  public void testIndexUpdater() throws IOException {
    IndexUpdateConfiguration iconf = new IndexUpdateConfiguration(conf);
    // max field length, compound file and number of segments will be checked
    // later
    iconf.setIndexMaxFieldLength(2);
    iconf.setIndexUseCompoundFile(true);
    iconf.setIndexMaxNumSegments(1);

    long versionNumber = -1;
    long generation = -1;

    for (int i = 0; i < numRuns; i++) {
      if (fs.exists(outputPath)) {
        fs.delete(outputPath);
      }

      Shard[] shards = new Shard[initNumShards + i];
      for (int j = 0; j < shards.length; j++) {
        shards[j] =
            new Shard(versionNumber, new Path(indexPath,
                NUMBER_FORMAT.format(j)).toString(), generation);
      }
      run(i + 1, shards);
    }
  }

  private void run(int numRuns, Shard[] shards) throws IOException {
    IIndexUpdater updater = new IndexUpdater();
    updater.run(conf, new Path[] { inputPath }, outputPath, numMapTasks,
        shards);

    // verify the done files
    Path[] doneFileNames = new Path[shards.length];
    int count = 0;
    FileStatus[] fileStatus = fs.listStatus(outputPath);
    for (int i = 0; i < fileStatus.length; i++) {
      FileStatus[] doneFiles = fs.listStatus(fileStatus[i].getPath());
      for (int j = 0; j < doneFiles.length; j++) {
        doneFileNames[count++] = doneFiles[j].getPath();
      }
    }
    assertEquals(shards.length, count);
    for (int i = 0; i < count; i++) {
      assertTrue(doneFileNames[i].getName().startsWith(
          IndexUpdateReducer.DONE.toString()));
    }

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

    assertEquals(numRuns * numDocsPerRun, hits.length());

    int[] counts = new int[numDocsPerRun];
    for (int i = 0; i < hits.length(); i++) {
      Document doc = hits.doc(i);
      counts[Integer.parseInt(doc.get("id"))]++;
    }

    for (int i = 0; i < numDocsPerRun; i++) {
      assertEquals(numRuns, counts[i]);
    }

    // max field length is 2, so "dot" is also indexed but not "org"
    hits = searcher.search(new TermQuery(new Term("content", "dot")));
    assertEquals(numRuns, hits.length());

    hits = searcher.search(new TermQuery(new Term("content", "org")));
    assertEquals(0, hits.length());

    searcher.close();
    reader.close();

    // open and close an index writer with KeepOnlyLastCommitDeletionPolicy
    // to remove earlier checkpoints
    for (int i = 0; i < shards.length; i++) {
      Directory dir =
          new FileSystemDirectory(fs, new Path(shards[i].getDirectory()),
              false, conf);
      IndexWriter writer =
          new IndexWriter(dir, false, null,
              new KeepOnlyLastCommitDeletionPolicy());
      writer.close();
    }

    // verify the number of segments, must be done after an writer with
    // KeepOnlyLastCommitDeletionPolicy so that earlier checkpoints are removed
    for (int i = 0; i < shards.length; i++) {
      PathFilter cfsFilter = new PathFilter() {
        public boolean accept(Path path) {
          return path.getName().endsWith(".cfs");
        }
      };
      FileStatus[] cfsFiles =
          fs.listStatus(new Path(shards[i].getDirectory()), cfsFilter);
      assertEquals(1, cfsFiles.length);
    }
  }

}
