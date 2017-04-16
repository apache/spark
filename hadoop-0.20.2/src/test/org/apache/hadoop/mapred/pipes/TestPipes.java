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

package org.apache.hadoop.mapred.pipes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

public class TestPipes extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestPipes.class.getName());
  
  private static Path cppExamples = 
    new Path(System.getProperty("install.c++.examples"));
  static Path wordCountSimple = 
    new Path(cppExamples, "bin/wordcount-simple");
  static Path wordCountPart = 
    new Path(cppExamples, "bin/wordcount-part");
  static Path wordCountNoPipes = 
    new Path(cppExamples,"bin/wordcount-nopipe");
  
  static Path nonPipedOutDir;
  
  static void cleanup(FileSystem fs, Path p) throws IOException {
    fs.delete(p, true);
    assertFalse("output not cleaned up", fs.exists(p));
  }

  public void testPipes() throws IOException {
    if (System.getProperty("compile.c++") == null) {
      LOG.info("compile.c++ is not defined, so skipping TestPipes");
      return;
    }
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    Path inputPath = new Path("testing/in");
    Path outputPath = new Path("testing/out");
    try {
      final int numSlaves = 2;
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, numSlaves, true, null);
      mr = new MiniMRCluster(numSlaves, dfs.getFileSystem().getName(), 1);
      writeInputFile(dfs.getFileSystem(), inputPath);
      runProgram(mr, dfs, wordCountSimple, 
                 inputPath, outputPath, 3, 2, twoSplitOutput, null);
      cleanup(dfs.getFileSystem(), outputPath);
      runProgram(mr, dfs, wordCountSimple, 
                 inputPath, outputPath, 3, 0, noSortOutput, null);
      cleanup(dfs.getFileSystem(), outputPath);
      runProgram(mr, dfs, wordCountPart,
                 inputPath, outputPath, 3, 2, fixedPartitionOutput, null);
      runNonPipedProgram(mr, dfs, wordCountNoPipes, null);
      mr.waitUntilIdle();
    } finally {
      mr.shutdown();
      dfs.shutdown();
    }
  }


  final static String[] twoSplitOutput = new String[] {
    "`and\t1\na\t1\nand\t1\nbeginning\t1\nbook\t1\nbut\t1\nby\t1\n" +
    "conversation?'\t1\ndo:\t1\nhad\t2\nhaving\t1\nher\t2\nin\t1\nit\t1\n"+
    "it,\t1\nno\t1\nnothing\t1\nof\t3\non\t1\nonce\t1\nor\t3\npeeped\t1\n"+
    "pictures\t2\nthe\t3\nthought\t1\nto\t2\nuse\t1\nwas\t2\n",

    "Alice\t2\n`without\t1\nbank,\t1\nbook,'\t1\nconversations\t1\nget\t1\n" +
    "into\t1\nis\t1\nreading,\t1\nshe\t1\nsister\t2\nsitting\t1\ntired\t1\n" +
    "twice\t1\nvery\t1\nwhat\t1\n"
  };

  final static String[] noSortOutput = new String[] {
    "it,\t1\n`and\t1\nwhat\t1\nis\t1\nthe\t1\nuse\t1\nof\t1\na\t1\n" +
    "book,'\t1\nthought\t1\nAlice\t1\n`without\t1\npictures\t1\nor\t1\n"+
    "conversation?'\t1\n",

    "Alice\t1\nwas\t1\nbeginning\t1\nto\t1\nget\t1\nvery\t1\ntired\t1\n"+
    "of\t1\nsitting\t1\nby\t1\nher\t1\nsister\t1\non\t1\nthe\t1\nbank,\t1\n"+
    "and\t1\nof\t1\nhaving\t1\nnothing\t1\nto\t1\ndo:\t1\nonce\t1\n", 

    "or\t1\ntwice\t1\nshe\t1\nhad\t1\npeeped\t1\ninto\t1\nthe\t1\nbook\t1\n"+
    "her\t1\nsister\t1\nwas\t1\nreading,\t1\nbut\t1\nit\t1\nhad\t1\nno\t1\n"+
    "pictures\t1\nor\t1\nconversations\t1\nin\t1\n"
  };
  
  final static String[] fixedPartitionOutput = new String[] {
    "Alice\t2\n`and\t1\n`without\t1\na\t1\nand\t1\nbank,\t1\nbeginning\t1\n" +
    "book\t1\nbook,'\t1\nbut\t1\nby\t1\nconversation?'\t1\nconversations\t1\n"+
    "do:\t1\nget\t1\nhad\t2\nhaving\t1\nher\t2\nin\t1\ninto\t1\nis\t1\n" +
    "it\t1\nit,\t1\nno\t1\nnothing\t1\nof\t3\non\t1\nonce\t1\nor\t3\n" +
    "peeped\t1\npictures\t2\nreading,\t1\nshe\t1\nsister\t2\nsitting\t1\n" +
    "the\t3\nthought\t1\ntired\t1\nto\t2\ntwice\t1\nuse\t1\n" +
    "very\t1\nwas\t2\nwhat\t1\n",
    
    ""                                                   
  };
  
  static void writeInputFile(FileSystem fs, Path dir) throws IOException {
    DataOutputStream out = fs.create(new Path(dir, "part0"));
    out.writeBytes("Alice was beginning to get very tired of sitting by her\n");
    out.writeBytes("sister on the bank, and of having nothing to do: once\n");
    out.writeBytes("or twice she had peeped into the book her sister was\n");
    out.writeBytes("reading, but it had no pictures or conversations in\n");
    out.writeBytes("it, `and what is the use of a book,' thought Alice\n");
    out.writeBytes("`without pictures or conversation?'\n");
    out.close();
  }

  static void runProgram(MiniMRCluster mr, MiniDFSCluster dfs, 
                          Path program, Path inputPath, Path outputPath,
                          int numMaps, int numReduces, String[] expectedResults,
                          JobConf conf
                         ) throws IOException {
    Path wordExec = new Path("testing/bin/application");
    JobConf job = null;
    if(conf == null) {
      job = mr.createJobConf();
    }else {
      job = new JobConf(conf);
    } 
    job.setNumMapTasks(numMaps);
    job.setNumReduceTasks(numReduces);
    {
      FileSystem fs = dfs.getFileSystem();
      fs.delete(wordExec.getParent(), true);
      fs.copyFromLocalFile(program, wordExec);                                         
      Submitter.setExecutable(job, fs.makeQualified(wordExec).toString());
      Submitter.setIsJavaRecordReader(job, true);
      Submitter.setIsJavaRecordWriter(job, true);
      FileInputFormat.setInputPaths(job, inputPath);
      FileOutputFormat.setOutputPath(job, outputPath);
      RunningJob rJob = null;
      if (numReduces == 0) {
        rJob = Submitter.jobSubmit(job);
        
        while (!rJob.isComplete()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
          }
        }
      } else {
        rJob = Submitter.runJob(job);
      }
      assertTrue("pipes job failed", rJob.isSuccessful());
      
      Counters counters = rJob.getCounters();
      Counters.Group wordCountCounters = counters.getGroup("WORDCOUNT");
      int numCounters = 0;
      for (Counter c : wordCountCounters) {
        System.out.println(c);
        ++numCounters;
      }
      assertTrue("No counters found!", (numCounters > 0));
    }

    List<String> results = new ArrayList<String>();
    for (Path p:FileUtil.stat2Paths(dfs.getFileSystem().listStatus(outputPath,
        new Utils.OutputFileUtils.OutputFilesFilter()))) {
      results.add(MapReduceTestUtil.readOutput(p, job));
    }
    assertEquals("number of reduces is wrong", 
                 expectedResults.length, results.size());
    for(int i=0; i < results.size(); i++) {
      assertEquals("pipes program " + program + " output " + i + " wrong",
                   expectedResults[i], results.get(i));
    }
  }
  
  /**
   * Run a map/reduce word count that does all of the map input and reduce
   * output directly rather than sending it back up to Java.
   * @param mr The mini mr cluster
   * @param dfs the dfs cluster
   * @param program the program to run
   * @throws IOException
   */
  static void runNonPipedProgram(MiniMRCluster mr, MiniDFSCluster dfs,
                                  Path program, JobConf conf) throws IOException {
    JobConf job;
    if(conf == null) {
      job = mr.createJobConf();
    }else {
      job = new JobConf(conf);
    }
    
    job.setInputFormat(WordCountInputFormat.class);
    FileSystem local = FileSystem.getLocal(job);
    Path testDir = new Path("file:" + System.getProperty("test.build.data"), 
                            "pipes");
    Path inDir = new Path(testDir, "input");
    nonPipedOutDir = new Path(testDir, "output");
    Path wordExec = new Path("testing/bin/application");
    Path jobXml = new Path(testDir, "job.xml");
    {
      FileSystem fs = dfs.getFileSystem();
      fs.delete(wordExec.getParent(), true);
      fs.copyFromLocalFile(program, wordExec);
    }
    DataOutputStream out = local.create(new Path(inDir, "part0"));
    out.writeBytes("i am a silly test\n");
    out.writeBytes("you are silly\n");
    out.writeBytes("i am a cat test\n");
    out.writeBytes("you is silly\n");
    out.writeBytes("i am a billy test\n");
    out.writeBytes("hello are silly\n");
    out.close();
    out = local.create(new Path(inDir, "part1"));
    out.writeBytes("mall world things drink java\n");
    out.writeBytes("hall silly cats drink java\n");
    out.writeBytes("all dogs bow wow\n");
    out.writeBytes("hello drink java\n");
    local.delete(nonPipedOutDir, true);
    local.mkdirs(nonPipedOutDir, new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.ALL));
    out.close();
    out = local.create(jobXml);
    job.writeXml(out);
    out.close();
    System.err.println("About to run: Submitter -conf " + jobXml + " -input "
        + inDir + " -output " + nonPipedOutDir + " -program "
        + dfs.getFileSystem().makeQualified(wordExec));
    try {
      int ret = ToolRunner.run(new Submitter(),
                               new String[]{"-conf", jobXml.toString(),
                                  "-input", inDir.toString(),
                                  "-output", nonPipedOutDir.toString(),
                                  "-program", 
                        dfs.getFileSystem().makeQualified(wordExec).toString(),
                                  "-reduces", "2"});
      assertEquals(0, ret);
    } catch (Exception e) {
      assertTrue("got exception: " + StringUtils.stringifyException(e), false);
    }
  }
}
