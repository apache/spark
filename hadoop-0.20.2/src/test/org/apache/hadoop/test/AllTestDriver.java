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

package org.apache.hadoop.test;

import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.mapred.BigMapOutput;
import org.apache.hadoop.mapred.GenericMRLoadGenerator;
import org.apache.hadoop.mapred.MRBench;
import org.apache.hadoop.mapred.ReliabilityTest;
import org.apache.hadoop.mapred.SortValidator;
import org.apache.hadoop.mapred.TestMapRed;
import org.apache.hadoop.mapred.TestSequenceFileInputFormat;
import org.apache.hadoop.mapred.TestTextInputFormat;
import org.apache.hadoop.hdfs.BenchmarkThroughput;
import org.apache.hadoop.hdfs.NNBench;
import org.apache.hadoop.fs.DistributedFSCheck;
import org.apache.hadoop.fs.TestDFSIO;
import org.apache.hadoop.fs.DFSCIOTest;
import org.apache.hadoop.fs.TestFileSystem;
import org.apache.hadoop.io.FileBench;
import org.apache.hadoop.io.TestArrayFile;
import org.apache.hadoop.io.TestSequenceFile;
import org.apache.hadoop.io.TestSetFile;
import org.apache.hadoop.ipc.TestIPC;
import org.apache.hadoop.ipc.TestRPC;
import org.apache.hadoop.mapred.ThreadedMapBenchmark;

public class AllTestDriver {
  
  /**
   * A description of the test program for running all the tests using jar file
   */
  public static void main(String argv[]){
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("threadedmapbench", ThreadedMapBenchmark.class, 
                   "A map/reduce benchmark that compares the performance " + 
                   "of maps with multiple spills over maps with 1 spill");
      pgd.addClass("mrbench", MRBench.class, "A map/reduce benchmark that can create many small jobs");
      pgd.addClass("nnbench", NNBench.class, "A benchmark that stresses the namenode.");
      pgd.addClass("mapredtest", TestMapRed.class, "A map/reduce test check.");
      pgd.addClass("testfilesystem", TestFileSystem.class, "A test for FileSystem read/write.");
      pgd.addClass("testsequencefile", TestSequenceFile.class, "A test for flat files of binary key value pairs.");
      pgd.addClass("testsetfile", TestSetFile.class, "A test for flat files of binary key/value pairs.");
      pgd.addClass("testarrayfile", TestArrayFile.class, "A test for flat files of binary key/value pairs.");
      pgd.addClass("testrpc", TestRPC.class, "A test for rpc.");
      pgd.addClass("testipc", TestIPC.class, "A test for ipc.");
      pgd.addClass("testsequencefileinputformat", TestSequenceFileInputFormat.class, "A test for sequence file input format.");
      pgd.addClass("testtextinputformat", TestTextInputFormat.class, "A test for text input format.");
      pgd.addClass("TestDFSIO", TestDFSIO.class, "Distributed i/o benchmark.");
      pgd.addClass("DFSCIOTest", DFSCIOTest.class, "Distributed i/o benchmark of libhdfs.");
      pgd.addClass("DistributedFSCheck", DistributedFSCheck.class, "Distributed checkup of the file system consistency.");
      pgd.addClass("testmapredsort", SortValidator.class, 
                   "A map/reduce program that validates the map-reduce framework's sort.");
      pgd.addClass("testbigmapoutput", BigMapOutput.class, 
                   "A map/reduce program that works on a very big " + 
                   "non-splittable file and does identity map/reduce");
      pgd.addClass("loadgen", GenericMRLoadGenerator.class, "Generic map/reduce load generator");
      pgd.addClass("filebench", FileBench.class, "Benchmark SequenceFile(Input|Output)Format (block,record compressed and uncompressed), Text(Input|Output)Format (compressed and uncompressed)");
      pgd.addClass("dfsthroughput", BenchmarkThroughput.class, 
                   "measure hdfs throughput");
      pgd.addClass("MRReliabilityTest", ReliabilityTest.class,
          "A program that tests the reliability of the MR framework by " +
          "injecting faults/failures");
      pgd.addClass("minicluster", MiniHadoopClusterManager.class,
          "Single process HDFS and MR cluster.");
      pgd.driver(argv);
    } catch(Throwable e) {
      e.printStackTrace();
    }
  }
}

