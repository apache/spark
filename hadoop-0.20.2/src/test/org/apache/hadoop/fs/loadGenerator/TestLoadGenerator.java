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
package org.apache.hadoop.fs.loadGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.junit.Assert.*;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * This class tests if a balancer schedules tasks correctly.
 */
public class TestLoadGenerator extends Configured implements Tool {
  private static final Configuration CONF = new Configuration();
  private static final int DEFAULT_BLOCK_SIZE = 10;
  private static final String OUT_DIR = 
    System.getProperty("test.build.data","build/test/data");
  private static final File DIR_STRUCTURE_FILE = 
    new File(OUT_DIR, StructureGenerator.DIR_STRUCTURE_FILE_NAME);
  private static final File FILE_STRUCTURE_FILE =
    new File(OUT_DIR, StructureGenerator.FILE_STRUCTURE_FILE_NAME);
  private static final String DIR_STRUCTURE_FIRST_LINE = "/dir0";
  private static final String DIR_STRUCTURE_SECOND_LINE = "/dir1";
  private static final String FILE_STRUCTURE_FIRST_LINE =
    "/dir0/_file_0 0.3754598635933768";
  private static final String FILE_STRUCTURE_SECOND_LINE =
    "/dir1/_file_1 1.4729310851145203";
  

  static {
    CONF.setLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    CONF.setInt("io.bytes.per.checksum", DEFAULT_BLOCK_SIZE);
    CONF.setLong("dfs.heartbeat.interval", 1L);
  }

  /** Test if the structure generator works fine */ 
  @Test
  public void testStructureGenerator() throws Exception {
    StructureGenerator sg = new StructureGenerator();
    String[] args = new String[]{"-maxDepth", "2", "-minWidth", "1",
        "-maxWidth", "2", "-numOfFiles", "2",
        "-avgFileSize", "1", "-outDir", OUT_DIR, "-seed", "1"};
    
    final int MAX_DEPTH = 1;
    final int MIN_WIDTH = 3;
    final int MAX_WIDTH = 5;
    final int NUM_OF_FILES = 7;
    final int AVG_FILE_SIZE = 9;
    final int SEED = 13;
    try {
      // successful case
      assertEquals(0, sg.run(args));
      BufferedReader in = new BufferedReader(new FileReader(DIR_STRUCTURE_FILE));
      assertEquals(DIR_STRUCTURE_FIRST_LINE, in.readLine());
      assertEquals(DIR_STRUCTURE_SECOND_LINE, in.readLine());
      assertEquals(null, in.readLine());
      in.close();
      
      in = new BufferedReader(new FileReader(FILE_STRUCTURE_FILE));
      assertEquals(FILE_STRUCTURE_FIRST_LINE, in.readLine());
      assertEquals(FILE_STRUCTURE_SECOND_LINE, in.readLine());
      assertEquals(null, in.readLine());
      in.close();

      String oldArg = args[MAX_DEPTH];
      args[MAX_DEPTH] = "0";
      assertEquals(-1, sg.run(args));
      args[MAX_DEPTH] = oldArg;
      
      oldArg = args[MIN_WIDTH];
      args[MIN_WIDTH] = "-1";
      assertEquals(-1, sg.run(args));
      args[MIN_WIDTH] = oldArg;
      
      oldArg = args[MAX_WIDTH];
      args[MAX_WIDTH] = "-1";
      assertEquals(-1, sg.run(args));
      args[MAX_WIDTH] = oldArg;
      
      oldArg = args[NUM_OF_FILES];
      args[NUM_OF_FILES] = "-1";
      assertEquals(-1, sg.run(args));
      args[NUM_OF_FILES] = oldArg;
      
      oldArg = args[NUM_OF_FILES];
      args[NUM_OF_FILES] = "-1";
      assertEquals(-1, sg.run(args));
      args[NUM_OF_FILES] = oldArg;
      
      oldArg = args[AVG_FILE_SIZE];
      args[AVG_FILE_SIZE] = "-1";
      assertEquals(-1, sg.run(args));
      args[AVG_FILE_SIZE] = oldArg;
      
      oldArg = args[SEED];
      args[SEED] = "34.d4";
      assertEquals(-1, sg.run(args));
      args[SEED] = oldArg;
    } finally {
      DIR_STRUCTURE_FILE.delete();
      FILE_STRUCTURE_FILE.delete();
    }
  }

  /** Test if the load generator works fine */
  @Test
  public void testLoadGenerator() throws Exception {
    final String TEST_SPACE_ROOT = "/test";

    FileWriter writer = new FileWriter(DIR_STRUCTURE_FILE);
    writer.write(DIR_STRUCTURE_FIRST_LINE+"\n");
    writer.write(DIR_STRUCTURE_SECOND_LINE+"\n");
    writer.close();
    
    writer = new FileWriter(FILE_STRUCTURE_FILE);
    writer.write(FILE_STRUCTURE_FIRST_LINE+"\n");
    writer.write(FILE_STRUCTURE_SECOND_LINE+"\n");
    writer.close();
    
    MiniDFSCluster cluster = new MiniDFSCluster(CONF, 3, true, null);
    cluster.waitActive();
    
    try {
      DataGenerator dg = new DataGenerator();
      dg.setConf(CONF);
      String [] args = new String[] {"-inDir", OUT_DIR, "-root", TEST_SPACE_ROOT};
      assertEquals(0, dg.run(args));

      final int READ_PROBABILITY = 1;
      final int WRITE_PROBABILITY = 3;
      final int MAX_DELAY_BETWEEN_OPS = 7;
      final int NUM_OF_THREADS = 9;
      final int START_TIME = 11;
      final int ELAPSED_TIME = 13;
      
      LoadGenerator lg = new LoadGenerator();
      lg.setConf(CONF);
      args = new String[] {"-readProbability", "0.3", "-writeProbability", "0.3",
          "-root", TEST_SPACE_ROOT, "-maxDelayBetweenOps", "0",
          "-numOfThreads", "1", "-startTime", 
          Long.toString(System.currentTimeMillis()), "-elapsedTime", "10"};
      
      assertEquals(0, lg.run(args));

      String oldArg = args[READ_PROBABILITY];
      args[READ_PROBABILITY] = "1.1";
      assertEquals(-1, lg.run(args));
      args[READ_PROBABILITY] = "-1.1";
      assertEquals(-1, lg.run(args));
      args[READ_PROBABILITY] = oldArg;

      oldArg = args[WRITE_PROBABILITY];
      args[WRITE_PROBABILITY] = "1.1";
      assertEquals(-1, lg.run(args));
      args[WRITE_PROBABILITY] = "-1.1";
      assertEquals(-1, lg.run(args));
      args[WRITE_PROBABILITY] = "0.9";
      assertEquals(-1, lg.run(args));
      args[READ_PROBABILITY] = oldArg;

      oldArg = args[MAX_DELAY_BETWEEN_OPS];
      args[MAX_DELAY_BETWEEN_OPS] = "1.x1";
      assertEquals(-1, lg.run(args));
      args[MAX_DELAY_BETWEEN_OPS] = oldArg;
      
      oldArg = args[MAX_DELAY_BETWEEN_OPS];
      args[MAX_DELAY_BETWEEN_OPS] = "1.x1";
      assertEquals(-1, lg.run(args));
      args[MAX_DELAY_BETWEEN_OPS] = oldArg;
      
      oldArg = args[NUM_OF_THREADS];
      args[NUM_OF_THREADS] = "-1";
      assertEquals(-1, lg.run(args));
      args[NUM_OF_THREADS] = oldArg;
      
      oldArg = args[START_TIME];
      args[START_TIME] = "-1";
      assertEquals(-1, lg.run(args));
      args[START_TIME] = oldArg;

      oldArg = args[ELAPSED_TIME];
      args[ELAPSED_TIME] = "-1";
      assertEquals(-1, lg.run(args));
      args[ELAPSED_TIME] = oldArg;
    } finally {
      cluster.shutdown();
      DIR_STRUCTURE_FILE.delete();
      FILE_STRUCTURE_FILE.delete();
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new TestLoadGenerator(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    TestLoadGenerator loadGeneratorTest = new TestLoadGenerator();
    loadGeneratorTest.testStructureGenerator();
    loadGeneratorTest.testLoadGenerator();
    return 0;
  }
}
