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

package org.apache.hadoop.tools.rumen;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestRumenJobTraces {
  @Test
  public void testSmallTrace() throws Exception {
    performSingleTest("sample-job-tracker-logs.gz",
        "job-tracker-logs-topology-output", "job-tracker-logs-trace-output.gz");
  }

  @Test
  public void testTruncatedTask() throws Exception {
    performSingleTest("truncated-job-tracker-log", "truncated-topology-output",
        "truncated-trace-output");
  }

  private void performSingleTest(String jtLogName, String goldTopology,
      String goldTrace) throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", ""))
            .makeQualified(lfs);
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp"))
            .makeQualified(lfs);

    final Path rootInputFile = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    final Path topologyFile = new Path(tempDir, jtLogName + "-topology.json");
    final Path traceFile = new Path(tempDir, jtLogName + "-trace.json");

    final Path inputFile = new Path(rootInputFile, jtLogName);

    System.out.println("topology result file = " + topologyFile);
    System.out.println("trace result file = " + traceFile);

    String[] args = new String[6];

    args[0] = "-v1";

    args[1] = "-write-topology";
    args[2] = topologyFile.toString();

    args[3] = "-write-job-trace";
    args[4] = traceFile.toString();

    args[5] = inputFile.toString();

    final Path topologyGoldFile = new Path(rootInputFile, goldTopology);
    final Path traceGoldFile = new Path(rootInputFile, goldTrace);

    HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces
        .<LoggedNetworkTopology> jsonFileMatchesGold(lfs, topologyFile,
            topologyGoldFile, LoggedNetworkTopology.class, "topology");
    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(lfs, traceFile,
        traceGoldFile, LoggedJob.class, "trace");
  }

  static private <T extends DeepCompare> void jsonFileMatchesGold(
      FileSystem lfs, Path result, Path gold, Class<? extends T> clazz,
      String fileDescription) throws IOException {
    JsonObjectMapperParser<T> goldParser =
        new JsonObjectMapperParser<T>(gold, clazz, new Configuration());
    InputStream resultStream = lfs.open(result);
    JsonObjectMapperParser<T> resultParser =
        new JsonObjectMapperParser<T>(resultStream, clazz);
    try {
      while (true) {
        DeepCompare goldJob = goldParser.getNext();
        DeepCompare resultJob = resultParser.getNext();
        if ((goldJob == null) || (resultJob == null)) {
          assertTrue(goldJob == resultJob);
          break;
        }

        try {
          resultJob.deepCompare(goldJob, new TreePath(null, "<root>"));
        } catch (DeepInequalityException e) {
          String error = e.path.toString();

          assertFalse(fileDescription + " mismatches: " + error, true);
        }
      }
    } finally {
      IOUtils.cleanup(null, goldParser, resultParser);
    }
  }
}
