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
package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.Properties;

public class TestClusterMapReduceTestCase extends ClusterMapReduceTestCase {
  public void _testMapReduce(boolean restart) throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello1\n");
    wr.write("hello2\n");
    wr.write("hello3\n");
    wr.write("hello4\n");
    wr.close();

    if (restart) {
      stopCluster();
      startCluster(false, null);
    }
    
    JobConf conf = createJobConf();
    conf.setJobName("mr");

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
    conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

    FileInputFormat.setInputPaths(conf, getInputDir());

    FileOutputFormat.setOutputPath(conf, getOutputDir());


    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                    getFileSystem().listStatus(getOutputDir(),
                    new Utils.OutputFileUtils.OutputFilesFilter()));
    if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      int counter = 0;
      while (line != null) {
        counter++;
        assertTrue(line.contains("hello"));
        line = reader.readLine();
      }
      reader.close();
      assertEquals(4, counter);
    }

  }

  public void testMapReduce() throws Exception {
    _testMapReduce(false);
  }

  public void testMapReduceRestarting() throws Exception {
    _testMapReduce(true);
  }

  public void testDFSRestart() throws Exception {
    Path file = new Path(getInputDir(), "text.txt");
    OutputStream os = getFileSystem().create(file);
    Writer wr = new OutputStreamWriter(os);
    wr.close();

    stopCluster();
    startCluster(false, null);
    assertTrue(getFileSystem().exists(file));

    stopCluster();
    startCluster(true, null);
    assertFalse(getFileSystem().exists(file));
    
  }

  public void testMRConfig() throws Exception {
    JobConf conf = createJobConf();
    assertNull(conf.get("xyz"));

    Properties config = new Properties();
    config.setProperty("xyz", "XYZ");
    stopCluster();
    startCluster(false, config);

    conf = createJobConf();
    assertEquals("XYZ", conf.get("xyz"));
  }

}
