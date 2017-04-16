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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapred.lib.IdentityMapper;

public class TestJobName extends ClusterMapReduceTestCase {

  public void testComplexName() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(),
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();

    JobConf conf = createJobConf();
    conf.setJobName("[name][some other value that gets truncated internally that this test attempts to aggravate]");

    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(IdentityMapper.class);

    FileInputFormat.setInputPaths(conf, getInputDir());

    FileOutputFormat.setOutputPath(conf, getOutputDir());

    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                           getFileSystem().listStatus(getOutputDir(),
                           new Utils.OutputFileUtils.OutputFilesFilter()));

    assertEquals(1, outputFiles.length);
    InputStream is = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("0\tb a", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }

  public void testComplexNameWithRegex() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(),
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();

    JobConf conf = createJobConf();
    conf.setJobName("name \\Evalue]");

    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(IdentityMapper.class);

    FileInputFormat.setInputPaths(conf, getInputDir());

    FileOutputFormat.setOutputPath(conf, getOutputDir());

    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                           getFileSystem().listStatus(getOutputDir(),
                           new Utils.OutputFileUtils.OutputFilesFilter()));

    assertEquals(1, outputFiles.length);
    InputStream is = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("0\tb a", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }

}