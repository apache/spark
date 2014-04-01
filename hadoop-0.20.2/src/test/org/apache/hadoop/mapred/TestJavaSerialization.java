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

public class TestJavaSerialization extends ClusterMapReduceTestCase {
  
  static class WordCountMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, String, Long> {

    public void map(LongWritable key, Text value,
        OutputCollector<String, Long> output, Reporter reporter)
        throws IOException {
      StringTokenizer st = new StringTokenizer(value.toString());
      while (st.hasMoreTokens()) {
        output.collect(st.nextToken(), 1L);
      }
    }

  }
  
  static class SumReducer<K> extends MapReduceBase implements
      Reducer<K, Long, K, Long> {
    
    public void reduce(K key, Iterator<Long> values,
        OutputCollector<K, Long> output, Reporter reporter)
      throws IOException {

      long sum = 0;
      while (values.hasNext()) {
        sum += values.next();
      }
      output.collect(key, sum);
    }
    
  }
  
  public void testMapReduceJob() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(),
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();

    JobConf conf = createJobConf();
    conf.setJobName("JavaSerialization");
    
    conf.set("io.serializations",
    "org.apache.hadoop.io.serializer.JavaSerialization," +
    "org.apache.hadoop.io.serializer.WritableSerialization");

    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(String.class);
    conf.setOutputValueClass(Long.class);
    conf.setOutputKeyComparatorClass(JavaSerializationComparator.class);

    conf.setMapperClass(WordCountMapper.class);
    conf.setReducerClass(SumReducer.class);

    FileInputFormat.setInputPaths(conf, getInputDir());

    FileOutputFormat.setOutputPath(conf, getOutputDir());

    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                           getFileSystem().listStatus(getOutputDir(),
                           new Utils.OutputFileUtils.OutputFilesFilter()));
    assertEquals(1, outputFiles.length);
    InputStream is = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    assertEquals("a\t1", reader.readLine());
    assertEquals("b\t1", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }

  /**
   * HADOOP-4466:
   * This test verifies the JavSerialization impl can write to SequenceFiles. by virtue other
   * SequenceFileOutputFormat is not coupled to Writable types, if so, the job will fail.
   *
   */
  public void testWriteToSequencefile() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(),
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();

    JobConf conf = createJobConf();
    conf.setJobName("JavaSerialization");

    conf.set("io.serializations",
    "org.apache.hadoop.io.serializer.JavaSerialization," +
    "org.apache.hadoop.io.serializer.WritableSerialization");

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class); // test we can write to sequence files

    conf.setOutputKeyClass(String.class);
    conf.setOutputValueClass(Long.class);
    conf.setOutputKeyComparatorClass(JavaSerializationComparator.class);

    conf.setMapperClass(WordCountMapper.class);
    conf.setReducerClass(SumReducer.class);

    FileInputFormat.setInputPaths(conf, getInputDir());

    FileOutputFormat.setOutputPath(conf, getOutputDir());

    JobClient.runJob(conf);

    Path[] outputFiles = FileUtil.stat2Paths(
                           getFileSystem().listStatus(getOutputDir(),
                           new Utils.OutputFileUtils.OutputFilesFilter()));
    assertEquals(1, outputFiles.length);
}

}
