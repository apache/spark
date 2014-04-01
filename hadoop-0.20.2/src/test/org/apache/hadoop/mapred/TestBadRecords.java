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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class TestBadRecords extends ClusterMapReduceTestCase {
  
  private static final Log LOG = 
    LogFactory.getLog(TestBadRecords.class);
  
  private static final List<String> MAPPER_BAD_RECORDS = 
    Arrays.asList("hello01","hello04","hello05");
  
  private static final List<String> REDUCER_BAD_RECORDS = 
    Arrays.asList("hello08","hello10");
  
  private List<String> input;
  
  public TestBadRecords() {
    input = new ArrayList<String>();
    for(int i=1;i<=10;i++) {
      String str = ""+i;
      int zerosToPrepend = 2 - str.length();
      for(int j=0;j<zerosToPrepend;j++){
        str = "0"+str;
      }
      input.add("hello"+str);
    }
  }
  
  private void runMapReduce(JobConf conf, 
      List<String> mapperBadRecords, List<String> redBadRecords) 
        throws Exception {
    createInput();
    conf.setJobName("mr");
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    conf.setInt("mapred.task.timeout", 30*1000);
    SkipBadRecords.setMapperMaxSkipRecords(conf, Long.MAX_VALUE);
    SkipBadRecords.setReducerMaxSkipGroups(conf, Long.MAX_VALUE);
    
    SkipBadRecords.setAttemptsToStartSkipping(conf,0);
    //the no of attempts to successfully complete the task depends 
    //on the no of bad records.
    conf.setMaxMapAttempts(SkipBadRecords.getAttemptsToStartSkipping(conf)+1+
        mapperBadRecords.size());
    conf.setMaxReduceAttempts(SkipBadRecords.getAttemptsToStartSkipping(conf)+
        1+redBadRecords.size());
    
    FileInputFormat.setInputPaths(conf, getInputDir());
    FileOutputFormat.setOutputPath(conf, getOutputDir());
    conf.setInputFormat(TextInputFormat.class);
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);
    RunningJob runningJob = JobClient.runJob(conf);
    validateOutput(conf, runningJob, mapperBadRecords, redBadRecords);
  }
  
  
  private void createInput() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), 
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    for(String inp : input) {
      wr.write(inp+"\n");
    }wr.close();
  }
  
  private void validateOutput(JobConf conf, RunningJob runningJob, 
      List<String> mapperBadRecords, List<String> redBadRecords) 
    throws Exception{
    LOG.info(runningJob.getCounters().toString());
    assertTrue(runningJob.isSuccessful());
    
    //validate counters
    Counters counters = runningJob.getCounters();
    assertEquals(counters.findCounter(Task.Counter.MAP_SKIPPED_RECORDS).
        getCounter(),mapperBadRecords.size());
    
    int mapRecs = input.size() - mapperBadRecords.size();
    assertEquals(counters.findCounter(Task.Counter.MAP_INPUT_RECORDS).
        getCounter(),mapRecs);
    assertEquals(counters.findCounter(Task.Counter.MAP_OUTPUT_RECORDS).
        getCounter(),mapRecs);
    
    int redRecs = mapRecs - redBadRecords.size();
    assertEquals(counters.findCounter(Task.Counter.REDUCE_SKIPPED_RECORDS).
        getCounter(),redBadRecords.size());
    assertEquals(counters.findCounter(Task.Counter.REDUCE_SKIPPED_GROUPS).
        getCounter(),redBadRecords.size());
    assertEquals(counters.findCounter(Task.Counter.REDUCE_INPUT_GROUPS).
        getCounter(),redRecs);
    assertEquals(counters.findCounter(Task.Counter.REDUCE_INPUT_RECORDS).
        getCounter(),redRecs);
    assertEquals(counters.findCounter(Task.Counter.REDUCE_OUTPUT_RECORDS).
        getCounter(),redRecs);
    
    //validate skipped records
    Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
    Path[] skips = FileUtil.stat2Paths(getFileSystem().listStatus(skipDir));
    List<String> mapSkipped = new ArrayList<String>();
    List<String> redSkipped = new ArrayList<String>();
    for(Path skipPath : skips) {
      LOG.info("skipPath: " + skipPath);
      
      SequenceFile.Reader reader = new SequenceFile.Reader(
          getFileSystem(), skipPath, conf);
      Object key = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Object value = ReflectionUtils.newInstance(reader.getValueClass(), 
          conf);
      key = reader.next(key);
      while(key!=null) {
        value = reader.getCurrentValue(value);
        LOG.debug("key:"+key+" value:"+value.toString());
        if(skipPath.getName().contains("_r_")) {
          redSkipped.add(value.toString());
        } else {
          mapSkipped.add(value.toString());
        }
        key = reader.next(key);
      }
      reader.close();
    }
    assertTrue(mapSkipped.containsAll(mapperBadRecords));
    assertTrue(redSkipped.containsAll(redBadRecords));
    
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(getOutputDir(),
        new Utils.OutputFileUtils.OutputFilesFilter()));
    
    List<String> mapperOutput=getProcessed(input, mapperBadRecords);
    LOG.debug("mapperOutput " + mapperOutput.size());
    List<String> reducerOutput=getProcessed(mapperOutput, redBadRecords);
    LOG.debug("reducerOutput " + reducerOutput.size());
    
   if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      int counter = 0;
      while (line != null) {
        counter++;
        StringTokenizer tokeniz = new StringTokenizer(line, "\t");
        String key = tokeniz.nextToken();
        String value = tokeniz.nextToken();
        LOG.debug("Output: key:"+key + "  value:"+value);
        assertTrue(value.contains("hello"));
        
        
        assertTrue(reducerOutput.contains(value));
        line = reader.readLine();
      }
      reader.close();
      assertEquals(reducerOutput.size(), counter);
    }
  }
  
  private List<String> getProcessed(List<String> inputs, List<String> badRecs) {
    List<String> processed = new ArrayList<String>();
    for(String input : inputs) {
      if(!badRecs.contains(input)) {
        processed.add(input);
      }
    }
    return processed;
  }
  
  public void testBadMapRed() throws Exception {
    JobConf conf = createJobConf();
    conf.setMapperClass(BadMapper.class);
    conf.setReducerClass(BadReducer.class);
    runMapReduce(conf, MAPPER_BAD_RECORDS, REDUCER_BAD_RECORDS);
  }
  
    
  static class BadMapper extends MapReduceBase implements 
    Mapper<LongWritable, Text, LongWritable, Text> {
    
    public void map(LongWritable key, Text val,
        OutputCollector<LongWritable, Text> output, Reporter reporter)
        throws IOException {
      String str = val.toString();
      LOG.debug("MAP key:" +key +"  value:" + str);
      if(MAPPER_BAD_RECORDS.get(0).equals(str)) {
        LOG.warn("MAP Encountered BAD record");
        System.exit(-1);
      }
      else if(MAPPER_BAD_RECORDS.get(1).equals(str)) {
        LOG.warn("MAP Encountered BAD record");
        throw new RuntimeException("Bad record "+str);
      }
      else if(MAPPER_BAD_RECORDS.get(2).equals(str)) {
        try {
          LOG.warn("MAP Encountered BAD record");
          Thread.sleep(15*60*1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      output.collect(key, val);
    }
  }
  
  static class BadReducer extends MapReduceBase implements 
    Reducer<LongWritable, Text, LongWritable, Text> {
    
    public void reduce(LongWritable key, Iterator<Text> values,
        OutputCollector<LongWritable, Text> output, Reporter reporter)
        throws IOException {
      while(values.hasNext()) {
        Text value = values.next();
        LOG.debug("REDUCE key:" +key +"  value:" + value);
        if(REDUCER_BAD_RECORDS.get(0).equals(value.toString())) {
          LOG.warn("REDUCE Encountered BAD record");
          System.exit(-1);
        }
        else if(REDUCER_BAD_RECORDS.get(1).equals(value.toString())) {
          try {
            LOG.warn("REDUCE Encountered BAD record");
            Thread.sleep(15*60*1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        output.collect(key, value);
      }
      
    }
  }
  

}
