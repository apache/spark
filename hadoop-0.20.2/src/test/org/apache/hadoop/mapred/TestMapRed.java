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
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**********************************************************
 * MapredLoadTest generates a bunch of work that exercises
 * a Hadoop Map-Reduce system (and DFS, too).  It goes through
 * the following steps:
 *
 * 1) Take inputs 'range' and 'counts'.
 * 2) Generate 'counts' random integers between 0 and range-1.
 * 3) Create a file that lists each integer between 0 and range-1,
 *    and lists the number of times that integer was generated.
 * 4) Emit a (very large) file that contains all the integers
 *    in the order generated.
 * 5) After the file has been generated, read it back and count
 *    how many times each int was generated.
 * 6) Compare this big count-map against the original one.  If
 *    they match, then SUCCESS!  Otherwise, FAILURE!
 *
 * OK, that's how we can think about it.  What are the map-reduce
 * steps that get the job done?
 *
 * 1) In a non-mapred thread, take the inputs 'range' and 'counts'.
 * 2) In a non-mapread thread, generate the answer-key and write to disk.
 * 3) In a mapred job, divide the answer key into K jobs.
 * 4) A mapred 'generator' task consists of K map jobs.  Each reads
 *    an individual "sub-key", and generates integers according to
 *    to it (though with a random ordering).
 * 5) The generator's reduce task agglomerates all of those files
 *    into a single one.
 * 6) A mapred 'reader' task consists of M map jobs.  The output
 *    file is cut into M pieces. Each of the M jobs counts the 
 *    individual ints in its chunk and creates a map of all seen ints.
 * 7) A mapred job integrates all the count files into a single one.
 *
 **********************************************************/
public class TestMapRed extends TestCase implements Tool {
  /**
   * Modified to make it a junit test.
   * The RandomGen Job does the actual work of creating
   * a huge file of assorted numbers.  It receives instructions
   * as to how many times each number should be counted.  Then
   * it emits those numbers in a crazy order.
   *
   * The map() function takes a key/val pair that describes
   * a value-to-be-emitted (the key) and how many times it 
   * should be emitted (the value), aka "numtimes".  map() then
   * emits a series of intermediate key/val pairs.  It emits
   * 'numtimes' of these.  The key is a random number and the
   * value is the 'value-to-be-emitted'.
   *
   * The system collates and merges these pairs according to
   * the random number.  reduce() function takes in a key/value
   * pair that consists of a crazy random number and a series
   * of values that should be emitted.  The random number key
   * is now dropped, and reduce() emits a pair for every intermediate value.
   * The emitted key is an intermediate value.  The emitted value
   * is just a blank string.  Thus, we've created a huge file
   * of numbers in random order, but where each number appears
   * as many times as we were instructed.
   */
  static class RandomGenMapper
    implements Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void configure(JobConf job) {
    }

    public void map(IntWritable key, IntWritable val,
                    OutputCollector<IntWritable, IntWritable> out,
                    Reporter reporter) throws IOException {
      int randomVal = key.get();
      int randomCount = val.get();

      for (int i = 0; i < randomCount; i++) {
        out.collect(new IntWritable(Math.abs(r.nextInt())), new IntWritable(randomVal));
      }
    }
    public void close() {
    }
  }
  /**
   */
  static class RandomGenReducer
    implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void configure(JobConf job) {
    }

    public void reduce(IntWritable key, Iterator<IntWritable> it,
                       OutputCollector<IntWritable, IntWritable> out,
                       Reporter reporter) throws IOException {
      while (it.hasNext()) {
        out.collect(it.next(), null);
      }
    }
    public void close() {
    }
  }

  /**
   * The RandomCheck Job does a lot of our work.  It takes
   * in a num/string keyspace, and transforms it into a
   * key/count(int) keyspace.
   *
   * The map() function just emits a num/1 pair for every
   * num/string input pair.
   *
   * The reduce() function sums up all the 1s that were
   * emitted for a single key.  It then emits the key/total
   * pair.
   *
   * This is used to regenerate the random number "answer key".
   * Each key here is a random number, and the count is the
   * number of times the number was emitted.
   */
  static class RandomCheckMapper
    implements Mapper<WritableComparable, Text, IntWritable, IntWritable> {
    
    public void configure(JobConf job) {
    }

    public void map(WritableComparable key, Text val,
                    OutputCollector<IntWritable, IntWritable> out,
                    Reporter reporter) throws IOException {
      out.collect(new IntWritable(Integer.parseInt(val.toString().trim())), new IntWritable(1));
    }
    public void close() {
    }
  }
  /**
   */
  static class RandomCheckReducer
      implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void configure(JobConf job) {
    }
        
    public void reduce(IntWritable key, Iterator<IntWritable> it,
                       OutputCollector<IntWritable, IntWritable> out,
                       Reporter reporter) throws IOException {
      int keyint = key.get();
      int count = 0;
      while (it.hasNext()) {
        it.next();
        count++;
      }
      out.collect(new IntWritable(keyint), new IntWritable(count));
    }
    public void close() {
    }
  }

  /**
   * The Merge Job is a really simple one.  It takes in
   * an int/int key-value set, and emits the same set.
   * But it merges identical keys by adding their values.
   *
   * Thus, the map() function is just the identity function
   * and reduce() just sums.  Nothing to see here!
   */
  static class MergeMapper
    implements Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void configure(JobConf job) {
    }

    public void map(IntWritable key, IntWritable val,
                    OutputCollector<IntWritable, IntWritable> out,
                    Reporter reporter) throws IOException {
      int keyint = key.get();
      int valint = val.get();

      out.collect(new IntWritable(keyint), new IntWritable(valint));
    }
    public void close() {
    }
  }
  static class MergeReducer
    implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void configure(JobConf job) {
    }
        
    public void reduce(IntWritable key, Iterator<IntWritable> it,
                       OutputCollector<IntWritable, IntWritable> out,
                       Reporter reporter) throws IOException {
      int keyint = key.get();
      int total = 0;
      while (it.hasNext()) {
        total += it.next().get();
      }
      out.collect(new IntWritable(keyint), new IntWritable(total));
    }
    public void close() {
    }
  }

  private static int range = 10;
  private static int counts = 100;
  private static Random r = new Random();

  /**
     public TestMapRed(int range, int counts, Configuration conf) throws IOException {
     this.range = range;
     this.counts = counts;
     this.conf = conf;
     }
  **/

  
  public void testMapred() throws Exception {
    launch();
  }

  private static class MyMap
    implements Mapper<WritableComparable, Text, Text, Text> {
      
    public void configure(JobConf conf) {
    }
      
    public void map(WritableComparable key, Text value,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
      String str = value.toString().toLowerCase();
      output.collect(new Text(str), value);
    }

    public void close() throws IOException {
    }
  }
    
  private static class MyReduce extends IdentityReducer {
    private JobConf conf;
    private boolean compressInput;
    private boolean first = true;
      
    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
      compressInput = conf.getCompressMapOutput();
    }
      
    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter
                       ) throws IOException {
      if (first) {
        first = false;
        MapOutputFile mapOutputFile = new MapOutputFile();
        mapOutputFile.setConf(conf);
        Path input = mapOutputFile.getInputFile(0);
        FileSystem fs = FileSystem.get(conf);
        assertTrue("reduce input exists " + input, fs.exists(input));
        SequenceFile.Reader rdr = 
          new SequenceFile.Reader(fs, input, conf);
        assertEquals("is reduce input compressed " + input, 
                     compressInput, 
                     rdr.isCompressed());
        rdr.close();          
      }
    }
      
  }

  private static class BadPartitioner
      implements Partitioner<LongWritable,Text> {
    boolean low;
    public void configure(JobConf conf) {
      low = conf.getBoolean("test.testmapred.badpartition", true);
    }
    public int getPartition(LongWritable k, Text v, int numPartitions) {
      return low ? -1 : numPartitions;
    }
  }

  
  public void testPartitioner() throws Exception {
    JobConf conf = new JobConf(TestMapRed.class);
    conf.setPartitionerClass(BadPartitioner.class);
    FileSystem fs = FileSystem.getLocal(conf);
    Path testdir = new Path(
        System.getProperty("test.build.data","/tmp")).makeQualified(fs);
    Path inFile = new Path(testdir, "blah/blah");
    DataOutputStream f = fs.create(inFile);
    f.writeBytes("blah blah blah\n");
    f.close();
    FileInputFormat.setInputPaths(conf, inFile);
    FileOutputFormat.setOutputPath(conf, new Path(testdir, "out"));
    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    // partition too low
    conf.setBoolean("test.testmapred.badpartition", true);
    boolean pass = true;
    try {
      JobClient.runJob(conf);
    } catch (IOException e) {
      pass = false;
    }
    assertFalse("should fail for partition < 0", pass);

    // partition too high
    conf.setBoolean("test.testmapred.badpartition", false);
    pass = true;
    try {
      JobClient.runJob(conf);
    } catch (IOException e) {
      pass = false;
    }
    assertFalse("should fail for partition >= numPartitions", pass);
  }

  public static class NullMapper
      implements Mapper<NullWritable,Text,NullWritable,Text> {
    public void map(NullWritable key, Text val,
        OutputCollector<NullWritable,Text> output, Reporter reporter)
        throws IOException {
      output.collect(NullWritable.get(), val);
    }
    public void configure(JobConf conf) { }
    public void close() { }
  }

  
  public void testNullKeys() throws Exception {
    JobConf conf = new JobConf(TestMapRed.class);
    FileSystem fs = FileSystem.getLocal(conf);
    Path testdir = new Path(
        System.getProperty("test.build.data","/tmp")).makeQualified(fs);
    fs.delete(testdir, true);
    Path inFile = new Path(testdir, "nullin/blah");
    SequenceFile.Writer w = SequenceFile.createWriter(fs, conf, inFile,
        NullWritable.class, Text.class, SequenceFile.CompressionType.NONE);
    Text t = new Text();
    t.set("AAAAAAAAAAAAAA"); w.append(NullWritable.get(), t);
    t.set("BBBBBBBBBBBBBB"); w.append(NullWritable.get(), t);
    t.set("CCCCCCCCCCCCCC"); w.append(NullWritable.get(), t);
    t.set("DDDDDDDDDDDDDD"); w.append(NullWritable.get(), t);
    t.set("EEEEEEEEEEEEEE"); w.append(NullWritable.get(), t);
    t.set("FFFFFFFFFFFFFF"); w.append(NullWritable.get(), t);
    t.set("GGGGGGGGGGGGGG"); w.append(NullWritable.get(), t);
    t.set("HHHHHHHHHHHHHH"); w.append(NullWritable.get(), t);
    w.close();
    FileInputFormat.setInputPaths(conf, inFile);
    FileOutputFormat.setOutputPath(conf, new Path(testdir, "nullout"));
    conf.setMapperClass(NullMapper.class);
    conf.setReducerClass(IdentityReducer.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setNumReduceTasks(1);

    JobClient.runJob(conf);

    SequenceFile.Reader r = new SequenceFile.Reader(fs,
        new Path(testdir, "nullout/part-00000"), conf);
    String m = "AAAAAAAAAAAAAA";
    for (int i = 1; r.next(NullWritable.get(), t); ++i) {
      assertTrue(t.toString() + " doesn't match " + m, m.equals(t.toString()));
      m = m.replace((char)('A' + i - 1), (char)('A' + i));
    }
  }

  private void checkCompression(boolean compressMapOutputs,
                                CompressionType redCompression,
                                boolean includeCombine
                                ) throws Exception {
    JobConf conf = new JobConf(TestMapRed.class);
    Path testdir = new Path("build/test/test.mapred.compress");
    Path inDir = new Path(testdir, "in");
    Path outDir = new Path(testdir, "out");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(testdir, true);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setMapperClass(MyMap.class);
    conf.setReducerClass(MyReduce.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    if (includeCombine) {
      conf.setCombinerClass(IdentityReducer.class);
    }
    conf.setCompressMapOutput(compressMapOutputs);
    SequenceFileOutputFormat.setOutputCompressionType(conf, redCompression);
    try {
      if (!fs.mkdirs(testdir)) {
        throw new IOException("Mkdirs failed to create " + testdir.toString());
      }
      if (!fs.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      Path inFile = new Path(inDir, "part0");
      DataOutputStream f = fs.create(inFile);
      f.writeBytes("Owen was here\n");
      f.writeBytes("Hadoop is fun\n");
      f.writeBytes("Is this done, yet?\n");
      f.close();
      RunningJob rj = JobClient.runJob(conf);
      assertTrue("job was complete", rj.isComplete());
      assertTrue("job was successful", rj.isSuccessful());
      Path output = new Path(outDir,
                             Task.getOutputName(0));
      assertTrue("reduce output exists " + output, fs.exists(output));
      SequenceFile.Reader rdr = 
        new SequenceFile.Reader(fs, output, conf);
      assertEquals("is reduce output compressed " + output, 
                   redCompression != CompressionType.NONE, 
                   rdr.isCompressed());
      rdr.close();
    } finally {
      fs.delete(testdir, true);
    }
  }
  
   
  public void testCompression() throws Exception {
    EnumSet<SequenceFile.CompressionType> seq =
      EnumSet.allOf(SequenceFile.CompressionType.class);
    for (CompressionType redCompression : seq) {
      for(int combine=0; combine < 2; ++combine) {
        checkCompression(false, redCompression, combine == 1);
        checkCompression(true, redCompression, combine == 1);
      }
    }
  }
    
    
  /**
   * 
   */
  public void launch() throws Exception {
    //
    // Generate distribution of ints.  This is the answer key.
    //
    JobConf conf = null;
    //Check to get configuration and check if it is configured thro' Configured
    //interface. This would happen when running testcase thro' command line.
    if(getConf() == null) {
      conf = new JobConf();
    } else {
      conf = new JobConf(getConf());
    }
    conf.setJarByClass(TestMapRed.class);
    int countsToGo = counts;
    int dist[] = new int[range];
    for (int i = 0; i < range; i++) {
      double avgInts = (1.0 * countsToGo) / (range - i);
      dist[i] = (int) Math.max(0, Math.round(avgInts + (Math.sqrt(avgInts) * r.nextGaussian())));
      countsToGo -= dist[i];
    }
    if (countsToGo > 0) {
      dist[dist.length-1] += countsToGo;
    }

    //
    // Write the answer key to a file.  
    //
    FileSystem fs = FileSystem.get(conf);
    Path testdir = new Path("mapred.loadtest");
    if (!fs.mkdirs(testdir)) {
      throw new IOException("Mkdirs failed to create " + testdir.toString());
    }

    Path randomIns = new Path(testdir, "genins");
    if (!fs.mkdirs(randomIns)) {
      throw new IOException("Mkdirs failed to create " + randomIns.toString());
    }

    Path answerkey = new Path(randomIns, "answer.key");
    SequenceFile.Writer out = 
      SequenceFile.createWriter(fs, conf, answerkey, IntWritable.class,
                                IntWritable.class, 
                                SequenceFile.CompressionType.NONE);
    try {
      for (int i = 0; i < range; i++) {
        out.append(new IntWritable(i), new IntWritable(dist[i]));
      }
    } finally {
      out.close();
    }
    //printFiles(randomIns, conf);

    //
    // Now we need to generate the random numbers according to
    // the above distribution.
    //
    // We create a lot of map tasks, each of which takes at least
    // one "line" of the distribution.  (That is, a certain number
    // X is to be generated Y number of times.)
    //
    // A map task emits Y key/val pairs.  The val is X.  The key
    // is a randomly-generated number.
    //
    // The reduce task gets its input sorted by key.  That is, sorted
    // in random order.  It then emits a single line of text that
    // for the given values.  It does not emit the key.
    //
    // Because there's just one reduce task, we emit a single big
    // file of random numbers.
    //
    Path randomOuts = new Path(testdir, "genouts");
    fs.delete(randomOuts, true);


    JobConf genJob = new JobConf(conf, TestMapRed.class);
    FileInputFormat.setInputPaths(genJob, randomIns);
    genJob.setInputFormat(SequenceFileInputFormat.class);
    genJob.setMapperClass(RandomGenMapper.class);

    FileOutputFormat.setOutputPath(genJob, randomOuts);
    genJob.setOutputKeyClass(IntWritable.class);
    genJob.setOutputValueClass(IntWritable.class);
    genJob.setOutputFormat(TextOutputFormat.class);
    genJob.setReducerClass(RandomGenReducer.class);
    genJob.setNumReduceTasks(1);

    JobClient.runJob(genJob);
    //printFiles(randomOuts, conf);

    //
    // Next, we read the big file in and regenerate the 
    // original map.  It's split into a number of parts.
    // (That number is 'intermediateReduces'.)
    //
    // We have many map tasks, each of which read at least one
    // of the output numbers.  For each number read in, the
    // map task emits a key/value pair where the key is the
    // number and the value is "1".
    //
    // We have a single reduce task, which receives its input
    // sorted by the key emitted above.  For each key, there will
    // be a certain number of "1" values.  The reduce task sums
    // these values to compute how many times the given key was
    // emitted.
    //
    // The reduce task then emits a key/val pair where the key
    // is the number in question, and the value is the number of
    // times the key was emitted.  This is the same format as the
    // original answer key (except that numbers emitted zero times
    // will not appear in the regenerated key.)  The answer set
    // is split into a number of pieces.  A final MapReduce job
    // will merge them.
    //
    // There's not really a need to go to 10 reduces here 
    // instead of 1.  But we want to test what happens when
    // you have multiple reduces at once.
    //
    int intermediateReduces = 10;
    Path intermediateOuts = new Path(testdir, "intermediateouts");
    fs.delete(intermediateOuts, true);
    JobConf checkJob = new JobConf(conf, TestMapRed.class);
    FileInputFormat.setInputPaths(checkJob, randomOuts);
    checkJob.setInputFormat(TextInputFormat.class);
    checkJob.setMapperClass(RandomCheckMapper.class);

    FileOutputFormat.setOutputPath(checkJob, intermediateOuts);
    checkJob.setOutputKeyClass(IntWritable.class);
    checkJob.setOutputValueClass(IntWritable.class);
    checkJob.setOutputFormat(MapFileOutputFormat.class);
    checkJob.setReducerClass(RandomCheckReducer.class);
    checkJob.setNumReduceTasks(intermediateReduces);

    JobClient.runJob(checkJob);
    //printFiles(intermediateOuts, conf); 

    //
    // OK, now we take the output from the last job and
    // merge it down to a single file.  The map() and reduce()
    // functions don't really do anything except reemit tuples.
    // But by having a single reduce task here, we end up merging
    // all the files.
    //
    Path finalOuts = new Path(testdir, "finalouts");
    fs.delete(finalOuts, true);
    JobConf mergeJob = new JobConf(conf, TestMapRed.class);
    FileInputFormat.setInputPaths(mergeJob, intermediateOuts);
    mergeJob.setInputFormat(SequenceFileInputFormat.class);
    mergeJob.setMapperClass(MergeMapper.class);
        
    FileOutputFormat.setOutputPath(mergeJob, finalOuts);
    mergeJob.setOutputKeyClass(IntWritable.class);
    mergeJob.setOutputValueClass(IntWritable.class);
    mergeJob.setOutputFormat(SequenceFileOutputFormat.class);
    mergeJob.setReducerClass(MergeReducer.class);
    mergeJob.setNumReduceTasks(1);
        
    JobClient.runJob(mergeJob);
    //printFiles(finalOuts, conf); 
 
    //
    // Finally, we compare the reconstructed answer key with the
    // original one.  Remember, we need to ignore zero-count items
    // in the original key.
    //
    boolean success = true;
    Path recomputedkey = new Path(finalOuts, "part-00000");
    SequenceFile.Reader in = new SequenceFile.Reader(fs, recomputedkey, conf);
    int totalseen = 0;
    try {
      IntWritable key = new IntWritable();
      IntWritable val = new IntWritable();            
      for (int i = 0; i < range; i++) {
        if (dist[i] == 0) {
          continue;
        }
        if (!in.next(key, val)) {
          System.err.println("Cannot read entry " + i);
          success = false;
          break;
        } else {
          if (!((key.get() == i) && (val.get() == dist[i]))) {
            System.err.println("Mismatch!  Pos=" + key.get() + ", i=" + i + 
                               ", val=" + val.get() + ", dist[i]=" + dist[i]);
            success = false;
          }
          totalseen += val.get();
        }
      }
      if (success) {
        if (in.next(key, val)) {
          System.err.println("Unnecessary lines in recomputed key!");
          success = false;
        }
      }
    } finally {
      in.close();
    }
    int originalTotal = 0;
    for (int i = 0; i < dist.length; i++) {
      originalTotal += dist[i];
    }
    System.out.println("Original sum: " + originalTotal);
    System.out.println("Recomputed sum: " + totalseen);

    //
    // Write to "results" whether the test succeeded or not.
    //
    Path resultFile = new Path(testdir, "results");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(resultFile)));
    try {
      bw.write("Success=" + success + "\n");
      System.out.println("Success=" + success);
    } finally {
      bw.close();
    }
    assertTrue("testMapRed failed", success);
    fs.delete(testdir, true);
  }

  private static void printTextFile(FileSystem fs, Path p) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p)));
    String line;
    while ((line = in.readLine()) != null) {
      System.out.println("  Row: " + line);
    }
    in.close();
  }

  private static void printSequenceFile(FileSystem fs, Path p, 
                                        Configuration conf) throws IOException {
    SequenceFile.Reader r = new SequenceFile.Reader(fs, p, conf);
    Object key = null;
    Object value = null;
    while ((key = r.next(key)) != null) {
      value = r.getCurrentValue(value);
      System.out.println("  Row: " + key + ", " + value);
    }
    r.close();    
  }

  private static boolean isSequenceFile(FileSystem fs,
                                        Path f) throws IOException {
    DataInputStream in = fs.open(f);
    byte[] seq = "SEQ".getBytes();
    for(int i=0; i < seq.length; ++i) {
      if (seq[i] != in.read()) {
        return false;
      }
    }
    return true;
  }

  private static void printFiles(Path dir, 
                                 Configuration conf) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    for(FileStatus f: fs.listStatus(dir)) {
      System.out.println("Reading " + f.getPath() + ": ");
      if (f.isDir()) {
        System.out.println("  it is a map file.");
        printSequenceFile(fs, new Path(f.getPath(), "data"), conf);
      } else if (isSequenceFile(fs, f.getPath())) {
        System.out.println("  it is a sequence file.");
        printSequenceFile(fs, f.getPath(), conf);
      } else {
        System.out.println("  it is a text file.");
        printTextFile(fs, f.getPath());
      }
    }
  }

  /**
   * Launches all the tasks in order.
   */
  public static void main(String[] argv) throws Exception {
    int res = ToolRunner.run(new TestMapRed(), argv);
    System.exit(res);
  }
    
  public void testSmallInput(){
    runJob(100);
  }
  
  
  public void testBiggerInput(){
    runJob(1000);
  }

  public void runJob(int items) {
    try {
      JobConf conf = new JobConf(TestMapRed.class);
      Path testdir = new Path("build/test/test.mapred.spill");
      Path inDir = new Path(testdir, "in");
      Path outDir = new Path(testdir, "out");
      FileSystem fs = FileSystem.get(conf);
      fs.delete(testdir, true);
      conf.setInt("io.sort.mb", 1);
      conf.setInputFormat(SequenceFileInputFormat.class);
      FileInputFormat.setInputPaths(conf, inDir);
      FileOutputFormat.setOutputPath(conf, outDir);
      conf.setMapperClass(IdentityMapper.class);
      conf.setReducerClass(IdentityReducer.class);
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
      conf.setOutputFormat(SequenceFileOutputFormat.class);
      if (!fs.mkdirs(testdir)) {
        throw new IOException("Mkdirs failed to create " + testdir.toString());
      }
      if (!fs.mkdirs(inDir)) {
        throw new IOException("Mkdirs failed to create " + inDir.toString());
      }
      Path inFile = new Path(inDir, "part0");
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, inFile,
                                                             Text.class, Text.class);

      StringBuffer content = new StringBuffer();

      for (int i = 0; i < 1000; i++) {
        content.append(i).append(": This is one more line of content\n");
      }

      Text text = new Text(content.toString());

      for (int i = 0; i < items; i++) {
        writer.append(new Text("rec:" + i), text);
      }
      writer.close();

      JobClient.runJob(conf);
    } catch (Exception e) {
      assertTrue("Threw exception:" + e,false);
    }
  }

  @Override
  public int run(String[] argv) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: TestMapRed <range> <counts>");
      System.err.println();
      System.err.println("Note: a good test will have a " +
      		"<counts> value that is substantially larger than the <range>");
      return -1;
    }

    int i = 0;
    range = Integer.parseInt(argv[i++]);
    counts = Integer.parseInt(argv[i++]);
    launch();
    return 0;
  }

  Configuration myConf = null;
  
  @Override
  public Configuration getConf() {
    return myConf;
  }

  @Override
  public void setConf(Configuration conf) {
    myConf = conf;
  }
}
