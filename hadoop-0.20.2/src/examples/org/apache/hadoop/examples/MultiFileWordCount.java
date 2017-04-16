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

package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MultiFileWordCount is an example to demonstrate the usage of 
 * MultiFileInputFormat. This examples counts the occurrences of
 * words in the text files under the given input directory.
 */
public class MultiFileWordCount extends Configured implements Tool {

  /**
   * This record keeps &lt;filename,offset&gt; pairs.
   */
  public static class WordOffset implements WritableComparable {

    private long offset;
    private String fileName;

    public void readFields(DataInput in) throws IOException {
      this.offset = in.readLong();
      this.fileName = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(offset);
      Text.writeString(out, fileName);
    }

    public int compareTo(Object o) {
      WordOffset that = (WordOffset)o;

      int f = this.fileName.compareTo(that.fileName);
      if(f == 0) {
        return (int)Math.signum((double)(this.offset - that.offset));
      }
      return f;
    }
    @Override
    public boolean equals(Object obj) {
      if(obj instanceof WordOffset)
        return this.compareTo(obj) == 0;
      return false;
    }
    @Override
    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; //an arbitrary constant
    }
  }


  /**
   * To use {@link CombineFileInputFormat}, one should extend it, to return a 
   * (custom) {@link RecordReader}. CombineFileInputFormat uses 
   * {@link CombineFileSplit}s. 
   */
  public static class MyInputFormat 
    extends CombineFileInputFormat<WordOffset, Text>  {

    public RecordReader<WordOffset,Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException {
      return new CombineFileRecordReader<WordOffset, Text>(
        (CombineFileSplit)split, context, CombineFileLineRecordReader.class);
    }
  }

  /**
   * RecordReader is responsible from extracting records from a chunk
   * of the CombineFileSplit. 
   */
  public static class CombineFileLineRecordReader 
    extends RecordReader<WordOffset, Text> {

    private long startOffset; //offset of the chunk;
    private long end; //end of the chunk;
    private long pos; // current pos 
    private FileSystem fs;
    private Path path;
    private WordOffset key;
    private Text value;
    
    private FSDataInputStream fileIn;
    private LineReader reader;
    
    public CombineFileLineRecordReader(CombineFileSplit split,
        TaskAttemptContext context, Integer index) throws IOException {
      
      fs = FileSystem.get(context.getConfiguration());
      this.path = split.getPath(index);
      this.startOffset = split.getOffset(index);
      this.end = startOffset + split.getLength(index);
      boolean skipFirstLine = false;
      
      //open the file
      fileIn = fs.open(path);
      if (startOffset != 0) {
        skipFirstLine = true;
        --startOffset;
        fileIn.seek(startOffset);
      }
      reader = new LineReader(fileIn);
      if (skipFirstLine) {  // skip first line and re-establish "startOffset".
        startOffset += reader.readLine(new Text(), 0,
                    (int)Math.min((long)Integer.MAX_VALUE, end - startOffset));
      }
      this.pos = startOffset;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    public void close() throws IOException { }

    public float getProgress() throws IOException {
      if (startOffset == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (pos - startOffset) / (float)(end - startOffset));
      }
    }

    public boolean nextKeyValue() throws IOException {
      if (key == null) {
        key = new WordOffset();
        key.fileName = path.getName();
      }
      key.offset = pos;
      if (value == null) {
        value = new Text();
      }
      int newSize = 0;
      if (pos < end) {
        newSize = reader.readLine(value);
        pos += newSize;
      }
      if (newSize == 0) {
        key = null;
        value = null;
        return false;
      } else {
        return true;
      }
    }

    public WordOffset getCurrentKey() 
        throws IOException, InterruptedException {
      return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
  }

  /**
   * This Mapper is similar to the one in {@link WordCount.MapClass}.
   */
  public static class MapClass extends 
      Mapper<WordOffset, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(WordOffset key, Text value, Context context)
        throws IOException, InterruptedException {
      
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  private void printUsage() {
    System.out.println("Usage : multifilewc <input_dir> <output>" );
  }

  public int run(String[] args) throws Exception {

    if(args.length < 2) {
      printUsage();
      return 2;
    }

    Job job = new Job(getConf());
    job.setJobName("MultiFileWordCount");
    job.setJarByClass(MultiFileWordCount.class);

    //set the InputFormat of the job to our InputFormat
    job.setInputFormatClass(MyInputFormat.class);
    
    // the keys are words (strings)
    job.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    job.setOutputValueClass(IntWritable.class);

    //use the defined mapper
    job.setMapperClass(MapClass.class);
    //use the WordCount Reducer
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    FileInputFormat.addInputPaths(job, args[0]);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new MultiFileWordCount(), args);
    System.exit(ret);
  }

}
