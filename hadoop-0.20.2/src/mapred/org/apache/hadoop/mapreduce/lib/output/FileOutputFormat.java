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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.security.TokenCache;

/** A base class for {@link OutputFormat}s that read from {@link FileSystem}s.*/
public abstract class FileOutputFormat<K, V> extends OutputFormat<K, V> {

  /** Construct output file names so that, when an output directory listing is
   * sorted lexicographically, positions correspond to output partitions.*/
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  protected static final String BASE_OUTPUT_NAME = "mapreduce.output.basename";
  protected static final String PART = "part";
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  private FileOutputCommitter committer = null;

  /**
   * Set whether the output of the job is compressed.
   * @param job the job to modify
   * @param compress should the output of the job be compressed?
   */
  public static void setCompressOutput(Job job, boolean compress) {
    job.getConfiguration().setBoolean("mapred.output.compress", compress);
  }
  
  /**
   * Is the job output compressed?
   * @param job the Job to look in
   * @return <code>true</code> if the job output should be compressed,
   *         <code>false</code> otherwise
   */
  public static boolean getCompressOutput(JobContext job) {
    return job.getConfiguration().getBoolean("mapred.output.compress", false);
  }
  
  /**
   * Set the {@link CompressionCodec} to be used to compress job outputs.
   * @param job the job to modify
   * @param codecClass the {@link CompressionCodec} to be used to
   *                   compress the job outputs
   */
  public static void 
  setOutputCompressorClass(Job job, 
                           Class<? extends CompressionCodec> codecClass) {
    setCompressOutput(job, true);
    job.getConfiguration().setClass("mapred.output.compression.codec", 
                                    codecClass, 
                                    CompressionCodec.class);
  }
  
  /**
   * Get the {@link CompressionCodec} for compressing the job outputs.
   * @param job the {@link Job} to look in
   * @param defaultValue the {@link CompressionCodec} to return if not set
   * @return the {@link CompressionCodec} to be used to compress the 
   *         job outputs
   * @throws IllegalArgumentException if the class was specified, but not found
   */
  public static Class<? extends CompressionCodec> 
  getOutputCompressorClass(JobContext job, 
		                       Class<? extends CompressionCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    Configuration conf = job.getConfiguration();
    String name = conf.get("mapred.output.compression.codec");
    if (name != null) {
      try {
        codecClass = 
        	conf.getClassByName(name).asSubclass(CompressionCodec.class);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name + 
                                           " was not found.", e);
      }
    }
    return codecClass;
  }
  
  public abstract RecordWriter<K, V> 
     getRecordWriter(TaskAttemptContext job
                     ) throws IOException, InterruptedException;

  public void checkOutputSpecs(JobContext job
                               ) throws FileAlreadyExistsException, IOException{
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.");
    }
    
    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), 
                                        new Path[] {outDir}, 
                                        job.getConfiguration());

    if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
      throw new FileAlreadyExistsException("Output directory " + outDir + 
                                           " already exists");
    }
  }

  /**
   * Set the {@link Path} of the output directory for the map-reduce job.
   *
   * @param job The job to modify
   * @param outputDir the {@link Path} of the output directory for 
   * the map-reduce job.
   */
  public static void setOutputPath(Job job, Path outputDir) {
    job.getConfiguration().set("mapred.output.dir", outputDir.toString());
  }

  /**
   * Get the {@link Path} to the output directory for the map-reduce job.
   * 
   * @return the {@link Path} to the output directory for the map-reduce job.
   * @see FileOutputFormat#getWorkOutputPath(TaskInputOutputContext)
   */
  public static Path getOutputPath(JobContext job) {
    String name = job.getConfiguration().get("mapred.output.dir");
    return name == null ? null: new Path(name);
  }
  
  /**
   *  Get the {@link Path} to the task's temporary output directory 
   *  for the map-reduce job
   *  
   * <h4 id="SideEffectFiles">Tasks' Side-Effect Files</h4>
   * 
   * <p>Some applications need to create/write-to side-files, which differ from
   * the actual job-outputs.
   * 
   * <p>In such cases there could be issues with 2 instances of the same TIP 
   * (running simultaneously e.g. speculative tasks) trying to open/write-to the
   * same file (path) on HDFS. Hence the application-writer will have to pick 
   * unique names per task-attempt (e.g. using the attemptid, say 
   * <tt>attempt_200709221812_0001_m_000000_0</tt>), not just per TIP.</p> 
   * 
   * <p>To get around this the Map-Reduce framework helps the application-writer 
   * out by maintaining a special 
   * <tt>${mapred.output.dir}/_temporary/_${taskid}</tt> 
   * sub-directory for each task-attempt on HDFS where the output of the 
   * task-attempt goes. On successful completion of the task-attempt the files 
   * in the <tt>${mapred.output.dir}/_temporary/_${taskid}</tt> (only) 
   * are <i>promoted</i> to <tt>${mapred.output.dir}</tt>. Of course, the 
   * framework discards the sub-directory of unsuccessful task-attempts. This 
   * is completely transparent to the application.</p>
   * 
   * <p>The application-writer can take advantage of this by creating any 
   * side-files required in a work directory during execution 
   * of his task i.e. via 
   * {@link #getWorkOutputPath(TaskInputOutputContext)}, and
   * the framework will move them out similarly - thus she doesn't have to pick 
   * unique paths per task-attempt.</p>
   * 
   * <p>The entire discussion holds true for maps of jobs with 
   * reducer=NONE (i.e. 0 reduces) since output of the map, in that case, 
   * goes directly to HDFS.</p> 
   * 
   * @return the {@link Path} to the task's temporary output directory 
   * for the map-reduce job.
   */
  public static Path getWorkOutputPath(TaskInputOutputContext<?,?,?,?> context
                                       ) throws IOException, 
                                                InterruptedException {
    FileOutputCommitter committer = (FileOutputCommitter) 
      context.getOutputCommitter();
    return committer.getWorkPath();
  }

  /**
   * Helper function to generate a {@link Path} for a file that is unique for
   * the task within the job output directory.
   *
   * <p>The path can be used to create custom files from within the map and
   * reduce tasks. The path name will be unique for each task. The path parent
   * will be the job output directory.</p>ls
   *
   * <p>This method uses the {@link #getUniqueFile} method to make the file name
   * unique for the task.</p>
   *
   * @param context the context for the task.
   * @param name the name for the file.
   * @param extension the extension for the file
   * @return a unique path accross all tasks of the job.
   */
  public 
  static Path getPathForWorkFile(TaskInputOutputContext<?,?,?,?> context, 
                                 String name,
                                 String extension
                                ) throws IOException, InterruptedException {
    return new Path(getWorkOutputPath(context),
                    getUniqueFile(context, name, extension));
  }

  /**
   * Generate a unique filename, based on the task id, name, and extension
   * @param context the task that is calling this
   * @param name the base filename
   * @param extension the filename extension
   * @return a string like $name-[mr]-$id$extension
   */
  public synchronized static String getUniqueFile(TaskAttemptContext context,
                                                  String name,
                                                  String extension) {
    TaskID taskId = context.getTaskAttemptID().getTaskID();
    int partition = taskId.getId();
    StringBuilder result = new StringBuilder();
    result.append(name);
    result.append('-');
    result.append(taskId.isMap() ? 'm' : 'r');
    result.append('-');
    result.append(NUMBER_FORMAT.format(partition));
    result.append(extension);
    return result.toString();
  }

  /**
   * Get the default path and filename for the output format.
   * @param context the task context
   * @param extension an extension to add to the filename
   * @return a full path $output/_temporary/$taskid/part-[mr]-$id
   * @throws IOException
   */
  public Path getDefaultWorkFile(TaskAttemptContext context,
                                 String extension) throws IOException{
    FileOutputCommitter committer = 
      (FileOutputCommitter) getOutputCommitter(context);
    return new Path(committer.getWorkPath(), getUniqueFile(context, 
      getOutputName(context), extension));
  }

  /**
   * Get the base output name for the output file.
   */
  protected static String getOutputName(JobContext job) {
    return job.getConfiguration().get(BASE_OUTPUT_NAME, PART);
  }

  /**
   * Set the base output name for output file to be created.
   */
  protected static void setOutputName(JobContext job, String name) {
    job.getConfiguration().set(BASE_OUTPUT_NAME, name);
  }

  public synchronized 
     OutputCommitter getOutputCommitter(TaskAttemptContext context
                                        ) throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new FileOutputCommitter(output, context);
    }
    return committer;
  }
}

