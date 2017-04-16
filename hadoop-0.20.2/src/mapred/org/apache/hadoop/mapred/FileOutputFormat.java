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

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Progressable;

/** A base class for {@link OutputFormat}. */
public abstract class FileOutputFormat<K, V> implements OutputFormat<K, V> {

  /**
   * Set whether the output of the job is compressed.
   * @param conf the {@link JobConf} to modify
   * @param compress should the output of the job be compressed?
   */
  public static void setCompressOutput(JobConf conf, boolean compress) {
    conf.setBoolean("mapred.output.compress", compress);
  }
  
  /**
   * Is the job output compressed?
   * @param conf the {@link JobConf} to look in
   * @return <code>true</code> if the job output should be compressed,
   *         <code>false</code> otherwise
   */
  public static boolean getCompressOutput(JobConf conf) {
    return conf.getBoolean("mapred.output.compress", false);
  }
  
  /**
   * Set the {@link CompressionCodec} to be used to compress job outputs.
   * @param conf the {@link JobConf} to modify
   * @param codecClass the {@link CompressionCodec} to be used to
   *                   compress the job outputs
   */
  public static void 
  setOutputCompressorClass(JobConf conf, 
                           Class<? extends CompressionCodec> codecClass) {
    setCompressOutput(conf, true);
    conf.setClass("mapred.output.compression.codec", codecClass, 
                  CompressionCodec.class);
  }
  
  /**
   * Get the {@link CompressionCodec} for compressing the job outputs.
   * @param conf the {@link JobConf} to look in
   * @param defaultValue the {@link CompressionCodec} to return if not set
   * @return the {@link CompressionCodec} to be used to compress the 
   *         job outputs
   * @throws IllegalArgumentException if the class was specified, but not found
   */
  public static Class<? extends CompressionCodec> 
  getOutputCompressorClass(JobConf conf, 
		                       Class<? extends CompressionCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;
    
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
  
  public abstract RecordWriter<K, V> getRecordWriter(FileSystem ignored,
                                               JobConf job, String name,
                                               Progressable progress)
    throws IOException;

  public void checkOutputSpecs(FileSystem ignored, JobConf job) 
    throws FileAlreadyExistsException, 
           InvalidJobConfException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null && job.getNumReduceTasks() != 0) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    if (outDir != null) {
      FileSystem fs = outDir.getFileSystem(job);
      // normalize the output directory
      outDir = fs.makeQualified(outDir);
      setOutputPath(job, outDir);
      
      // get delegation token for the outDir's file system
      TokenCache.obtainTokensForNamenodes(job.getCredentials(), 
                                          new Path[] {outDir}, job);
      
      // check its existence
      if (fs.exists(outDir)) {
        throw new FileAlreadyExistsException("Output directory " + outDir + 
                                             " already exists");
      }
    }
  }

  /**
   * Set the {@link Path} of the output directory for the map-reduce job.
   *
   * @param conf The configuration of the job.
   * @param outputDir the {@link Path} of the output directory for 
   * the map-reduce job.
   */
  public static void setOutputPath(JobConf conf, Path outputDir) {
    outputDir = new Path(conf.getWorkingDirectory(), outputDir);
    conf.set("mapred.output.dir", outputDir.toString());
  }

  /**
   * Set the {@link Path} of the task's temporary output directory 
   * for the map-reduce job.
   * 
   * <p><i>Note</i>: Task output path is set by the framework.
   * </p>
   * @param conf The configuration of the job.
   * @param outputDir the {@link Path} of the output directory 
   * for the map-reduce job.
   */
  
  static void setWorkOutputPath(JobConf conf, Path outputDir) {
    outputDir = new Path(conf.getWorkingDirectory(), outputDir);
    conf.set("mapred.work.output.dir", outputDir.toString());
  }
  
  /**
   * Get the {@link Path} to the output directory for the map-reduce job.
   * 
   * @return the {@link Path} to the output directory for the map-reduce job.
   * @see FileOutputFormat#getWorkOutputPath(JobConf)
   */
  public static Path getOutputPath(JobConf conf) {
    String name = conf.get("mapred.output.dir");
    return name == null ? null: new Path(name);
  }
  
  /**
   *  Get the {@link Path} to the task's temporary output directory 
   *  for the map-reduce job
   *  
   * <h4 id="SideEffectFiles">Tasks' Side-Effect Files</h4>
   * 
   * <p><i>Note:</i> The following is valid only if the {@link OutputCommitter}
   *  is {@link FileOutputCommitter}. If <code>OutputCommitter</code> is not 
   *  a <code>FileOutputCommitter</code>, the task's temporary output
   *  directory is same as {@link #getOutputPath(JobConf)} i.e.
   *  <tt>${mapred.output.dir}$</tt></p>
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
   * side-files required in <tt>${mapred.work.output.dir}</tt> during execution 
   * of his reduce-task i.e. via {@link #getWorkOutputPath(JobConf)}, and the 
   * framework will move them out similarly - thus she doesn't have to pick 
   * unique paths per task-attempt.</p>
   * 
   * <p><i>Note</i>: the value of <tt>${mapred.work.output.dir}</tt> during 
   * execution of a particular task-attempt is actually 
   * <tt>${mapred.output.dir}/_temporary/_{$taskid}</tt>, and this value is 
   * set by the map-reduce framework. So, just create any side-files in the 
   * path  returned by {@link #getWorkOutputPath(JobConf)} from map/reduce 
   * task to take advantage of this feature.</p>
   * 
   * <p>The entire discussion holds true for maps of jobs with 
   * reducer=NONE (i.e. 0 reduces) since output of the map, in that case, 
   * goes directly to HDFS.</p> 
   * 
   * @return the {@link Path} to the task's temporary output directory 
   * for the map-reduce job.
   */
  public static Path getWorkOutputPath(JobConf conf) {
    String name = conf.get("mapred.work.output.dir");
    return name == null ? null: new Path(name);
  }

  /**
   * Helper function to create the task's temporary output directory and 
   * return the path to the task's output file.
   * 
   * @param conf job-configuration
   * @param name temporary task-output filename
   * @return path to the task's temporary output file
   * @throws IOException
   */
  public static Path getTaskOutputPath(JobConf conf, String name) 
  throws IOException {
    // ${mapred.out.dir}
    Path outputPath = getOutputPath(conf);
    if (outputPath == null) {
      throw new IOException("Undefined job output-path");
    }

    OutputCommitter committer = conf.getOutputCommitter();
    Path workPath = outputPath;
    TaskAttemptContext context = new TaskAttemptContext(conf,
                TaskAttemptID.forName(conf.get("mapred.task.id")));
    if (committer instanceof FileOutputCommitter) {
      workPath = ((FileOutputCommitter)committer).getWorkPath(context,
                                                              outputPath);
    }
    
    // ${mapred.out.dir}/_temporary/_${taskid}/${name}
    return new Path(workPath, name);
  } 

  /**
   * Helper function to generate a name that is unique for the task.
   *
   * <p>The generated name can be used to create custom files from within the
   * different tasks for the job, the names for different tasks will not collide
   * with each other.</p>
   *
   * <p>The given name is postfixed with the task type, 'm' for maps, 'r' for
   * reduces and the task partition number. For example, give a name 'test'
   * running on the first map o the job the generated name will be
   * 'test-m-00000'.</p>
   *
   * @param conf the configuration for the job.
   * @param name the name to make unique.
   * @return a unique name accross all tasks of the job.
   */
  public static String getUniqueName(JobConf conf, String name) {
    int partition = conf.getInt("mapred.task.partition", -1);
    if (partition == -1) {
      throw new IllegalArgumentException(
        "This method can only be called from within a Job");
    }

    String taskType = (conf.getBoolean("mapred.task.is.map", true)) ? "m" : "r";

    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(5);
    numberFormat.setGroupingUsed(false);

    return name + "-" + taskType + "-" + numberFormat.format(partition);
  }

  /**
   * Helper function to generate a {@link Path} for a file that is unique for
   * the task within the job output directory.
   *
   * <p>The path can be used to create custom files from within the map and
   * reduce tasks. The path name will be unique for each task. The path parent
   * will be the job output directory.</p>ls
   *
   * <p>This method uses the {@link #getUniqueName} method to make the file name
   * unique for the task.</p>
   *
   * @param conf the configuration for the job.
   * @param name the name for the file.
   * @return a unique path accross all tasks of the job.
   */
  public static Path getPathForCustomFile(JobConf conf, String name) {
    return new Path(getWorkOutputPath(conf), getUniqueName(conf, name));
  }
}

