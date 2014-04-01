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
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/** An {@link OutputCommitter} that commits files specified 
 * in job output directory i.e. ${mapred.output.dir}. 
 **/
public class FileOutputCommitter extends OutputCommitter {

  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FileOutputCommitter");
/**
   * Temporary directory name 
   */
  public static final String TEMP_DIR_NAME = "_temporary";
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";

  public void setupJob(JobContext context) throws IOException {
    JobConf conf = context.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path tmpDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(conf);
      if (!fileSys.mkdirs(tmpDir)) {
        LOG.error("Mkdirs failed to create " + tmpDir.toString());
      }
    }
  }

  private static boolean getOutputDirMarking(JobConf conf) {
    return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, 
                           true);
  }

  // Mark the output dir of the job for which the context is passed.
  private void markSuccessfulOutputDir(JobContext context) 
  throws IOException {
    JobConf conf = context.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      FileSystem fileSys = outputPath.getFileSystem(conf);
      // create a file in the folder to mark it
      if (fileSys.exists(outputPath)) {
        Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
        fileSys.create(filePath).close();
      }
    }
  }
  
  @Override
  public void commitJob(JobContext context) throws IOException {
    cleanupJob(context);
    if (getOutputDirMarking(context.getJobConf())) {
      markSuccessfulOutputDir(context);
    }
  }
  
  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    JobConf conf = context.getJobConf();
    // do the clean up of temporary directory
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path tmpDir = new Path(outputPath, FileOutputCommitter.TEMP_DIR_NAME);
      FileSystem fileSys = tmpDir.getFileSystem(conf);
      context.getProgressible().progress();
      if (fileSys.exists(tmpDir)) {
        fileSys.delete(tmpDir, true);
      }
    } else {
      LOG.warn("Output path is null in cleanup");
    }
  }

  /**
   * Delete the temporary directory, including all of the work directories.
   * @param context the job's context
   * @param runState final run state of the job, should be
   * {@link JobStatus#KILLED} or {@link JobStatus#FAILED}
   */
  @Override
  public void abortJob(JobContext context, int runState) throws IOException {
    cleanupJob(context);
  }
  
  public void setupTask(TaskAttemptContext context) throws IOException {
    // FileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the 
    // task is writing.
  }
		  
  public void commitTask(TaskAttemptContext context) 
  throws IOException {
    Path taskOutputPath = getTempTaskOutputPath(context);
    TaskAttemptID attemptId = context.getTaskAttemptID();
    JobConf job = context.getJobConf();
    if (taskOutputPath != null) {
      FileSystem fs = taskOutputPath.getFileSystem(job);
      context.getProgressible().progress();
      if (fs.exists(taskOutputPath)) {
        Path jobOutputPath = taskOutputPath.getParent().getParent();
        // Move the task outputs to their final place
        moveTaskOutputs(context, fs, jobOutputPath, taskOutputPath);
        // Delete the temporary task-specific output directory
        if (!fs.delete(taskOutputPath, true)) {
          LOG.info("Failed to delete the temporary output" + 
          " directory of task: " + attemptId + " - " + taskOutputPath);
        }
        LOG.info("Saved output of task '" + attemptId + "' to " + 
                 jobOutputPath);
      }
    }
  }
		  
  private void moveTaskOutputs(TaskAttemptContext context,
                               FileSystem fs,
                               Path jobOutputDir,
                               Path taskOutput) 
  throws IOException {
    TaskAttemptID attemptId = context.getTaskAttemptID();
    context.getProgressible().progress();
    if (fs.isFile(taskOutput)) {
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, 
                                          getTempTaskOutputPath(context));
      if (!fs.rename(taskOutput, finalOutputPath)) {
        if (!fs.delete(finalOutputPath, true)) {
          throw new IOException("Failed to delete earlier output of task: " + 
                                 attemptId);
        }
        if (!fs.rename(taskOutput, finalOutputPath)) {
          throw new IOException("Failed to save output of task: " + 
        		  attemptId);
        }
      }
      LOG.debug("Moved " + taskOutput + " to " + finalOutputPath);
    } else if(fs.getFileStatus(taskOutput).isDir()) {
      FileStatus[] paths = fs.listStatus(taskOutput);
      Path finalOutputPath = getFinalPath(jobOutputDir, taskOutput, 
	          getTempTaskOutputPath(context));
      fs.mkdirs(finalOutputPath);
      if (paths != null) {
        for (FileStatus path : paths) {
          moveTaskOutputs(context, fs, jobOutputDir, path.getPath());
        }
      }
    }
  }

  public void abortTask(TaskAttemptContext context) throws IOException {
    Path taskOutputPath =  getTempTaskOutputPath(context);
    try {
      if (taskOutputPath != null) {
        FileSystem fs = taskOutputPath.getFileSystem(context.getJobConf());
        context.getProgressible().progress();
        fs.delete(taskOutputPath, true);
      }
    } catch (IOException ie) {
      LOG.warn("Error discarding output" + StringUtils.stringifyException(ie));
    }
  }

  private Path getFinalPath(Path jobOutputDir, Path taskOutput, 
                            Path taskOutputPath) throws IOException {
    URI taskOutputUri = taskOutput.toUri();
    URI relativePath = taskOutputPath.toUri().relativize(taskOutputUri);
    if (taskOutputUri == relativePath) {//taskOutputPath is not a parent of taskOutput
      throw new IOException("Can not get the relative path: base = " + 
          taskOutputPath + " child = " + taskOutput);
    }
    if (relativePath.getPath().length() > 0) {
      return new Path(jobOutputDir, relativePath.getPath());
    } else {
      return jobOutputDir;
    }
  }

  public boolean needsTaskCommit(TaskAttemptContext context) 
  throws IOException {
    try {
      Path taskOutputPath = getTempTaskOutputPath(context);
      if (taskOutputPath != null) {
        context.getProgressible().progress();
        // Get the file-system for the task output directory
        FileSystem fs = taskOutputPath.getFileSystem(context.getJobConf());
        // since task output path is created on demand, 
        // if it exists, task needs a commit
        if (fs.exists(taskOutputPath)) {
          return true;
        }
      }
    } catch (IOException  ioe) {
      throw ioe;
    }
    return false;
  }

  Path getTempTaskOutputPath(TaskAttemptContext taskContext) {
    JobConf conf = taskContext.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path p = new Path(outputPath,
                     (FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
                      "_" + taskContext.getTaskAttemptID().toString()));
      try {
        FileSystem fs = p.getFileSystem(conf);
        return p.makeQualified(fs);
      } catch (IOException ie) {
        LOG.warn(StringUtils .stringifyException(ie));
        return p;
      }
    }
    return null;
  }
  
  Path getWorkPath(TaskAttemptContext taskContext, Path basePath) 
  throws IOException {
    // ${mapred.out.dir}/_temporary
    Path jobTmpDir = new Path(basePath, FileOutputCommitter.TEMP_DIR_NAME);
    FileSystem fs = jobTmpDir.getFileSystem(taskContext.getJobConf());
    if (!fs.exists(jobTmpDir)) {
      throw new IOException("The temporary job-output directory " + 
          jobTmpDir.toString() + " doesn't exist!"); 
    }
    // ${mapred.out.dir}/_temporary/_${taskid}
    String taskid = taskContext.getTaskAttemptID().toString();
    Path taskTmpDir = new Path(jobTmpDir, "_" + taskid);
    if (!fs.mkdirs(taskTmpDir)) {
      throw new IOException("Mkdirs failed to create " 
          + taskTmpDir.toString());
    }
    return taskTmpDir;
  }
}
