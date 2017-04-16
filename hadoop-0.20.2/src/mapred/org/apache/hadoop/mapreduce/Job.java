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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;

/**
 * The job submitter's view of the Job. It allows the user to configure the
 * job, submit it, control its execution, and query the state. The set methods
 * only work until the job is submitted, afterwards they will throw an 
 * IllegalStateException.
 */
public class Job extends JobContext {  
  public static enum JobState {DEFINE, RUNNING};
  private JobState state = JobState.DEFINE;
  private JobClient jobClient;
  private RunningJob info;

  public Job() throws IOException {
    this(new Configuration());
  }

  public Job(Configuration conf) throws IOException {
    super(conf, null);
  }

  public Job(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }

  JobClient getJobClient() {
    return jobClient;
  }
  
  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if (state == JobState.RUNNING && jobClient == null) {
      throw new IllegalStateException("Job in state " + JobState.RUNNING + 
                                      " however jobClient is not initialized!");
    }
  }

  /**
   * Set the number of reduce tasks for the job.
   * @param tasks the number of reduce tasks
   * @throws IllegalStateException if the job is submitted
   */
  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setNumReduceTasks(tasks);
  }

  /**
   * Set the current working directory for the default file system.
   * 
   * @param dir the new current working directory.
   * @throws IllegalStateException if the job is submitted
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    conf.setWorkingDirectory(dir);
  }

  /**
   * Set the {@link InputFormat} for the job.
   * @param cls the <code>InputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setInputFormatClass(Class<? extends InputFormat> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(INPUT_FORMAT_CLASS_ATTR, cls, InputFormat.class);
  }

  /**
   * Set the {@link OutputFormat} for the job.
   * @param cls the <code>OutputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(OUTPUT_FORMAT_CLASS_ATTR, cls, OutputFormat.class);
  }

  /**
   * Set the {@link Mapper} for the job.
   * @param cls the <code>Mapper</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapperClass(Class<? extends Mapper> cls
                             ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(MAP_CLASS_ATTR, cls, Mapper.class);
  }

  /**
   * Set the Jar by finding where a given class came from.
   * @param cls the example class
   */
  public void setJarByClass(Class<?> cls) {
    conf.setJarByClass(cls);
  }
  
  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar() {
    return conf.getJar();
  }

  /**
   * Set the combiner class for the job.
   * @param cls the combiner to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setCombinerClass(Class<? extends Reducer> cls
                               ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(COMBINE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Reducer} for the job.
   * @param cls the <code>Reducer</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setReducerClass(Class<? extends Reducer> cls
                              ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(REDUCE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Partitioner} for the job.
   * @param cls the <code>Partitioner</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setPartitionerClass(Class<? extends Partitioner> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(PARTITIONER_CLASS_ATTR, cls, Partitioner.class);
  }

  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * value class.
   * 
   * @param theClass the map output key class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputKeyClass(Class<?> theClass
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputKeyClass(theClass);
  }

  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class.
   * 
   * @param theClass the map output value class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputValueClass(Class<?> theClass
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputValueClass(theClass);
  }

  /**
   * Set the key class for the job output data.
   * 
   * @param theClass the key class for the job output data.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputKeyClass(Class<?> theClass
                                ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyClass(theClass);
  }

  /**
   * Set the value class for job outputs.
   * 
   * @param theClass the value class for job outputs.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputValueClass(Class<?> theClass
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueClass(theClass);
  }

  /**
   * Define the comparator that controls how the keys are sorted before they
   * are passed to the {@link Reducer}.
   * @param cls the raw comparator
   * @throws IllegalStateException if the job is submitted
   */
  public void setSortComparatorClass(Class<? extends RawComparator> cls
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyComparatorClass(cls);
  }

  /**
   * Define the comparator that controls which keys are grouped together
   * for a single call to 
   * {@link Reducer#reduce(Object, Iterable, 
   *                       org.apache.hadoop.mapreduce.Reducer.Context)}
   * @param cls the raw comparator to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setGroupingComparatorClass(Class<? extends RawComparator> cls
                                         ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueGroupingComparator(cls);
  }

  /**
   * Set the user-specified job name.
   * 
   * @param name the job's new name.
   * @throws IllegalStateException if the job is submitted
   */
  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setJobName(name);
  }
  
  /**
   * Set the boolean property for specifying which classpath takes precedence -
   * the user's one or the system one, when the tasks are launched
   * @param value pass true if user's classes should take precedence
   */
  public void setUserClassesTakesPrecedence(boolean value) {
    ensureState(JobState.DEFINE);
    conf.setUserClassesTakesPrecedence(value);
  }
  

  /**
   * Get the URL where some job progress information will be displayed.
   * 
   * @return the URL where some job progress information will be displayed.
   */
  public String getTrackingURL() {
    ensureState(JobState.RUNNING);
    return info.getTrackingURL();
  }

  /**
   * Get the <i>progress</i> of the job's setup, as a float between 0.0 
   * and 1.0.  When the job setup is completed, the function returns 1.0.
   * 
   * @return the progress of the job's setup.
   * @throws IOException
   */
  public float setupProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.setupProgress();
  }

  /**
   * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
   * and 1.0.  When all map tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's map-tasks.
   * @throws IOException
   */
  public float mapProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.mapProgress();
  }

  /**
   * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
   * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's reduce-tasks.
   * @throws IOException
   */
  public float reduceProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.reduceProgress();
  }

  /**
   * Check if the job is finished or not. 
   * This is a non-blocking call.
   * 
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  /**
   * Check if the job completed successfully. 
   * 
   * @return <code>true</code> if the job succeeded, else <code>false</code>.
   * @throws IOException
   */
  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }
    
  /**
   * Get events indicating completion (success/failure) of component tasks.
   *  
   * @param startFrom index to start fetching events from
   * @return an array of {@link TaskCompletionEvent}s
   * @throws IOException
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom
                                                       ) throws IOException {
    ensureState(JobState.RUNNING);
    return info.getTaskCompletionEvents(startFrom);
  }
  
  /**
   * Kill indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void killTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId), 
                  false);
  }

  /**
   * Fail indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void failTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId), 
                  true);
  }

  /**
   * Gets the counters for this job.
   * 
   * @return the counters for this job.
   * @throws IOException
   */
  public Counters getCounters() throws IOException {
    ensureState(JobState.RUNNING);
    return new Counters(info.getCounters());
  }

  private void ensureNotSet(String attr, String msg) throws IOException {
    if (conf.get(attr) != null) {
      throw new IOException(attr + " is incompatible with " + msg + " mode.");
    }    
  }
  
  /**
   * Sets the flag that will allow the JobTracker to cancel the HDFS delegation
   * tokens upon job completion. Defaults to true.
   */
  public void setCancelDelegationTokenUponJobCompletion(boolean value) {
    ensureState(JobState.DEFINE);
    conf.setBoolean(JOB_CANCEL_DELEGATION_TOKEN, value);
  }

  /**
   * Default to the new APIs unless they are explicitly set or the old mapper or
   * reduce attributes are used.
   * @throws IOException if the configuration is inconsistant
   */
  private void setUseNewAPI() throws IOException {
    int numReduces = conf.getNumReduceTasks();
    String oldMapperClass = "mapred.mapper.class";
    String oldReduceClass = "mapred.reducer.class";
    conf.setBooleanIfUnset("mapred.mapper.new-api",
                           conf.get(oldMapperClass) == null);
    if (conf.getUseNewMapper()) {
      String mode = "new map API";
      ensureNotSet("mapred.input.format.class", mode);
      ensureNotSet(oldMapperClass, mode);
      if (numReduces != 0) {
        ensureNotSet("mapred.partitioner.class", mode);
       } else {
        ensureNotSet("mapred.output.format.class", mode);
      }      
    } else {
      String mode = "map compatability";
      ensureNotSet(JobContext.INPUT_FORMAT_CLASS_ATTR, mode);
      ensureNotSet(JobContext.MAP_CLASS_ATTR, mode);
      if (numReduces != 0) {
        ensureNotSet(JobContext.PARTITIONER_CLASS_ATTR, mode);
       } else {
        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
      }
    }
    if (numReduces != 0) {
      conf.setBooleanIfUnset("mapred.reducer.new-api",
                             conf.get(oldReduceClass) == null);
      if (conf.getUseNewReducer()) {
        String mode = "new reduce API";
        ensureNotSet("mapred.output.format.class", mode);
        ensureNotSet(oldReduceClass, mode);   
      } else {
        String mode = "reduce compatability";
        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
        ensureNotSet(JobContext.REDUCE_CLASS_ATTR, mode);   
      }
    }   
  }

  /**
   * Submit the job to the cluster and return immediately.
   * @throws IOException
   */
  public void submit() throws IOException, InterruptedException, 
                              ClassNotFoundException {
    ensureState(JobState.DEFINE);
    setUseNewAPI();
    
    // Connect to the JobTracker and submit the job
    connect();
    info = jobClient.submitJobInternal(conf);
    super.setJobID(info.getID());
    state = JobState.RUNNING;
   }
  
  /**
   * Open a connection to the JobTracker
   * @throws IOException
   * @throws InterruptedException 
   */
  private void connect() throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        jobClient = new JobClient((JobConf) getConfiguration());    
        return null;
      }
    });
  }
  
  /**
   * Submit the job to the cluster and wait for it to finish.
   * @param verbose print the progress to the user
   * @return true if the job succeeded
   * @throws IOException thrown if the communication with the 
   *         <code>JobTracker</code> is lost
   */
  public boolean waitForCompletion(boolean verbose
                                   ) throws IOException, InterruptedException,
                                            ClassNotFoundException {
    if (state == JobState.DEFINE) {
      submit();
    }
    if (verbose) {
      jobClient.monitorAndPrintJob(conf, info);
    } else {
      info.waitForCompletion();
    }
    return isSuccessful();
  }
  
}
