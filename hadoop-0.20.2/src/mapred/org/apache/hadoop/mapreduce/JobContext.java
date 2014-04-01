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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A read-only view of the job that is provided to the tasks while they
 * are running.
 */
public class JobContext {
  // Put all of the attribute names in here so that Job and JobContext are
  // consistent.
  protected static final String INPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.inputformat.class";
  protected static final String MAP_CLASS_ATTR = "mapreduce.map.class";
  protected static final String COMBINE_CLASS_ATTR = "mapreduce.combine.class";
  protected static final String REDUCE_CLASS_ATTR = "mapreduce.reduce.class";
  protected static final String OUTPUT_FORMAT_CLASS_ATTR = 
    "mapreduce.outputformat.class";
  protected static final String PARTITIONER_CLASS_ATTR = 
    "mapreduce.partitioner.class";
  public static final String JAR_UNPACK_PATTERN = "mapreduce.job.jar.unpack.pattern";

  protected final org.apache.hadoop.mapred.JobConf conf;
  protected final Credentials credentials;
  private JobID jobId;

  public static final String JOB_NAMENODES = "mapreduce.job.hdfs-servers";

  public static final String JOB_ACL_VIEW_JOB = "mapreduce.job.acl-view-job";
  public static final String JOB_ACL_MODIFY_JOB =
    "mapreduce.job.acl-modify-job";

  public static final String CACHE_FILE_VISIBILITIES = 
    "mapreduce.job.cache.files.visibilities";
  public static final String CACHE_ARCHIVES_VISIBILITIES = 
    "mapreduce.job.cache.archives.visibilities";
  
  public static final String JOB_CANCEL_DELEGATION_TOKEN = 
    "mapreduce.job.complete.cancel.delegation.tokens";
  public static final String USER_LOG_RETAIN_HOURS = 
    "mapred.userlog.retain.hours";
  public static final String MAPREDUCE_TASK_CLASSPATH_PRECEDENCE = 
    "mapreduce.task.classpath.user.precedence";
  
  public static final String MAP_MEMORY_PHYSICAL_MB =
    "mapreduce.map.memory.physical.mb";
  public static final String REDUCE_MEMORY_PHYSICAL_MB = 
     "mapreduce.reduce.memory.physical.mb";
  
  /**
   * The UserGroupInformation object that has a reference to the current user
   */
  protected UserGroupInformation ugi;
  
  public JobContext(Configuration conf, JobID jobId) {
    this.conf = new org.apache.hadoop.mapred.JobConf(conf);
    this.credentials = this.conf.getCredentials();
    this.jobId = jobId;
    try {
      this.ugi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  void setJobID(JobID jobId) {
    this.jobId = jobId;
  }

  /**
   * Return the configuration for the job.
   * @return the shared configuration object
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Get credentials for the job.
   * @return credentials for the job
   */
  public Credentials getCredentials() {
    return credentials;
  }

  /**
   * Get the unique ID for the job.
   * @return the object with the job id
   */
  public JobID getJobID() {
    return jobId;
  }
  
  /**
   * Get configured the number of reduce tasks for this job. Defaults to 
   * <code>1</code>.
   * @return the number of reduce tasks for this job.
   */
  public int getNumReduceTasks() {
    return conf.getNumReduceTasks();
  }
  
  /**
   * Get the current working directory for the default file system.
   * 
   * @return the directory name.
   */
  public Path getWorkingDirectory() throws IOException {
    return conf.getWorkingDirectory();
  }

  /**
   * Get the key class for the job output data.
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass() {
    return conf.getOutputKeyClass();
  }
  
  /**
   * Get the value class for job outputs.
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass() {
    return conf.getOutputValueClass();
  }

  /**
   * Get the key class for the map output data. If it is not set, use the
   * (final) output key class. This allows the map output key class to be
   * different than the final output key class.
   * @return the map output key class.
   */
  public Class<?> getMapOutputKeyClass() {
    return conf.getMapOutputKeyClass();
  }

  /**
   * Get the value class for the map output data. If it is not set, use the
   * (final) output value class This allows the map output value class to be
   * different than the final output value class.
   *  
   * @return the map output value class.
   */
  public Class<?> getMapOutputValueClass() {
    return conf.getMapOutputValueClass();
  }

  /**
   * Get the user-specified job name. This is only used to identify the 
   * job to the user.
   * 
   * @return the job's name, defaulting to "".
   */
  public String getJobName() {
    return conf.getJobName();
  }
  
  /**
   * Get the boolean value for the property that specifies which classpath
   * takes precedence when tasks are launched. True - user's classes takes
   * precedence. False - system's classes takes precedence.
   * @return true if user's classes should take precedence
   */
  public boolean userClassesTakesPrecedence() {
    return conf.userClassesTakesPrecedence();
  }

  /**
   * Get the {@link InputFormat} class for the job.
   * 
   * @return the {@link InputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends InputFormat<?,?>> getInputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends InputFormat<?,?>>) 
      conf.getClass(INPUT_FORMAT_CLASS_ATTR, TextInputFormat.class);
  }

  /**
   * Get the {@link Mapper} class for the job.
   * 
   * @return the {@link Mapper} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Mapper<?,?,?,?>> getMapperClass() 
     throws ClassNotFoundException {
    return (Class<? extends Mapper<?,?,?,?>>) 
      conf.getClass(MAP_CLASS_ATTR, Mapper.class);
  }

  /**
   * Get the combiner class for the job.
   * 
   * @return the combiner class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getCombinerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getClass(COMBINE_CLASS_ATTR, null);
  }

  /**
   * Get the {@link Reducer} class for the job.
   * 
   * @return the {@link Reducer} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Reducer<?,?,?,?>> getReducerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Reducer<?,?,?,?>>) 
      conf.getClass(REDUCE_CLASS_ATTR, Reducer.class);
  }

  /**
   * Get the {@link OutputFormat} class for the job.
   * 
   * @return the {@link OutputFormat} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends OutputFormat<?,?>> getOutputFormatClass() 
     throws ClassNotFoundException {
    return (Class<? extends OutputFormat<?,?>>) 
      conf.getClass(OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class);
  }

  /**
   * Get the {@link Partitioner} class for the job.
   * 
   * @return the {@link Partitioner} class for the job.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends Partitioner<?,?>> getPartitionerClass() 
     throws ClassNotFoundException {
    return (Class<? extends Partitioner<?,?>>) 
      conf.getClass(PARTITIONER_CLASS_ATTR, HashPartitioner.class);
  }

  /**
   * Get the {@link RawComparator} comparator used to compare keys.
   * 
   * @return the {@link RawComparator} comparator used to compare keys.
   */
  public RawComparator<?> getSortComparator() {
    return conf.getOutputKeyComparator();
  }

  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar() {
    return conf.getJar();
  }

  /** 
   * Get the user defined {@link RawComparator} comparator for 
   * grouping keys of inputs to the reduce.
   * 
   * @return comparator set by the user for grouping values.
   * @see Job#setGroupingComparatorClass(Class) for details.  
   */
  public RawComparator<?> getGroupingComparator() {
    return conf.getOutputValueGroupingComparator();
  }
}
