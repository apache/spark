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

package org.apache.hadoop.mapreduce.lib.map;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper.
 * <p>
 * It can be used instead of the default implementation,
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Mapper implementations using this MapRunnable must be thread-safe.
 * <p>
 * The Map-Reduce job has to be configured with the mapper to use via 
 * {@link #setMapperClass(Configuration, Class)} and
 * the number of thread the thread-pool can use with the
 * {@link #getNumberOfThreads(Configuration) method. The default
 * value is 10 threads.
 * <p>
 */
public class MultithreadedMapper<K1, V1, K2, V2> 
  extends Mapper<K1, V1, K2, V2> {

  private static final Log LOG = LogFactory.getLog(MultithreadedMapper.class);
  private Class<? extends Mapper<K1,V1,K2,V2>> mapClass;
  private Context outer;
  private List<MapRunner> runners;

  /**
   * The number of threads in the thread pool that will run the map function.
   * @param job the job
   * @return the number of threads
   */
  public static int getNumberOfThreads(JobContext job) {
    return job.getConfiguration().
            getInt("mapred.map.multithreadedrunner.threads", 10);
  }

  /**
   * Set the number of threads in the pool for running maps.
   * @param job the job to modify
   * @param threads the new number of threads
   */
  public static void setNumberOfThreads(Job job, int threads) {
    job.getConfiguration().setInt("mapred.map.multithreadedrunner.threads", 
                                  threads);
  }

  /**
   * Get the application's mapper class.
   * @param <K1> the map's input key type
   * @param <V1> the map's input value type
   * @param <K2> the map's output key type
   * @param <V2> the map's output value type
   * @param job the job
   * @return the mapper class to run
   */
  @SuppressWarnings("unchecked")
  public static <K1,V1,K2,V2>
  Class<Mapper<K1,V1,K2,V2>> getMapperClass(JobContext job) {
    return (Class<Mapper<K1,V1,K2,V2>>) 
         job.getConfiguration().getClass("mapred.map.multithreadedrunner.class",
                                         Mapper.class);
  }
  
  /**
   * Set the application's mapper class.
   * @param <K1> the map input key type
   * @param <V1> the map input value type
   * @param <K2> the map output key type
   * @param <V2> the map output value type
   * @param job the job to modify
   * @param cls the class to use as the mapper
   */
  public static <K1,V1,K2,V2> 
  void setMapperClass(Job job, 
                      Class<? extends Mapper<K1,V1,K2,V2>> cls) {
    if (MultithreadedMapper.class.isAssignableFrom(cls)) {
      throw new IllegalArgumentException("Can't have recursive " + 
                                         "MultithreadedMapper instances.");
    }
    job.getConfiguration().setClass("mapred.map.multithreadedrunner.class",
                                    cls, Mapper.class);
  }

  /**
   * Run the application's maps using a thread pool.
   */
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    outer = context;
    int numberOfThreads = getNumberOfThreads(context);
    mapClass = getMapperClass(context);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring multithread runner to use " + numberOfThreads + 
                " threads");
    }
    
    runners =  new ArrayList<MapRunner>(numberOfThreads);
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = new MapRunner(context);
      thread.start();
      runners.add(i, thread);
    }
    for(int i=0; i < numberOfThreads; ++i) {
      MapRunner thread = runners.get(i);
      thread.join();
      Throwable th = thread.throwable;
      if (th != null) {
        if (th instanceof IOException) {
          throw (IOException) th;
        } else if (th instanceof InterruptedException) {
          throw (InterruptedException) th;
        } else {
          throw new RuntimeException(th);
        }
      }
    }
  }

  private class SubMapRecordReader extends RecordReader<K1,V1> {
    private K1 key;
    private V1 value;
    private Configuration conf;

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void initialize(InputSplit split, 
                           TaskAttemptContext context
                           ) throws IOException, InterruptedException {
      conf = context.getConfiguration();
    }


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      synchronized (outer) {
        if (!outer.nextKeyValue()) {
          return false;
        }
        key = ReflectionUtils.copy(outer.getConfiguration(),
                                   outer.getCurrentKey(), key);
        value = ReflectionUtils.copy(conf, outer.getCurrentValue(), value);
        return true;
      }
    }

    public K1 getCurrentKey() {
      return key;
    }

    @Override
    public V1 getCurrentValue() {
      return value;
    }
  }
  
  private class SubMapRecordWriter extends RecordWriter<K2,V2> {

    @Override
    public void close(TaskAttemptContext context) throws IOException,
                                                 InterruptedException {
    }

    @Override
    public void write(K2 key, V2 value) throws IOException,
                                               InterruptedException {
      synchronized (outer) {
        outer.write(key, value);
      }
    }  
  }

  private class SubMapStatusReporter extends StatusReporter {

    @Override
    public Counter getCounter(Enum<?> name) {
      return outer.getCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
      return outer.getCounter(group, name);
    }

    @Override
    public void progress() {
      outer.progress();
    }

    @Override
    public void setStatus(String status) {
      outer.setStatus(status);
    }
    
  }

  private class MapRunner extends Thread {
    private Mapper<K1,V1,K2,V2> mapper;
    private Context subcontext;
    private Throwable throwable;

    MapRunner(Context context) throws IOException, InterruptedException {
      mapper = ReflectionUtils.newInstance(mapClass, 
                                           context.getConfiguration());
      subcontext = new Context(outer.getConfiguration(), 
                            outer.getTaskAttemptID(),
                            new SubMapRecordReader(),
                            new SubMapRecordWriter(), 
                            context.getOutputCommitter(),
                            new SubMapStatusReporter(),
                            outer.getInputSplit());
    }

    public Throwable getThrowable() {
      return throwable;
    }

    @Override
    public void run() {
      try {
        mapper.run(subcontext);
      } catch (Throwable ie) {
        throwable = ie;
      }
    }
  }

}
