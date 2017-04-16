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

package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * Multithreaded implementation for @link org.apache.hadoop.mapred.MapRunnable.
 * <p>
 * It can be used instead of the default implementation,
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Map implementations using this MapRunnable must be thread-safe.
 * <p>
 * The Map-Reduce job has to be configured to use this MapRunnable class (using
 * the JobConf.setMapRunnerClass method) and
 * the number of thread the thread-pool can use with the
 * <code>mapred.map.multithreadedrunner.threads</code> property, its default
 * value is 10 threads.
 * <p>
 */
public class MultithreadedMapRunner<K1, V1, K2, V2>
    implements MapRunnable<K1, V1, K2, V2> {

  private static final Log LOG =
    LogFactory.getLog(MultithreadedMapRunner.class.getName());

  private JobConf job;
  private Mapper<K1, V1, K2, V2> mapper;
  private ExecutorService executorService;
  private volatile IOException ioException;
  private volatile RuntimeException runtimeException;
  private boolean incrProcCount;

  @SuppressWarnings("unchecked")
  public void configure(JobConf jobConf) {
    int numberOfThreads =
      jobConf.getInt("mapred.map.multithreadedrunner.threads", 10);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Configuring jobConf " + jobConf.getJobName() +
                " to use " + numberOfThreads + " threads");
    }

    this.job = jobConf;
    //increment processed counter only if skipping feature is enabled
    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job)>0 && 
      SkipBadRecords.getAutoIncrMapperProcCount(job);
    this.mapper = ReflectionUtils.newInstance(jobConf.getMapperClass(),
        jobConf);

    // Creating a threadpool of the configured size to execute the Mapper
    // map method in parallel.
    executorService = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 
                                             0L, TimeUnit.MILLISECONDS,
                                             new BlockingArrayQueue
                                               (numberOfThreads));
  }

  /**
   * A blocking array queue that replaces offer and add, which throws on a full
   * queue, to a put, which waits on a full queue.
   */
  private static class BlockingArrayQueue extends ArrayBlockingQueue<Runnable> {
 
    private static final long serialVersionUID = 1L;
    public BlockingArrayQueue(int capacity) {
      super(capacity);
    }
    public boolean offer(Runnable r) {
      return add(r);
    }
    public boolean add(Runnable r) {
      try {
        put(r);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      return true;
    }
  }

  private void checkForExceptionsFromProcessingThreads()
      throws IOException, RuntimeException {
    // Checking if a Mapper.map within a Runnable has generated an
    // IOException. If so we rethrow it to force an abort of the Map
    // operation thus keeping the semantics of the default
    // implementation.
    if (ioException != null) {
      throw ioException;
    }

    // Checking if a Mapper.map within a Runnable has generated a
    // RuntimeException. If so we rethrow it to force an abort of the Map
    // operation thus keeping the semantics of the default
    // implementation.
    if (runtimeException != null) {
      throw runtimeException;
    }
  }

  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter)
    throws IOException {
    try {
      // allocate key & value instances these objects will not be reused
      // because execution of Mapper.map is not serialized.
      K1 key = input.createKey();
      V1 value = input.createValue();

      while (input.next(key, value)) {

        executorService.execute(new MapperInvokeRunable(key, value, output,
                                reporter));

        checkForExceptionsFromProcessingThreads();

        // Allocate new key & value instances as mapper is running in parallel
        key = input.createKey();
        value = input.createValue();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished dispatching all Mappper.map calls, job "
                  + job.getJobName());
      }

      // Graceful shutdown of the Threadpool, it will let all scheduled
      // Runnables to end.
      executorService.shutdown();

      try {

        // Now waiting for all Runnables to end.
        while (!executorService.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Awaiting all running Mappper.map calls to finish, job "
                      + job.getJobName());
          }

          // NOTE: while Mapper.map dispatching has concluded there are still
          // map calls in progress and exceptions would be thrown.
          checkForExceptionsFromProcessingThreads();

        }

        // NOTE: it could be that a map call has had an exception after the
        // call for awaitTermination() returing true. And edge case but it
        // could happen.
        checkForExceptionsFromProcessingThreads();

      } catch (IOException ioEx) {
        // Forcing a shutdown of all thread of the threadpool and rethrowing
        // the IOException
        executorService.shutdownNow();
        throw ioEx;
      } catch (InterruptedException iEx) {
        throw new RuntimeException(iEx);
      }

    } finally {
      mapper.close();
    }
  }


  /**
   * Runnable to execute a single Mapper.map call from a forked thread.
   */
  private class MapperInvokeRunable implements Runnable {
    private K1 key;
    private V1 value;
    private OutputCollector<K2, V2> output;
    private Reporter reporter;

    /**
     * Collecting all required parameters to execute a Mapper.map call.
     * <p>
     *
     * @param key
     * @param value
     * @param output
     * @param reporter
     */
    public MapperInvokeRunable(K1 key, V1 value,
                               OutputCollector<K2, V2> output,
                               Reporter reporter) {
      this.key = key;
      this.value = value;
      this.output = output;
      this.reporter = reporter;
    }

    /**
     * Executes a Mapper.map call with the given Mapper and parameters.
     * <p>
     * This method is called from the thread-pool thread.
     *
     */
    public void run() {
      try {
        // map pair to output
        MultithreadedMapRunner.this.mapper.map(key, value, output, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      } catch (IOException ex) {
        // If there is an IOException during the call it is set in an instance
        // variable of the MultithreadedMapRunner from where it will be
        // rethrown.
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.ioException == null) {
            MultithreadedMapRunner.this.ioException = ex;
          }
        }
      } catch (RuntimeException ex) {
        // If there is a RuntimeException during the call it is set in an
        // instance variable of the MultithreadedMapRunner from where it will be
        // rethrown.
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.runtimeException == null) {
            MultithreadedMapRunner.this.runtimeException = ex;
          }
        }
      }
    }
  }

}
