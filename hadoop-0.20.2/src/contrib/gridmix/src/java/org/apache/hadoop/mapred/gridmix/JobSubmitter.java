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
package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Component accepting deserialized job traces, computing split data, and
 * submitting to the cluster on deadline. Each job added from an upstream
 * factory must be submitted to the cluster by the deadline recorded on it.
 * Once submitted, jobs must be added to a downstream component for
 * monitoring.
 */
class JobSubmitter implements Gridmix.Component<GridmixJob> {

  public static final Log LOG = LogFactory.getLog(JobSubmitter.class);

  private final Semaphore sem;
  private final FilePool inputDir;
  private final JobMonitor monitor;
  private final Statistics statistics;
  private final ExecutorService sched;
  private volatile boolean shutdown = false;

  /**
   * Initialize the submission component with downstream monitor and pool of
   * files from which split data may be read.
   * @param monitor Monitor component to which jobs should be passed
   * @param threads Number of submission threads
   *   See {@link Gridmix#GRIDMIX_SUB_THR}.
   * @param queueDepth Max depth of pending work queue
   *   See {@link Gridmix#GRIDMIX_QUE_DEP}.
   * @param inputDir Set of files from which split data may be mined for
   * synthetic job
   * @param statistics
   */
  public JobSubmitter(
    JobMonitor monitor, int threads, int queueDepth, FilePool inputDir,
    Statistics statistics) {
    sem = new Semaphore(queueDepth);
    sched = new ThreadPoolExecutor(threads, threads, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    this.inputDir = inputDir;
    this.monitor = monitor;
    this.statistics = statistics;
  }

  /**
   * Runnable wrapping a job to be submitted to the cluster.
   */
  private class SubmitTask implements Runnable {

    final GridmixJob job;
    public SubmitTask(GridmixJob job) {
      this.job = job;
    }
    public void run() {
      try {
        // pre-compute split information
        try {
          job.buildSplits(inputDir);
        } catch (IOException e) {
          LOG.warn("Failed to submit " + job.getJob().getJobName() + " as " +
              job.getUgi(), e);
          monitor.submissionFailed(job.getJob());
          return;
        }catch (Exception e) {
          LOG.warn("Failed to submit " + job.getJob().getJobName() + " as " +
              job.getUgi(), e);
          monitor.submissionFailed(job.getJob());
          return;
        }
        // Sleep until deadline
        long nsDelay = job.getDelay(TimeUnit.NANOSECONDS);
        while (nsDelay > 0) {
          TimeUnit.NANOSECONDS.sleep(nsDelay);
          nsDelay = job.getDelay(TimeUnit.NANOSECONDS);
        }
        try {
          // submit job
          monitor.add(job.call());
          statistics.addJobStats(job.getJob(), job.getJobDesc());
          LOG.debug("SUBMIT " + job + "@" + System.currentTimeMillis() +
              " (" + job.getJob().getJobID() + ")");
        } catch (IOException e) {
          LOG.warn("Failed to submit " + job.getJob().getJobName() + " as " +
              job.getUgi(), e);
          if (e.getCause() instanceof ClosedByInterruptException) {
            throw new InterruptedException("Failed to submit " +
                job.getJob().getJobName());
          }
          monitor.submissionFailed(job.getJob());
        } catch (ClassNotFoundException e) {
          LOG.warn("Failed to submit " + job.getJob().getJobName(), e);
          monitor.submissionFailed(job.getJob());
        }
      } catch (InterruptedException e) {
        // abort execution, remove splits if nesc
        // TODO release ThdLoc
        GridmixJob.pullDescription(job.id());
        Thread.currentThread().interrupt();
        monitor.submissionFailed(job.getJob());
      } catch(Exception e) {
        //Due to some exception job wasnt submitted.
        LOG.info(" Job " + job.getJob() + " submission failed " , e);
        monitor.submissionFailed(job.getJob());
      } finally {
        sem.release();
      }
    }
  }

  /**
   * Enqueue the job to be submitted per the deadline associated with it.
   */
  public void add(final GridmixJob job) throws InterruptedException {
    final boolean addToQueue = !shutdown;
    if (addToQueue) {
      final SubmitTask task = new SubmitTask(job);
      sem.acquire();
      try {
        sched.execute(task);
      } catch (RejectedExecutionException e) {
        sem.release();
      }
    }
  }

  /**
   * (Re)scan the set of input files from which splits are derived.
   * @throws java.io.IOException
   */
  public void refreshFilePool() throws IOException {
    inputDir.refresh();
  }

  /**
   * Does nothing, as the threadpool is already initialized and waiting for
   * work from the upstream factory.
   */
  public void start() { }

  /**
   * Continue running until all queued jobs have been submitted to the
   * cluster.
   */
  public void join(long millis) throws InterruptedException {
    if (!shutdown) {
      throw new IllegalStateException("Cannot wait for active submit thread");
    }
    sched.awaitTermination(millis, TimeUnit.MILLISECONDS);
  }

  /**
   * Finish all jobs pending submission, but do not accept new work.
   */
  public void shutdown() {
    // complete pending tasks, but accept no new tasks
    shutdown = true;
    sched.shutdown();
  }

  /**
   * Discard pending work, including precomputed work waiting to be
   * submitted.
   */
  public void abort() {
    //pendingJobs.clear();
    shutdown = true;
    sched.shutdownNow();
  }
}
