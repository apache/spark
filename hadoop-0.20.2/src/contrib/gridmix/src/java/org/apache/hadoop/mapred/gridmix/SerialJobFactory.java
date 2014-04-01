/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.mapred.gridmix.Statistics.JobStats;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;

public class SerialJobFactory extends JobFactory<JobStats> {

  public static final Log LOG = LogFactory.getLog(SerialJobFactory.class);
  private final Condition jobCompleted = lock.newCondition();

  /**
   * Creating a new instance does not start the thread.
   *
   * @param submitter   Component to which deserialized jobs are passed
   * @param jobProducer Job story producer
   *                    {@link org.apache.hadoop.tools.rumen.ZombieJobProducer}
   * @param scratch     Directory into which to write output from simulated jobs
   * @param conf        Config passed to all jobs to be submitted
   * @param startFlag   Latch released from main to start pipeline
   * @throws java.io.IOException
   */
  public SerialJobFactory(
    JobSubmitter submitter, JobStoryProducer jobProducer, Path scratch,
    Configuration conf, CountDownLatch startFlag, UserResolver resolver)
    throws IOException {
    super(submitter, jobProducer, scratch, conf, startFlag, resolver);
  }

  @Override
  public Thread createReaderThread() {
    return new SerialReaderThread("SerialJobFactory");
  }

  private class SerialReaderThread extends Thread {

    public SerialReaderThread(String threadName) {
      super(threadName);
    }

    /**
     * SERIAL : In this scenario .  method waits on notification ,
     * that a submitted job is actually completed. Logic is simple.
     * ===
     * while(true) {
     * wait till previousjob is completed.
     * break;
     * }
     * submit newJob.
     * previousJob = newJob;
     * ==
     */
    @Override
    public void run() {
      try {
        startFlag.await();
        if (Thread.currentThread().isInterrupted()) {
          return;
        }
        LOG.info("START SERIAL @ " + System.currentTimeMillis());
        GridmixJob prevJob;
        while (!Thread.currentThread().isInterrupted()) {
          final JobStory job;
          try {
            job = getNextJobFiltered();
            if (null == job) {
              return;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "Serial mode submitting job " + job.getName());
            }
            prevJob = jobCreator.createGridmixJob(
              conf, 0L, job, scratch, userResolver.getTargetUgi(
                UserGroupInformation.createRemoteUser(job.getUser())),
              sequence.getAndIncrement());

            lock.lock();
            try {
              LOG.info(" Submitted the job " + prevJob);
              submitter.add(prevJob);
            } finally {
              lock.unlock();
            }
          } catch (IOException e) {
            error = e;
            //If submission of current job fails , try to submit the next job.
            return;
          }

          if (prevJob != null) {
            //Wait till previous job submitted is completed.
            lock.lock();
            try {
              while (true) {
                try {
                  jobCompleted.await();
                } catch (InterruptedException ie) {
                  LOG.error(
                    " Error in SerialJobFactory while waiting for job completion ",
                    ie);
                  return;
                }
                if (LOG.isDebugEnabled()) {
                  LOG.info(" job " + job.getName() + " completed ");
                }
                break;
              }
            } finally {
              lock.unlock();
            }
            prevJob = null;
          }
        }
      } catch (InterruptedException e) {
        return;
      } finally {
        IOUtils.cleanup(null, jobProducer);
      }
    }

  }

  /**
   * SERIAL. Once you get notification from StatsCollector about the job
   * completion ,simply notify the waiting thread.
   *
   * @param item
   */
  @Override
  public void update(Statistics.JobStats item) {
    //simply notify in case of serial submissions. We are just bothered
    //if submitted job is completed or not.
    lock.lock();
    try {
      jobCompleted.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Start the reader thread, wait for latch if necessary.
   */
  @Override
  public void start() {
    LOG.info(" Starting Serial submission ");
    this.rThread.start();
  }
}
