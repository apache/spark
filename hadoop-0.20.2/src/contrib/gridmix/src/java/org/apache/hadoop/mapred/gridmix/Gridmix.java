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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Driver class for the Gridmix3 benchmark. Gridmix accepts a timestamped
 * stream (trace) of job/task descriptions. For each job in the trace, the
 * client will submit a corresponding, synthetic job to the target cluster at
 * the rate in the original trace. The intent is to provide a benchmark that
 * can be configured and extended to closely match the measured resource
 * profile of actual, production loads.
 */
public class Gridmix extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(Gridmix.class);

  /**
   * Output (scratch) directory for submitted jobs. Relative paths are
   * resolved against the path provided as input and absolute paths remain
   * independent of it. The default is &quot;gridmix&quot;.
   */
  public static final String GRIDMIX_OUT_DIR = "gridmix.output.directory";

  /**
   * Number of submitting threads at the client and upper bound for
   * in-memory split data. Submitting threads precompute InputSplits for
   * submitted jobs. This limits the number of splits held in memory waiting
   * for submission and also permits parallel computation of split data.
   */
  public static final String GRIDMIX_SUB_THR = "gridmix.client.submit.threads";

  /**
   * The depth of the queue of job descriptions. Before splits are computed,
   * a queue of pending descriptions is stored in memoory. This parameter
   * limits the depth of that queue.
   */
  public static final String GRIDMIX_QUE_DEP =
    "gridmix.client.pending.queue.depth";

  /**
   * Multiplier to accelerate or decelerate job submission. As a crude means of
   * sizing a job trace to a cluster, the time separating two jobs is
   * multiplied by this factor.
   */
  public static final String GRIDMIX_SUB_MUL = "gridmix.submit.multiplier";

  /**
   * Class used to resolve users in the trace to the list of target users
   * on the cluster.
   */
  public static final String GRIDMIX_USR_RSV = "gridmix.user.resolve.class";

  // Submit data structures
  private JobFactory factory;
  private JobSubmitter submitter;
  private JobMonitor monitor;
  private Statistics statistics;

  // Shutdown hook
  private final Shutdown sdh = new Shutdown();

  /**
   * Write random bytes at the path provided.
   * @see org.apache.hadoop.mapred.gridmix.GenerateData
   */
  protected void writeInputData(long genbytes, Path ioPath)
      throws IOException, InterruptedException {
    final Configuration conf = getConf();
    final GridmixJob genData = new GenerateData(conf, ioPath, genbytes);
    submitter.add(genData);
    LOG.info("Generating " + StringUtils.humanReadableInt(genbytes) +
        " of test data...");
    // TODO add listeners, use for job dependencies
    TimeUnit.SECONDS.sleep(10);
    try {
      genData.getJob().waitForCompletion(false);
    } catch (ClassNotFoundException e) {
      throw new IOException("Internal error", e);
    }
    if (!genData.getJob().isSuccessful()) {
      throw new IOException("Data generation failed!");
    }

    FsShell shell = new FsShell(conf);
    try {
      LOG.info("Changing the permissions for inputPath " + ioPath.toString());
      shell.run(new String[] {"-chmod","-R","777", ioPath.toString()});
    } catch (Exception e) {
      LOG.error("Couldnt change the file permissions " , e);
      throw new IOException(e);
    }
    LOG.info("Done.");
  }

  protected InputStream createInputStream(String in) throws IOException {
    if ("-".equals(in)) {
      return System.in;
    }
    final Path pin = new Path(in);
    return pin.getFileSystem(getConf()).open(pin);
  }

  /**
   * Create each component in the pipeline and start it.
   * @param conf Configuration data, no keys specific to this context
   * @param traceIn Either a Path to the trace data or &quot;-&quot; for
   *                stdin
   * @param ioPath Path from which input data is read
   * @param scratchDir Path into which job output is written
   * @param startFlag Semaphore for starting job trace pipeline
   */
  private void startThreads(Configuration conf, String traceIn, Path ioPath,
      Path scratchDir, CountDownLatch startFlag, UserResolver userResolver)
      throws IOException {
    try {
      GridmixJobSubmissionPolicy policy = GridmixJobSubmissionPolicy.getPolicy(
        conf, GridmixJobSubmissionPolicy.STRESS);
      LOG.info(" Submission policy is " + policy.name());
      statistics = new Statistics(conf, policy.getPollingInterval(), startFlag);
      monitor = createJobMonitor(statistics);
      int noOfSubmitterThreads = (policy == GridmixJobSubmissionPolicy.SERIAL) ? 1
          : Runtime.getRuntime().availableProcessors() + 1;

      submitter = createJobSubmitter(
        monitor, conf.getInt(
          GRIDMIX_SUB_THR, noOfSubmitterThreads), conf.getInt(
          GRIDMIX_QUE_DEP, 5), new FilePool(
          conf, ioPath), userResolver,statistics);
      
      factory = createJobFactory(
        submitter, traceIn, scratchDir, conf, startFlag, userResolver);
      if (policy==GridmixJobSubmissionPolicy.SERIAL) {
        statistics.addJobStatsListeners(factory);
      } else {
        statistics.addClusterStatsObservers(factory);
      }
      
      monitor.start();
      submitter.start();
    }catch(Exception e) {
      LOG.error(" Exception at start " ,e);
      throw new IOException(e);
    }
  }

  protected JobMonitor createJobMonitor(Statistics stats) throws IOException {
    return new JobMonitor(stats);
  }

  protected JobSubmitter createJobSubmitter(
    JobMonitor monitor, int threads, int queueDepth, FilePool pool,
    UserResolver resolver, Statistics statistics) throws IOException {
    return new JobSubmitter(monitor, threads, queueDepth, pool, statistics);
  }

  protected JobFactory createJobFactory(
    JobSubmitter submitter, String traceIn, Path scratchDir, Configuration conf,
    CountDownLatch startFlag, UserResolver resolver)
    throws IOException {
    return GridmixJobSubmissionPolicy.getPolicy(
      conf, GridmixJobSubmissionPolicy.STRESS).createJobFactory(
      submitter, new ZombieJobProducer(
        createInputStream(
          traceIn), null), scratchDir, conf, startFlag, resolver);
  }

  public int run(final String[] argv) throws IOException, InterruptedException {
    int val = -1;
    final Configuration conf = getConf();
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();

    val = ugi.doAs(new PrivilegedExceptionAction<Integer>() {
      public Integer run() throws Exception {
        return runJob(conf,argv);
      }
    });
    return val; 
  }

  private static UserResolver userResolver;

  public UserResolver getCurrentUserResolver() {
    return userResolver;
  }

  private int runJob(Configuration conf, String[] argv)
    throws IOException, InterruptedException {
    if (argv.length < 2) {
      printUsage(System.err);
      return 1;
    }
    long genbytes = -1L;
    String traceIn = null;
    Path ioPath = null;
    URI userRsrc = null;
    userResolver = ReflectionUtils.newInstance(
        conf.getClass(GRIDMIX_USR_RSV, SubmitterUserResolver.class,
          UserResolver.class), conf);
    try {
      for (int i = 0; i < argv.length - 2; ++i) {
        if ("-generate".equals(argv[i])) {
          genbytes = StringUtils.TraditionalBinaryPrefix.string2long(argv[++i]);
        } else if ("-users".equals(argv[i])) {
          userRsrc = new URI(argv[++i]);
        } else {
          printUsage(System.err);
          return 1;
        }
      }
      if (!userResolver.setTargetUsers(userRsrc, conf)) {
        LOG.warn("Resource " + userRsrc + " ignored");
      }
      ioPath = new Path(argv[argv.length - 2]);
      traceIn = argv[argv.length - 1];
    } catch (Exception e) {
      e.printStackTrace();
      printUsage(System.err);
      return 1;
    }
    return start(conf, traceIn, ioPath, genbytes, userResolver);
  }

  int start(Configuration conf, String traceIn, Path ioPath, long genbytes,
      UserResolver userResolver) throws IOException, InterruptedException {
    InputStream trace = null;
    try {
      Path scratchDir = new Path(ioPath, conf.get(GRIDMIX_OUT_DIR, "gridmix"));
      final FileSystem scratchFs = scratchDir.getFileSystem(conf);
      scratchFs.mkdirs(scratchDir, new FsPermission((short) 0777));
      scratchFs.setPermission(scratchDir, new FsPermission((short) 0777));
      // add shutdown hook for SIGINT, etc.
      Runtime.getRuntime().addShutdownHook(sdh);
      CountDownLatch startFlag = new CountDownLatch(1);
      try {
        // Create, start job submission threads
        startThreads(conf, traceIn, ioPath, scratchDir, startFlag,
            userResolver);
        // Write input data if specified
        if (genbytes > 0) {
          writeInputData(genbytes, ioPath);
        }
        // scan input dir contents
        submitter.refreshFilePool();
        factory.start();
        statistics.start();
      } catch (Throwable e) {
        LOG.error("Startup failed", e);
        if (factory != null) factory.abort(); // abort pipeline
      } finally {
        // signal for factory to start; sets start time
        startFlag.countDown();
      }
      if (factory != null) {
        // wait for input exhaustion
        factory.join(Long.MAX_VALUE);
        final Throwable badTraceException = factory.error();
        if (null != badTraceException) {
          LOG.error("Error in trace", badTraceException);
          throw new IOException("Error in trace", badTraceException);
        }
        // wait for pending tasks to be submitted
        submitter.shutdown();
        submitter.join(Long.MAX_VALUE);
        // wait for running tasks to complete
        monitor.shutdown();
        monitor.join(Long.MAX_VALUE);

        statistics.shutdown();
        statistics.join(Long.MAX_VALUE);

      }
    } finally {
      IOUtils.cleanup(LOG, trace);
    }
    return 0;
  }

  /**
   * Handles orderly shutdown by requesting that each component in the
   * pipeline abort its progress, waiting for each to exit and killing
   * any jobs still running on the cluster.
   */
  class Shutdown extends Thread {

    static final long FAC_SLEEP = 1000;
    static final long SUB_SLEEP = 4000;
    static final long MON_SLEEP = 15000;

    private void killComponent(Component<?> component, long maxwait) {
      if (component == null) {
        return;
      }
      component.abort();
      try {
        component.join(maxwait);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted waiting for " + component);
      }

    }

    @Override
    public void run() {
      LOG.info("Exiting...");
      try {
        killComponent(factory, FAC_SLEEP);   // read no more tasks
        killComponent(submitter, SUB_SLEEP); // submit no more tasks
        killComponent(monitor, MON_SLEEP);   // process remaining jobs here
        killComponent(statistics,MON_SLEEP);
      } finally {
        if (monitor == null) {
          return;
        }
        List<Job> remainingJobs = monitor.getRemainingJobs();
        if (remainingJobs.isEmpty()) {
          return;
        }
        LOG.info("Killing running jobs...");
        for (Job job : remainingJobs) {
          try {
            if (!job.isComplete()) {
              job.killJob();
              LOG.info("Killed " + job.getJobName() + " (" +
                  job.getJobID() + ")");
            } else {
              if (job.isSuccessful()) {
                monitor.onSuccess(job);
              } else {
                monitor.onFailure(job);
              }
            }
          } catch (IOException e) {
            LOG.warn("Failure killing " + job.getJobName(), e);
          } catch (Exception e) {
            LOG.error("Unexcpected exception", e);
          }
        }
        LOG.info("Done.");
      }
    }

  }

  public static void main(String[] argv) throws Exception {
    int res = -1;
    try {
      res = ToolRunner.run(new Configuration(), new Gridmix(), argv);
    } finally {
      System.exit(res);
    }
  }

  private <T> String getEnumValues(Enum<? extends T>[] e) {
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (Enum<? extends T> v : e) {
      sb.append(sep);
      sb.append(v.name());
      sep = "|";
    }
    return sb.toString();
  }
  
  private String getJobTypes() {
    return getEnumValues(JobCreator.values());
  }
  
  private String getSubmissionPolicies() {
    return getEnumValues(GridmixJobSubmissionPolicy.values());
  }
  
  protected void printUsage(PrintStream out) {
    ToolRunner.printGenericCommandUsage(out);
    out.println("Usage: gridmix [-generate <MiB>] [-users URI] [-Dname=value ...] <iopath> <trace>");
    out.println("  e.g. gridmix -generate 100m foo -");
    out.println("Configuration parameters:");
    out.println("   General parameters:");
    out.printf("       %-48s : Output directory\n", GRIDMIX_OUT_DIR);
    out.printf("       %-48s : Submitting threads\n", GRIDMIX_SUB_THR);
    out.printf("       %-48s : Queued job desc\n", GRIDMIX_QUE_DEP);
    out.printf("       %-48s : User resolution class\n", GRIDMIX_USR_RSV);
    out.printf("       %-48s : Job types (%s)\n", JobCreator.GRIDMIX_JOB_TYPE, getJobTypes());
    out.println("   Parameters related to job submission:");    
    out.printf("       %-48s : Default queue\n",
        GridmixJob.GRIDMIX_DEFAULT_QUEUE);
    out.printf("       %-48s : Enable/disable using queues in trace\n",
        GridmixJob.GRIDMIX_USE_QUEUE_IN_TRACE);
    out.printf("       %-48s : Job submission policy (%s)\n",
        GridmixJobSubmissionPolicy.JOB_SUBMISSION_POLICY, getSubmissionPolicies());
    out.println("   Parameters specific for LOADJOB:");
    out.printf("       %-48s : Key fraction of rec\n",
        AvgRecordFactory.GRIDMIX_KEY_FRC);
    out.println("   Parameters specific for SLEEPJOB:");
    out.printf("       %-48s : Whether to ignore reduce tasks\n",
        SleepJob.SLEEPJOB_MAPTASK_ONLY);
    out.printf("       %-48s : Number of fake locations for map tasks\n",
        JobCreator.SLEEPJOB_RANDOM_LOCATIONS);
    out.printf("       %-48s : Maximum map task runtime in mili-sec\n",
        SleepJob.GRIDMIX_SLEEP_MAX_MAP_TIME);
    out.printf("       %-48s : Maximum reduce task runtime in mili-sec (merge+reduce)\n",
        SleepJob.GRIDMIX_SLEEP_MAX_REDUCE_TIME);
    out.println("   Parameters specific for STRESS submission throttling policy:");
    out.printf("       %-48s : jobs vs task-tracker ratio\n",
        StressJobFactory.CONF_MAX_JOB_TRACKER_RATIO);
    out.printf("       %-48s : maps vs map-slot ratio\n",
        StressJobFactory.CONF_OVERLOAD_MAPTASK_MAPSLOT_RATIO);
    out.printf("       %-48s : reduces vs reduce-slot ratio\n",
        StressJobFactory.CONF_OVERLOAD_REDUCETASK_REDUCESLOT_RATIO);
    out.printf("       %-48s : map-slot share per job\n",
        StressJobFactory.CONF_MAX_MAPSLOT_SHARE_PER_JOB);
    out.printf("       %-48s : reduce-slot share per job\n",
        StressJobFactory.CONF_MAX_REDUCESLOT_SHARE_PER_JOB);
  }

  /**
   * Components in the pipeline must support the following operations for
   * orderly startup and shutdown.
   */
  interface Component<T> {

    /**
     * Accept an item into this component from an upstream component. If
     * shutdown or abort have been called, this may fail, depending on the
     * semantics for the component.
     */
    void add(T item) throws InterruptedException;

    /**
     * Attempt to start the service.
     */
    void start();

    /**
     * Wait until the service completes. It is assumed that either a
     * {@link #shutdown} or {@link #abort} has been requested.
     */
    void join(long millis) throws InterruptedException;

    /**
     * Shut down gracefully, finishing all pending work. Reject new requests.
     */
    void shutdown();

    /**
     * Shut down immediately, aborting any work in progress and discarding
     * all pending work. It is legal to store pending work for another
     * thread to process.
     */
    void abort();
  }

}
