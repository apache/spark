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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.streaming.StreamJob;

public class GridMixRunner {

  private static final int NUM_OF_LARGE_JOBS_PER_CLASS = 0;
  private static final int NUM_OF_MEDIUM_JOBS_PER_CLASS = 0;
  private static final int NUM_OF_SMALL_JOBS_PER_CLASS = 0;

  private static final int NUM_OF_REDUCERS_FOR_SMALL_JOB = 15;
  private static final int NUM_OF_REDUCERS_FOR_MEDIUM_JOB = 170;
  private static final int NUM_OF_REDUCERS_FOR_LARGE_JOB = 370;

  private static final String GRID_MIX_DATA = "/gridmix/data";
  private static final String VARCOMPSEQ =
    GRID_MIX_DATA + "/WebSimulationBlockCompressed";
  private static final String FIXCOMPSEQ =
    GRID_MIX_DATA + "/MonsterQueryBlockCompressed";
  private static final String VARINFLTEXT =
    GRID_MIX_DATA + "/SortUncompressed";

  private static final String GRIDMIXCONFIG = "gridmix_config.xml";

  private static final Configuration config = initConfig();
  private static final FileSystem fs = initFs();
  private final JobControl gridmix;
  private int numOfJobs = 0;

  private enum Size {
    SMALL("small",                               // name
          "/{part-00000,part-00001,part-00002}", // default input subset
          NUM_OF_SMALL_JOBS_PER_CLASS,           // defuault num jobs
          NUM_OF_REDUCERS_FOR_SMALL_JOB),        // default num reducers
    MEDIUM("medium",                             // name
          "/{part-000*0,part-000*1,part-000*2}", // default input subset
          NUM_OF_MEDIUM_JOBS_PER_CLASS,          // defuault num jobs
          NUM_OF_REDUCERS_FOR_MEDIUM_JOB),       // default num reducers
    LARGE("large",                               // name
          "",                                    // default input subset
          NUM_OF_LARGE_JOBS_PER_CLASS,           // defuault num jobs
          NUM_OF_REDUCERS_FOR_LARGE_JOB);        // default num reducers

    private final String str;
    private final String path;
    private final int numJobs;
    private final int numReducers;
    Size(String str, String path, int numJobs, int numReducers) {
      this.str = str;
      this.path = path;
      this.numJobs = numJobs;
      this.numReducers = numReducers;
    }
    public String defaultPath(String base) {
      return base + path;
    }
    public int defaultNumJobs() {
      return numJobs;
    }
    public int defaultNumReducers() {
      return numReducers;
    }
    public String toString() {
      return str;
    }
  }

  private enum GridMixJob {
    STREAMSORT("streamSort") {
    public void addJob(int numReducers, boolean mapoutputCompressed,
        boolean outputCompressed, Size size, JobControl gridmix) {
      final String prop = String.format("streamSort.%sJobs.inputFiles", size);
      final String indir = getInputDirsFor(prop, size.defaultPath(VARINFLTEXT));
      final String outdir = addTSSuffix("perf-out/stream-out-dir-" + size);

      StringBuffer sb = new StringBuffer();
      sb.append("-input ").append(indir).append(" ");
      sb.append("-output ").append(outdir).append(" ");
      sb.append("-mapper cat ");
      sb.append("-reducer cat ");
      sb.append("-numReduceTasks ").append(numReducers);
      String[] args = sb.toString().split(" ");

      clearDir(outdir);
      try {
        JobConf jobconf = StreamJob.createJob(args);
        jobconf.setJobName("GridmixStreamingSorter." + size);
        jobconf.setCompressMapOutput(mapoutputCompressed);
        jobconf.setBoolean("mapred.output.compress", outputCompressed);
        Job job = new Job(jobconf);
        gridmix.addJob(job);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    },

    JAVASORT("javaSort") {
    public void addJob(int numReducers, boolean mapoutputCompressed,
        boolean outputCompressed, Size size, JobControl gridmix) {
      final String prop = String.format("javaSort.%sJobs.inputFiles", size);
      final String indir = getInputDirsFor(prop, size.defaultPath(VARINFLTEXT));
      final String outdir = addTSSuffix("perf-out/sort-out-dir-" + size);

      clearDir(outdir);

      try {
        JobConf jobConf = new JobConf();
        jobConf.setJarByClass(Sort.class);
        jobConf.setJobName("GridmixJavaSorter." + size);
        jobConf.setMapperClass(IdentityMapper.class);
        jobConf.setReducerClass(IdentityReducer.class);

        jobConf.setNumReduceTasks(numReducers);
        jobConf.setInputFormat(org.apache.hadoop.mapred.KeyValueTextInputFormat.class);
        jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);

        jobConf.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        jobConf.setOutputValueClass(org.apache.hadoop.io.Text.class);
        jobConf.setCompressMapOutput(mapoutputCompressed);
        jobConf.setBoolean("mapred.output.compress", outputCompressed);

        FileInputFormat.addInputPaths(jobConf, indir);
        FileOutputFormat.setOutputPath(jobConf, new Path(outdir));

        Job job = new Job(jobConf);
        gridmix.addJob(job);

      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    },

    WEBDATASCAN("webdataScan") {
    public void addJob(int numReducers, boolean mapoutputCompressed,
        boolean outputCompressed, Size size, JobControl gridmix) {
      final String prop = String.format("webdataScan.%sJobs.inputFiles", size);
      final String indir = getInputDirsFor(prop, size.defaultPath(VARCOMPSEQ));
      final String outdir = addTSSuffix("perf-out/webdata-scan-out-dir-" + size);
      StringBuffer sb = new StringBuffer();
      sb.append("-keepmap 0.2 ");
      sb.append("-keepred 5 ");
      sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
      sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
      sb.append("-outKey org.apache.hadoop.io.Text ");
      sb.append("-outValue org.apache.hadoop.io.Text ");
      sb.append("-indir ").append(indir).append(" ");
      sb.append("-outdir ").append(outdir).append(" ");
      sb.append("-r ").append(numReducers);

      String[] args = sb.toString().split(" ");
      clearDir(outdir);
      try {
        JobConf jobconf = GenericMRLoadJobCreator.createJob(
            args, mapoutputCompressed, outputCompressed);
        jobconf.setJobName("GridmixWebdatascan." + size);
        Job job = new Job(jobconf);
        gridmix.addJob(job);
      } catch (Exception ex) {
        System.out.println(ex.getStackTrace());
      }
    }
    },

    COMBINER("combiner") {
    public void addJob(int numReducers, boolean mapoutputCompressed,
        boolean outputCompressed, Size size, JobControl gridmix) {
      final String prop = String.format("combiner.%sJobs.inputFiles", size);
      final String indir = getInputDirsFor(prop, size.defaultPath(VARCOMPSEQ));
      final String outdir = addTSSuffix("perf-out/combiner-out-dir-" + size);

      StringBuffer sb = new StringBuffer();
      sb.append("-r ").append(numReducers).append(" ");
      sb.append("-indir ").append(indir).append(" ");
      sb.append("-outdir ").append(outdir);
      sb.append("-mapoutputCompressed ").append(mapoutputCompressed).append(" ");
      sb.append("-outputCompressed ").append(outputCompressed);

      String[] args = sb.toString().split(" ");
      clearDir(outdir);
      try {
        JobConf jobconf = CombinerJobCreator.createJob(args);
        jobconf.setJobName("GridmixCombinerJob." + size);
        Job job = new Job(jobconf);
        gridmix.addJob(job);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    },

    MONSTERQUERY("monsterQuery") {
    public void addJob(int numReducers, boolean mapoutputCompressed,
        boolean outputCompressed, Size size, JobControl gridmix) {
      final String prop = String.format("monsterQuery.%sJobs.inputFiles", size);
      final String indir = getInputDirsFor(prop, size.defaultPath(FIXCOMPSEQ));
      final String outdir = addTSSuffix("perf-out/mq-out-dir-" + size);
      int iter = 3;
      try {
        Job pjob = null;
        Job job = null;
        for (int i = 0; i < iter; i++) {
          String outdirfull = outdir + "." + i;
          String indirfull = (0 == i) ? indir : outdir + "." + (i - 1);
          Path outfile = new Path(outdirfull);

          StringBuffer sb = new StringBuffer();
          sb.append("-keepmap 10 ");
          sb.append("-keepred 40 ");
          sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
          sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
          sb.append("-outKey org.apache.hadoop.io.Text ");
          sb.append("-outValue org.apache.hadoop.io.Text ");
          sb.append("-indir ").append(indirfull).append(" ");
          sb.append("-outdir ").append(outdirfull).append(" ");
          sb.append("-r ").append(numReducers);
          String[] args = sb.toString().split(" ");

          try {
            fs.delete(outfile);
          } catch (IOException ex) {
            System.out.println(ex.toString());
          }

          JobConf jobconf = GenericMRLoadJobCreator.createJob(
              args, mapoutputCompressed, outputCompressed);
          jobconf.setJobName("GridmixMonsterQuery." + size);
          job = new Job(jobconf);
          if (pjob != null) {
            job.addDependingJob(pjob);
          }
          gridmix.addJob(job);
          pjob = job;
        }
      } catch (Exception e) {
        System.out.println(e.getStackTrace());
      }
    }
    },

    WEBDATASORT("webdataSort") {
    public void addJob(int numReducers, boolean mapoutputCompressed,
        boolean outputCompressed, Size size, JobControl gridmix) {
      final String prop = String.format("webdataSort.%sJobs.inputFiles", size);
      final String indir = getInputDirsFor(prop, size.defaultPath(VARCOMPSEQ));
      final String outdir = addTSSuffix("perf-out/webdata-sort-out-dir-" + size);

      StringBuffer sb = new StringBuffer();
      sb.append("-keepmap 100 ");
      sb.append("-keepred 100 ");
      sb.append("-inFormat org.apache.hadoop.mapred.SequenceFileInputFormat ");
      sb.append("-outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat ");
      sb.append("-outKey org.apache.hadoop.io.Text ");
      sb.append("-outValue org.apache.hadoop.io.Text ");
      sb.append("-indir ").append(indir).append(" ");
      sb.append("-outdir ").append(outdir).append(" ");
      sb.append("-r ").append(numReducers);

      String[] args = sb.toString().split(" ");
      clearDir(outdir);
      try {
        JobConf jobconf = GenericMRLoadJobCreator.createJob(
            args, mapoutputCompressed, outputCompressed);
        jobconf.setJobName("GridmixWebdataSort." + size);
        Job job = new Job(jobconf);
        gridmix.addJob(job);
      } catch (Exception ex) {
        System.out.println(ex.getStackTrace());
      }
    }
    };

    private final String name;
    GridMixJob(String name) {
      this.name = name;
    }
    public String getName() {
      return name;
    }
    public abstract void addJob(int numReducers, boolean mapComp,
        boolean outComp, Size size, JobControl gridmix);
  }

  public GridMixRunner() throws IOException {
    gridmix = new JobControl("GridMix");
    if (null == config || null == fs) {
      throw new IOException("Bad configuration. Cannot continue.");
    }
  }

  private static FileSystem initFs() {
    try {
      return FileSystem.get(config);
    } catch (Exception e) {
      System.out.println("fs initation error: " + e.getMessage());
    }
    return null;
  }

  private static Configuration initConfig() {
    Configuration conf = new Configuration();
    String configFile = System.getenv("GRIDMIXCONFIG");
    if (configFile == null) {
      String configDir = System.getProperty("user.dir");
      if (configDir == null) {
        configDir = ".";
      }
      configFile = configDir + "/" + GRIDMIXCONFIG;
    }
    try {
      Path fileResource = new Path(configFile);
      conf.addResource(fileResource);
    } catch (Exception e) {
      System.err.println("Error reading config file " + configFile + ":" +
          e.getMessage());
      return null;
    }
    return conf;
  }

  private static int[] getInts(Configuration conf, String name, int defaultV) {
    String[] vals = conf.getStrings(name, String.valueOf(defaultV));
    int[] results = new int[vals.length];
    for (int i = 0; i < vals.length; ++i) {
      results[i] = Integer.parseInt(vals[i]);
    }
    return results;
  }

  private static String getInputDirsFor(String jobType, String defaultIndir) {
    String inputFile[] = config.getStrings(jobType, defaultIndir);
    StringBuffer indirBuffer = new StringBuffer();
    for (int i = 0; i < inputFile.length; i++) {
      indirBuffer = indirBuffer.append(inputFile[i]).append(",");
    }
    return indirBuffer.substring(0, indirBuffer.length() - 1);
  }

  private static void clearDir(String dir) {
    try {
      Path outfile = new Path(dir);
      fs.delete(outfile);
    } catch (IOException ex) {
      ex.printStackTrace();
      System.out.println("delete file error:");
      System.out.println(ex.toString());
    }
  }

  private boolean select(int total, int selected, int index) {
    if (selected <= 0 || selected >= total) {
      return selected > 0;
    }
    int step = total / selected;
    int effectiveTotal = total - total % selected;
    return (index <= effectiveTotal - 1 && (index % step == 0));
  }

  private static String addTSSuffix(String s) {
    Date date = Calendar.getInstance().getTime();
    String ts = String.valueOf(date.getTime());
    return s + "_" + ts;
  }

  private void addJobs(GridMixJob job, Size size) throws IOException {
    final String prefix = String.format("%s.%sJobs", job.getName(), size);
    int[] numJobs = getInts(config, prefix + ".numOfJobs",
        size.defaultNumJobs());
    int[] numReduces = getInts(config, prefix + ".numOfReduces",
        size.defaultNumReducers());
    if (numJobs.length != numReduces.length) {
      throw new IOException("Configuration error: " +
          prefix + ".numOfJobs must match " +
          prefix + ".numOfReduces");
    }
    int numMapoutputCompressed = config.getInt(
        prefix + ".numOfMapoutputCompressed", 0);
    int numOutputCompressed = config.getInt(
        prefix + ".numOfOutputCompressed", size.defaultNumJobs());
    int totalJobs = 0;
    for (int nJob : numJobs) {
      totalJobs += nJob;
    }
    int currentIndex = 0;
    for (int i = 0; i < numJobs.length; ++i) {
      for (int j = 0; j < numJobs[i]; ++j) {
        boolean mapoutputComp =
          select(totalJobs, numMapoutputCompressed, currentIndex);
        boolean outputComp =
          select(totalJobs, numOutputCompressed, currentIndex);
        job.addJob(numReduces[i], mapoutputComp, outputComp, size, gridmix);
        ++numOfJobs;
        ++currentIndex;
      }
    }
  }

  private void addAllJobs(GridMixJob job) throws IOException {
    for (Size size : EnumSet.allOf(Size.class)) {
      addJobs(job, size);
    }
  }

  public void addjobs() throws IOException {
    for (GridMixJob jobtype : EnumSet.allOf(GridMixJob.class)) {
      addAllJobs(jobtype);
    }
    System.out.println("total " + gridmix.getWaitingJobs().size() + " jobs");
  }

  class SimpleStats {
    long minValue;
    long maxValue;
    long averageValue;
    long mediumValue;
    int n;

    SimpleStats(long[] data) {
      Arrays.sort(data);
      n = data.length;
      minValue = data[0];
      maxValue = data[n - 1];
      mediumValue = data[n / 2];
      long total = 0;
      for (int i = 0; i < n; i++) {
        total += data[i];
      }
      averageValue = total / n;
    }
  }

  class TaskExecutionStats {
    TreeMap<String, SimpleStats> theStats;

    void computeStats(String name, long[] data) {
      SimpleStats v = new SimpleStats(data);
      theStats.put(name, v);
    }

    TaskExecutionStats() {
      theStats = new TreeMap<String, SimpleStats>();
    }
  }

  private TreeMap<String, String> getStatForJob(Job job) {
    TreeMap<String, String> retv = new TreeMap<String, String>();
    String mapreduceID = job.getAssignedJobID().toString();
    JobClient jc = job.getJobClient();
    JobConf jobconf = job.getJobConf();
    String jobName = jobconf.getJobName();
    retv.put("JobId", mapreduceID);
    retv.put("JobName", jobName);

    TaskExecutionStats theTaskExecutionStats = new TaskExecutionStats();

    try {
      RunningJob running = jc.getJob(JobID.forName(mapreduceID));
      Counters jobCounters = running.getCounters();
      Iterator<Group> groups = jobCounters.iterator();
      while (groups.hasNext()) {
        Group g = groups.next();
        String gn = g.getName();
        Iterator<Counters.Counter> cs = g.iterator();
        while (cs.hasNext()) {
          Counters.Counter c = cs.next();
          String n = c.getName();
          long v = c.getCounter();
          retv.put(mapreduceID + "." + jobName + "." + gn + "." + n, "" + v);
        }
      }
      TaskReport[] maps = jc.getMapTaskReports(JobID.forName(mapreduceID));
      TaskReport[] reduces = jc
          .getReduceTaskReports(JobID.forName(mapreduceID));
      retv.put(mapreduceID + "." + jobName + "." + "numOfMapTasks", ""
          + maps.length);
      retv.put(mapreduceID + "." + jobName + "." + "numOfReduceTasks", ""
          + reduces.length);
      long[] mapExecutionTimes = new long[maps.length];
      long[] reduceExecutionTimes = new long[reduces.length];
      Date date = Calendar.getInstance().getTime();
      long startTime = date.getTime();
      long finishTime = 0;
      for (int j = 0; j < maps.length; j++) {
        TaskReport map = maps[j];
        long thisStartTime = map.getStartTime();
        long thisFinishTime = map.getFinishTime();
        if (thisStartTime > 0 && thisFinishTime > 0) {
          mapExecutionTimes[j] = thisFinishTime - thisStartTime;
        }
        if (startTime > thisStartTime) {
          startTime = thisStartTime;
        }
        if (finishTime < thisFinishTime) {
          finishTime = thisFinishTime;
        }
      }

      theTaskExecutionStats.computeStats("mapExecutionTimeStats",
          mapExecutionTimes);

      retv.put(mapreduceID + "." + jobName + "." + "mapStartTime", ""
          + startTime);
      retv.put(mapreduceID + "." + jobName + "." + "mapEndTime", ""
          + finishTime);
      for (int j = 0; j < reduces.length; j++) {
        TaskReport reduce = reduces[j];
        long thisStartTime = reduce.getStartTime();
        long thisFinishTime = reduce.getFinishTime();
        if (thisStartTime > 0 && thisFinishTime > 0) {
          reduceExecutionTimes[j] = thisFinishTime - thisStartTime;
        }
        if (startTime > thisStartTime) {
          startTime = thisStartTime;
        }
        if (finishTime < thisFinishTime) {
          finishTime = thisFinishTime;
        }
      }

      theTaskExecutionStats.computeStats("reduceExecutionTimeStats",
          reduceExecutionTimes);

      retv.put(mapreduceID + "." + jobName + "." + "reduceStartTime", ""
          + startTime);
      retv.put(mapreduceID + "." + jobName + "." + "reduceEndTime", ""
          + finishTime);
      if (job.getState() == Job.SUCCESS) {
        retv.put(mapreduceID + "." + "jobStatus", "successful");
      } else if (job.getState() == Job.FAILED) {
        retv.put(mapreduceID + "." + jobName + "." + "jobStatus", "failed");
      } else {
        retv.put(mapreduceID + "." + jobName + "." + "jobStatus", "unknown");
      }
      Iterator<Entry<String, SimpleStats>> entries = theTaskExecutionStats.theStats
          .entrySet().iterator();
      while (entries.hasNext()) {
        Entry<String, SimpleStats> e = entries.next();
        SimpleStats v = e.getValue();
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "." + "min",
            "" + v.minValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "." + "max",
            "" + v.maxValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "."
            + "medium", "" + v.mediumValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "." + "avg",
            "" + v.averageValue);
        retv.put(mapreduceID + "." + jobName + "." + e.getKey() + "."
            + "numOfItems", "" + v.n);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return retv;
  }

  private void printJobStat(TreeMap<String, String> stat) {
    Iterator<Entry<String, String>> entries = stat.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<String, String> e = entries.next();
      System.out.println(e.getKey() + "\t" + e.getValue());
    }
  }

  private void printStatsForJobs(ArrayList<Job> jobs) {
    for (int i = 0; i < jobs.size(); i++) {
      printJobStat(getStatForJob(jobs.get(i)));
    }
  }

  public void run() {

    Thread theGridmixRunner = new Thread(gridmix);
    theGridmixRunner.start();
    long startTime = System.currentTimeMillis();
    while (!gridmix.allFinished()) {
      System.out.println("Jobs in waiting state: "
          + gridmix.getWaitingJobs().size());
      System.out.println("Jobs in ready state: "
          + gridmix.getReadyJobs().size());
      System.out.println("Jobs in running state: "
          + gridmix.getRunningJobs().size());
      System.out.println("Jobs in success state: "
          + gridmix.getSuccessfulJobs().size());
      System.out.println("Jobs in failed state: "
          + gridmix.getFailedJobs().size());
      System.out.println("\n");

      try {
        Thread.sleep(10 * 1000);
      } catch (Exception e) {

      }
    }
    long endTime = System.currentTimeMillis();
    ArrayList<Job> fail = gridmix.getFailedJobs();
    ArrayList<Job> succeed = gridmix.getSuccessfulJobs();
    int numOfSuccessfulJob = succeed.size();
    if (numOfSuccessfulJob > 0) {
      System.out.println(numOfSuccessfulJob + " jobs succeeded");
      printStatsForJobs(succeed);

    }
    int numOfFailedjob = fail.size();
    if (numOfFailedjob > 0) {
      System.out.println("------------------------------- ");
      System.out.println(numOfFailedjob + " jobs failed");
      printStatsForJobs(fail);
    }
    System.out.println("GridMix results:");
    System.out.println("Total num of Jobs: " + numOfJobs);
    System.out.println("ExecutionTime: " + ((endTime-startTime)/1000));
    gridmix.stop();
  }

  public static void main(String argv[]) throws Exception {
    GridMixRunner gridmixRunner = new GridMixRunner();
    gridmixRunner.addjobs();
    gridmixRunner.run();
  }

}
