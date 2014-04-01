package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.UtilsForTests;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.StaticMapping;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

public class TestJobInProgress {
  static final Log LOG = LogFactory.getLog(TestJobInProgress.class);

  private static MiniMRCluster mrCluster;

  private static MiniDFSCluster dfsCluster;
  private static JobTracker jt;

  static final String trackers[] = new String[] {
      "tracker_tracker1.r1.com:1000", "tracker_tracker2.r1.com:1000",
      "tracker_tracker3.r2.com:1000", "tracker_tracker4.r3.com:1000" };

  static final String[] hosts = new String[] { "tracker1.r1.com",
      "tracker2.r1.com", "tracker3.r2.com", "tracker4.r3.com" };

  static final String[] racks = new String[] { "/r1", "/r1", "/r2", "/r3" };

  private static Path TEST_DIR = 
    new Path(System.getProperty("test.build.data","/tmp"), "jip-testing");
  private static int numSlaves = 4;

  public static class FailMapTaskJob extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      // reporter.incrCounter(TaskCounts.LaunchedTask, 1);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalArgumentException("Interrupted MAP task");
      }
      throw new IllegalArgumentException("Failing MAP task");
    }
  }

  // Suppressing waring as we just need to write a failing reduce task job
  // We don't need to bother about the actual key value pairs which are passed.
  @SuppressWarnings("unchecked")
  public static class FailReduceTaskJob extends MapReduceBase implements
      Reducer {

    @Override
    public void reduce(Object key, Iterator values, OutputCollector output,
        Reporter reporter) throws IOException {
      // reporter.incrCounter(TaskCounts.LaunchedTask, 1);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalArgumentException("Failing Reduce task");
      }
      throw new IllegalArgumentException("Failing Reduce task");
    }

  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.jobtracker.address", "localhost:0");
    conf.set("mapreduce.jobtracker.http.address", "0.0.0.0:0");
    conf.setClass("topology.node.switch.mapping.impl", StaticMapping.class,
        DNSToSwitchMapping.class);
    dfsCluster = new MiniDFSCluster(conf, numSlaves, true, null);
    mrCluster = new MiniMRCluster(numSlaves, dfsCluster.getFileSystem()
        .getUri().toString(), 1);
    jt = mrCluster.getJobTrackerRunner().getJobTracker();
    // Set up the Topology Information
    for (int i = 0; i < hosts.length; i++) {
      StaticMapping.addNodeToRack(hosts[i], racks[i]);
    }
    for (String s : trackers) {
      FakeObjectUtilities.establishFirstContact(jt, s);
    }
  }

  @Test
  public void testPendingMapTaskCount() throws Exception {
    launchTask(FailMapTaskJob.class, IdentityReducer.class);
    checkTaskCounts();
  }
  
  @Test
  public void testPendingReduceTaskCount() throws Exception {
    launchTask(IdentityMapper.class, FailReduceTaskJob.class);
    checkTaskCounts();
  }

  /**
   * Test if running tasks are correctly maintained for various types of jobs
   */
  private void testRunningTaskCount(boolean speculation, boolean locality)
  throws Exception {
    LOG.info("Testing running jobs with speculation : " + speculation 
             + ", locality : " + locality);
    // cleanup
    dfsCluster.getFileSystem().delete(TEST_DIR, true);
    
    final Path mapSignalFile = new Path(TEST_DIR, "map-signal");
    final Path redSignalFile = new Path(TEST_DIR, "reduce-signal");
    
    // configure a waiting job with 2 maps and 2 reducers
    JobConf job = 
      configure(UtilsForTests.WaitingMapper.class, IdentityReducer.class, 1, 1,
                locality);
    job.set(UtilsForTests.getTaskSignalParameter(true), mapSignalFile.toString());
    job.set(UtilsForTests.getTaskSignalParameter(false), redSignalFile.toString());
    
    // Disable slow-start for reduces since this maps don't complete
    // in these test-cases...
    job.setFloat("mapred.reduce.slowstart.completed.maps", 0.0f);
    
    // test jobs with speculation
    job.setSpeculativeExecution(speculation);
    JobClient jc = new JobClient(job);
    RunningJob running = jc.submitJob(job);
    JobTracker jobtracker = mrCluster.getJobTrackerRunner().getJobTracker();
    JobInProgress jip = jobtracker.getJob(running.getID());
    LOG.info("Running job " + jip.getJobID());
    
    // wait
    LOG.info("Waiting for job " + jip.getJobID() + " to be ready");
    waitTillReady(jip, job);
    
    // check if the running structures are populated
    Set<TaskInProgress> uniqueTasks = new HashSet<TaskInProgress>();
    for (Map.Entry<Node, Set<TaskInProgress>> s : 
           jip.getRunningMapCache().entrySet()) {
      uniqueTasks.addAll(s.getValue());
    }
    
    // add non local map tasks
    uniqueTasks.addAll(jip.getNonLocalRunningMaps());
    
    assertEquals("Running map count doesnt match for jobs with speculation " 
                 + speculation + ", and locality " + locality,
                 jip.runningMaps(), uniqueTasks.size());

    assertEquals("Running reducer count doesnt match for jobs with speculation "
                 + speculation + ", and locality " + locality,
                 jip.runningReduces(), jip.getRunningReduces().size());
    
    // signal the tasks
    LOG.info("Signaling the tasks");
    UtilsForTests.signalTasks(dfsCluster, dfsCluster.getFileSystem(),
                              mapSignalFile.toString(), 
                              redSignalFile.toString(), numSlaves);
    
    // wait for the job to complete
    LOG.info("Waiting for job " + jip.getJobID() + " to be complete");
    UtilsForTests.waitTillDone(jc);
    
    // cleanup
    dfsCluster.getFileSystem().delete(TEST_DIR, true);
  }
  
  // wait for the job to start
  private void waitTillReady(JobInProgress jip, JobConf job) {
    // wait for all the maps to get scheduled
    while (jip.runningMaps() < job.getNumMapTasks()) {
      UtilsForTests.waitFor(10);
    }
    
    // wait for all the reducers to get scheduled
    while (jip.runningReduces() < job.getNumReduceTasks()) {
      UtilsForTests.waitFor(10);
    }
  }
  
  @Test
  public void testRunningTaskCount() throws Exception {
    // test with spec = false and locality=true
    testRunningTaskCount(false, true);
    
    // test with spec = true and locality=true
    testRunningTaskCount(true, true);
    
    // test with spec = false and locality=false
    testRunningTaskCount(false, false);
    
    // test with spec = true and locality=false
    testRunningTaskCount(true, false);
  }
  
  @Test
  public void testLocality() throws Exception {
    NetworkTopology nt = new NetworkTopology();

    Node r1n1 = new NodeBase("/default/rack1/node1");
    nt.add(r1n1);
    Node r1n2 = new NodeBase("/default/rack1/node2");
    nt.add(r1n2);

    Node r2n3 = new NodeBase("/default/rack2/node3");
    nt.add(r2n3);

    LOG.debug("r1n1 parent: " + r1n1.getParent() + "\n" +
              "r1n2 parent: " + r1n2.getParent() + "\n" +
              "r2n3 parent: " + r2n3.getParent());

    // Same host
    assertEquals(0, JobInProgress.getMatchingLevelForNodes(r1n1, r1n1, 3));
    // Same rack
    assertEquals(1, JobInProgress.getMatchingLevelForNodes(r1n1, r1n2, 3));
    // Different rack
    assertEquals(2, JobInProgress.getMatchingLevelForNodes(r1n1, r2n3, 3));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    mrCluster.shutdown();
    dfsCluster.shutdown();
  }
  

  void launchTask(Class MapClass,Class ReduceClass) throws Exception{
    JobConf job = configure(MapClass, ReduceClass, 5, 10, true);
    try {
      JobClient.runJob(job);
    } catch (IOException ioe) {}
  }
  
  @SuppressWarnings("unchecked")
  JobConf configure(Class MapClass,Class ReduceClass, int maps, int reducers,
                    boolean locality) 
  throws Exception {
    JobConf jobConf = mrCluster.createJobConf();
    final Path inDir = new Path("./failjob/input");
    final Path outDir = new Path("./failjob/output");
    String input = "Test failing job.\n One more line";
    FileSystem inFs = inDir.getFileSystem(jobConf);
    FileSystem outFs = outDir.getFileSystem(jobConf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("create directory failed" + inDir.toString());
    }

    DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
    file.writeBytes(input);
    file.close();
    jobConf.setJobName("failmaptask");
    if (locality) {
      jobConf.setInputFormat(TextInputFormat.class);
    } else {
      jobConf.setInputFormat(UtilsForTests.RandomInputFormat.class);
    }
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMapperClass(MapClass);
    jobConf.setCombinerClass(ReduceClass);
    jobConf.setReducerClass(ReduceClass);
    FileInputFormat.setInputPaths(jobConf, inDir);
    FileOutputFormat.setOutputPath(jobConf, outDir);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reducers);
    return jobConf; 
  }

  void checkTaskCounts() {
    JobStatus[] status = jt.getAllJobs();
    for (JobStatus js : status) {
      JobInProgress jip = jt.getJob(js.getJobID());
      Counters counter = jip.getJobCounters();
      long totalTaskCount = counter
          .getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS)
          + counter.getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_REDUCES);
      while (jip.getNumTaskCompletionEvents() < totalTaskCount) {
        assertEquals(true, (jip.runningMaps() >= 0));
        assertEquals(true, (jip.pendingMaps() >= 0));
        assertEquals(true, (jip.runningReduces() >= 0));
        assertEquals(true, (jip.pendingReduces() >= 0));
      }
    }
  }
  
}
