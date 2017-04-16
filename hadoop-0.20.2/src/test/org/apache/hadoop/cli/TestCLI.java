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

package org.apache.hadoop.cli;

import java.io.File;
import java.util.ArrayList;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.cli.util.CLITestData;
import org.apache.hadoop.cli.util.CommandExecutor;
import org.apache.hadoop.cli.util.ComparatorBase;
import org.apache.hadoop.cli.util.ComparatorData;
import org.apache.hadoop.cli.util.CLITestData.TestCmd;
import org.apache.hadoop.cli.util.CLITestData.TestCmd.CommandType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.authorize.HadoopPolicyProvider;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.StringUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Tests for the Command Line Interface (CLI)
 */
public class TestCLI extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestCLI.class.getName());
  
  // In this mode, it runs the command and compares the actual output
  // with the expected output  
  public static final String TESTMODE_TEST = "test"; // Run the tests
  
  // If it is set to nocompare, run the command and do not compare.
  // This can be useful populate the testConfig.xml file the first time
  // a new command is added
  public static final String TESTMODE_NOCOMPARE = "nocompare";
  public static final String TEST_CACHE_DATA_DIR =
    System.getProperty("test.cache.data", "build/test/cache");
  public static final String TEST_DIR_ABSOLUTE = "/tmp/testcli";
  
  //By default, run the tests. The other mode is to run the commands and not
  // compare the output
  public static String testMode = TESTMODE_TEST;
  
  // Storage for tests read in from the config file
  static ArrayList<CLITestData> testsFromConfigFile = null;
  static ArrayList<ComparatorData> testComparators = null;
  static String testConfigFile = "testConf.xml";
  String thisTestCaseName = null;
  static ComparatorData comparatorData = null;
  
  private static Configuration conf = null;
  private static MiniDFSCluster dfsCluster = null;
  private static DistributedFileSystem dfs = null;
  private static MiniMRCluster mrCluster = null;
  private static String namenode = null;
  private static String jobtracker = null;
  private static String clitestDataDir = null;
  private static String username = null;
  private static String testDirAbsolute = TEST_DIR_ABSOLUTE;
  
  /**
   * Read the test config file - testConfig.xml
   */
  private void readTestConfigFile() {
    
    if (testsFromConfigFile == null) {
      boolean success = false;
      testConfigFile = System.getProperty("test.cli.config",
          TEST_CACHE_DATA_DIR + File.separator + testConfigFile);
      try {
        SAXParser p = (SAXParserFactory.newInstance()).newSAXParser();
        p.parse(testConfigFile, new TestConfigFileParser());
        LOG.info("Using test config file " + testConfigFile);
        success = true;
      } catch (Exception e) {
        LOG.info("File: " + testConfigFile + " not found");
        success = false;
      }
      assertTrue("Error reading test config file", success);
    }
  }
  
  /*
   * Setup
   */
  public void setUp() throws Exception {
    // Read the testConfig.xml file
    readTestConfigFile();
    
    conf = new Configuration();
    conf.setClass(PolicyProvider.POLICY_PROVIDER_CONFIG,
                  HadoopPolicyProvider.class, PolicyProvider.class);
    conf.setBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, 
                    true);
    // Many of the tests expect a replication value of 1 in the output
    conf.setInt("dfs.replication", 1);

    FileSystem fs;
    namenode = System.getProperty(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY);
    if (namenode == null) {
      // Start up the mini dfs cluster
      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
      namenode = conf.get("fs.default.name", "file:///");
      fs = dfsCluster.getFileSystem();
    } else {
      conf.set("fs.default.name", namenode);
      fs = FileSystem.get(conf);
    }
    
    clitestDataDir = new File(TEST_CACHE_DATA_DIR).
      toURI().toString().replace(' ', '+');
    username = System.getProperty("user.name");

    assertTrue("Not a HDFS: "+fs.getUri(),
               fs instanceof DistributedFileSystem);
    dfs = (DistributedFileSystem) fs;
    
    jobtracker = System.getProperty("mapred.job.tracker");
    if (jobtracker == null) {
      // Start up mini mr cluster
      JobConf mrConf = new JobConf(conf);
      mrCluster = new MiniMRCluster(1, dfsCluster.getFileSystem().getUri().toString(), 1, 
                           null, null, mrConf);
      jobtracker = mrCluster.createJobConf().get("mapred.job.tracker", "local");
    } else {
      conf.set("mapred.job.tracker", jobtracker);
    }

    LOG.info("Namenode: " + namenode);
    LOG.info("Jobtracker: " + jobtracker);
  }
  
  /**
   * Tear down
   */
  public void tearDown() throws Exception {
    dfs.delete(new Path(testDirAbsolute), true);
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfs.close();
      dfsCluster.shutdown();
      Thread.sleep(2000);
    }
    displayResults();
  }
  
  /**
   * Expand the commands from the test config xml file
   * @param cmd
   * @return String expanded command
   */
  private String expandCommand(final String cmd) {
    String expCmd = cmd;
    expCmd = expCmd.replaceAll("NAMENODE", namenode);
    expCmd = expCmd.replaceAll("JOBTRACKER", jobtracker);
    expCmd = expCmd.replaceAll("CLITEST_DATA", clitestDataDir);
    expCmd = expCmd.replaceAll("USERNAME", username);
    expCmd = expCmd.replaceAll("TEST_DIR_ABSOLUTE", testDirAbsolute);
    
    return expCmd;
  }
  
  /**
   * Display the summarized results
   */
  private void displayResults() {
    LOG.info("Detailed results:");
    LOG.info("----------------------------------\n");
    
    for (int i = 0; i < testsFromConfigFile.size(); i++) {
      CLITestData td = testsFromConfigFile.get(i);
      
      boolean testResult = td.getTestResult();
      
      // Display the details only if there is a failure
      if (!testResult) {
        LOG.info("-------------------------------------------");
        LOG.info("                    Test ID: [" + (i + 1) + "]");
        LOG.info("           Test Description: [" + td.getTestDesc() + "]");
        LOG.info("");

        ArrayList<TestCmd> testCommands = td.getTestCommands();
        for (TestCmd cmd : testCommands) {
          LOG.info("              Test Commands: [" + 
                   expandCommand(cmd.getCmd()) + "]");
        }

        LOG.info("");
        ArrayList<TestCmd> cleanupCommands = td.getCleanupCommands();
        for (TestCmd cmd : cleanupCommands) {
          LOG.info("           Cleanup Commands: [" +
                   expandCommand(cmd.getCmd()) + "]");
        }

        LOG.info("");
        ArrayList<ComparatorData> compdata = td.getComparatorData();
        for (ComparatorData cd : compdata) {
          boolean resultBoolean = cd.getTestResult();
          LOG.info("                 Comparator: [" + 
                   cd.getComparatorType() + "]");
          LOG.info("         Comparision result:   [" + 
                   (resultBoolean ? "pass" : "fail") + "]");
          LOG.info("            Expected output:   [" + 
                   cd.getExpectedOutput() + "]");
          LOG.info("              Actual output:   [" + 
                   cd.getActualOutput() + "]");
        }
        LOG.info("");
      }
    }
    
    LOG.info("Summary results:");
    LOG.info("----------------------------------\n");
    
    boolean overallResults = true;
    int totalPass = 0;
    int totalFail = 0;
    int totalComparators = 0;
    for (int i = 0; i < testsFromConfigFile.size(); i++) {
      CLITestData td = testsFromConfigFile.get(i);
      totalComparators += 
    	  testsFromConfigFile.get(i).getComparatorData().size();
      boolean resultBoolean = td.getTestResult();
      if (resultBoolean) {
        totalPass ++;
      } else {
        totalFail ++;
      }
      overallResults &= resultBoolean;
    }
    
    
    LOG.info("               Testing mode: " + testMode);
    LOG.info("");
    LOG.info("             Overall result: " + 
    		(overallResults ? "+++ PASS +++" : "--- FAIL ---"));
    LOG.info("               # Tests pass: " + totalPass +
    		" (" + (100 * totalPass / (totalPass + totalFail)) + "%)");
    LOG.info("               # Tests fail: " + totalFail + 
    		" (" + (100 * totalFail / (totalPass + totalFail)) + "%)");
    LOG.info("         # Validations done: " + totalComparators + 
    		" (each test may do multiple validations)");
    
    LOG.info("");
    LOG.info("Failing tests:");
    LOG.info("--------------");
    int i = 0;
    boolean foundTests = false;
    for (i = 0; i < testsFromConfigFile.size(); i++) {
      boolean resultBoolean = testsFromConfigFile.get(i).getTestResult();
      if (!resultBoolean) {
        LOG.info((i + 1) + ": " + 
        		testsFromConfigFile.get(i).getTestDesc());
        foundTests = true;
      }
    }
    if (!foundTests) {
    	LOG.info("NONE");
    }
    
    foundTests = false;
    LOG.info("");
    LOG.info("Passing tests:");
    LOG.info("--------------");
    for (i = 0; i < testsFromConfigFile.size(); i++) {
      boolean resultBoolean = testsFromConfigFile.get(i).getTestResult();
      if (resultBoolean) {
        LOG.info((i + 1) + ": " + 
        		testsFromConfigFile.get(i).getTestDesc());
        foundTests = true;
      }
    }
    if (!foundTests) {
    	LOG.info("NONE");
    }

    assertTrue("One of the tests failed. " +
    		"See the Detailed results to identify " +
    		"the command that failed", overallResults);
    
  }
  
  /**
   * Compare the actual output with the expected output
   * @param compdata
   * @return
   */
  private boolean compareTestOutput(ComparatorData compdata) {
    // Compare the output based on the comparator
    String comparatorType = compdata.getComparatorType();
    Class<?> comparatorClass = null;
    
    // If testMode is "test", then run the command and compare the output
    // If testMode is "nocompare", then run the command and dump the output.
    // Do not compare
    
    boolean compareOutput = false;
    
    if (testMode.equals(TESTMODE_TEST)) {
      try {
    	// Initialize the comparator class and run its compare method
        comparatorClass = Class.forName("org.apache.hadoop.cli.util." + 
          comparatorType);
        ComparatorBase comp = (ComparatorBase) comparatorClass.newInstance();
        compareOutput = comp.compare(CommandExecutor.getLastCommandOutput(), 
          compdata.getExpectedOutput());
      } catch (Exception e) {
        LOG.info("Error in instantiating the comparator" + e);
      }
    }
    
    return compareOutput;
  }
  
  /***********************************
   ************* TESTS
   *********************************/
  
  public void testAll() {
    LOG.info("TestAll");
    
    // Run the tests defined in the testConf.xml config file.
    for (int index = 0; index < testsFromConfigFile.size(); index++) {
      
      CLITestData testdata = (CLITestData) testsFromConfigFile.get(index);
   
      // Execute the test commands
      ArrayList<TestCmd> testCommands = testdata.getTestCommands();
      for (TestCmd cmd : testCommands) {
      try {
        CommandExecutor.executeCommand(cmd, namenode, jobtracker);
      } catch (Exception e) {
        fail(StringUtils.stringifyException(e));
      }
      }
      
      boolean overallTCResult = true;
      // Run comparators
      ArrayList<ComparatorData> compdata = testdata.getComparatorData();
      for (ComparatorData cd : compdata) {
        final String comptype = cd.getComparatorType();
        
        boolean compareOutput = false;
        
        if (! comptype.equalsIgnoreCase("none")) {
          compareOutput = compareTestOutput(cd);
          overallTCResult &= compareOutput;
        }
        
        cd.setExitCode(CommandExecutor.getLastExitCode());
        cd.setActualOutput(CommandExecutor.getLastCommandOutput());
        cd.setTestResult(compareOutput);
      }
      testdata.setTestResult(overallTCResult);
      
      // Execute the cleanup commands
      ArrayList<TestCmd> cleanupCommands = testdata.getCleanupCommands();
      for (TestCmd cmd : cleanupCommands) {
      try { 
        CommandExecutor.executeCommand(cmd, namenode, jobtracker);
      } catch (Exception e) {
        fail(StringUtils.stringifyException(e));
      }
      }
    }
  }
  
  /*
   * Parser class for the test config xml file
   */
  static class TestConfigFileParser extends DefaultHandler {
    String charString = null;
    CLITestData td = null;
    ArrayList<TestCmd> testCommands = null;
    ArrayList<TestCmd> cleanupCommands = null;
    
    @Override
    public void startDocument() throws SAXException {
      testsFromConfigFile = new ArrayList<CLITestData>();
    }
    
    @Override
    public void startElement(String uri, 
    		String localName, 
    		String qName, 
    		Attributes attributes) throws SAXException {
      if (qName.equals("test")) {
        td = new CLITestData();
      } else if (qName.equals("test-commands")) {
        testCommands = new ArrayList<TestCmd>();
      } else if (qName.equals("cleanup-commands")) {
        cleanupCommands = new ArrayList<TestCmd>();
      } else if (qName.equals("comparators")) {
        testComparators = new ArrayList<ComparatorData>();
      } else if (qName.equals("comparator")) {
        comparatorData = new ComparatorData();
      }
      charString = "";
    }
    
    @Override
    public void endElement(String uri, 
    		String localName, 
    		String qName) throws SAXException {
      if (qName.equals("description")) {
        td.setTestDesc(charString);
      } else if (qName.equals("test-commands")) {
        td.setTestCommands(testCommands);
        testCommands = null;
      } else if (qName.equals("cleanup-commands")) {
        td.setCleanupCommands(cleanupCommands);
        cleanupCommands = null;
      } else if (qName.equals("command")) {
        if (testCommands != null) {
          testCommands.add(new TestCmd(charString, CommandType.FS));
        } else if (cleanupCommands != null) {
          cleanupCommands.add(new TestCmd(charString, CommandType.FS));
        }
      } else if (qName.equals("dfs-admin-command")) {
          if (testCommands != null) {
              testCommands.add(new TestCmd(charString,CommandType.DFSADMIN));
            } else if (cleanupCommands != null) {
              cleanupCommands.add(new TestCmd(charString, CommandType.DFSADMIN));
            } 
      } else if (qName.equals("mr-admin-command")) {
        if (testCommands != null) {
            testCommands.add(new TestCmd(charString,CommandType.MRADMIN));
          } else if (cleanupCommands != null) {
            cleanupCommands.add(new TestCmd(charString, CommandType.MRADMIN));
          } 
      } else if (qName.equals("comparators")) {
        td.setComparatorData(testComparators);
      } else if (qName.equals("comparator")) {
        testComparators.add(comparatorData);
      } else if (qName.equals("type")) {
        comparatorData.setComparatorType(charString);
      } else if (qName.equals("expected-output")) {
        comparatorData.setExpectedOutput(charString);
      } else if (qName.equals("test")) {
        testsFromConfigFile.add(td);
        td = null;
      } else if (qName.equals("mode")) {
        testMode = charString;
        if (!testMode.equals(TESTMODE_NOCOMPARE) &&
            !testMode.equals(TESTMODE_TEST)) {
          testMode = TESTMODE_TEST;
        }
      }
    }
    
    @Override
    public void characters(char[] ch, 
    		int start, 
    		int length) throws SAXException {
      String s = new String(ch, start, length);
      charString += s;
    }
  }
}
