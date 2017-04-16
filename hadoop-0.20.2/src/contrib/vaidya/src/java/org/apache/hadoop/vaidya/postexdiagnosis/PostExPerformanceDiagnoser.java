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

package org.apache.hadoop.vaidya.postexdiagnosis;


import java.net.URL;
import java.io.InputStream;
import java.io.FileInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.vaidya.util.XMLUtils;
import org.apache.hadoop.vaidya.DiagnosticTest;
import org.apache.hadoop.vaidya.JobDiagnoser;
import org.apache.hadoop.vaidya.statistics.job.JobStatistics;
import org.w3c.dom.NodeList;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


/**
 * This class acts as a driver or rule engine for executing the post execution 
 * performance diagnostics tests of a map/reduce job. It prints or saves the 
 * diagnostic report as a xml document. 
 */
public class PostExPerformanceDiagnoser extends JobDiagnoser {
  
  private String _jobHistoryFile = null;
  private InputStream _testsConfFileIs = null;
  private String _reportFile = null;
  private String _jobConfFile = null;

  /* 
   * Data available for analysts to write post execution performance diagnostic rules 
   */
  private JobStatistics _jobExecutionStatistics;
  
  /*
   * Get the report file where diagnostic report is to be saved
   */
  public String getReportFile () {
    return this._reportFile;
  }
  
  /*
   * Get the job history log file used in collecting the job counters
   */
  public String getJobHistoryFile () {
    return this._jobHistoryFile;
  }
  
  /*
   * Get the test configuration file where all the diagnostic tests are registered
   * with their configuration information.
   */
  public InputStream getTestsConfFileIs () {
    return  this._testsConfFileIs;
  }
    
  /*
   * Set the test configuration file
   */
  public void setTestsConfFileIs (InputStream testsConfFileIs) {
    this._testsConfFileIs = testsConfFileIs;
  }
  
  /**
   * @return JobStatistics - Object storing the job configuration and execution
   * counters and statistics information
   */
  public JobStatistics getJobExecutionStatistics() {
    return _jobExecutionStatistics;
  }

  /**
   * @param jobConfFile - URL pointing to job configuration (job_conf.xml) file
   * @param jobHistoryLogFile - URL pointing to job history log file  
   * @param testsConfFile - file path for test configuration file (optional). 
   * If not specified default path is:$HADOOP_HOME/contrib/vaidya/pxpd_tests_config.xml
   * @param reportFile - file path for storing report (optional)
   */
  public PostExPerformanceDiagnoser (String jobConfFile, String jobHistoryFile, InputStream testsConfFileIs,
                String reportFile) throws Exception {
    
    this._jobHistoryFile = jobHistoryFile;
    this._testsConfFileIs = testsConfFileIs;
    this._reportFile = reportFile;
    this._jobConfFile = jobConfFile;
    
    /*
     * Read the job information necessary for post performance analysis
     */
    JobConf jobConf = new JobConf();
    JobInfo jobInfo = new JobInfo("");
    readJobInformation(jobConf, jobInfo);
    this._jobExecutionStatistics = new JobStatistics(jobConf, jobInfo);
  }

  /**
   * read and populate job statistics information.
   */
  private void readJobInformation(JobConf jobConf, JobInfo jobInfo) throws Exception {
  
    /*
     * Convert the input strings to URL
     */
    URL jobConfFileUrl = new URL(this._jobConfFile);
    URL jobHistoryFileUrl = new URL (this._jobHistoryFile);
    
    /*
     * Read the Job Configuration from the jobConfFile url
     */  
    jobConf.addResource(jobConfFileUrl);
    
    /* 
     * Read JobHistoryFile and build job counters to evaluate diagnostic rules
     */
    if (jobHistoryFileUrl.getProtocol().equals("hdfs")) {
      DefaultJobHistoryParser.parseJobTasks (jobHistoryFileUrl.getPath(), jobInfo, FileSystem.get(jobConf));
    } else if (jobHistoryFileUrl.getProtocol().equals("file")) {
      DefaultJobHistoryParser.parseJobTasks (jobHistoryFileUrl.getPath(), jobInfo, FileSystem.getLocal(jobConf));
    } else {
      throw new Exception("Malformed URL. Protocol: "+jobHistoryFileUrl.getProtocol());
    }
  }
  
  /*
   * print Help
   */
  private static void printHelp() {
    System.out.println("Usage:");
    System.out.println("PostExPerformanceDiagnoser -jobconf <fileurl> -joblog <fileurl> [-testconf <filepath>] [-report <filepath>]");
    System.out.println();
    System.out.println("-jobconf <fileurl>     : File path for job configuration file (e.g. job_xxxx_conf.xml). It can be on HDFS or");
    System.out.println("                       : local file system. It should be specified in the URL format.");
    System.out.println("                       : e.g. local file => file://localhost/Users/hadoop-user/job_0001_conf.xml");
    System.out.println("                       : e.g. hdfs file  => hdfs://namenode:port/Users/hadoop-user/hodlogs/.../job_0001_conf.xml");
    System.out.println();
    System.out.println("-joblog <fileurl>      : File path for job history log file. It can be on HDFS or local file system.");
    System.out.println("                       : It should be specified in the URL format.");
    System.out.println();
    System.out.println("-testconf <filepath>   : Optional file path for performance advisor tests configuration file. It should be available");
    System.out.println("                       : on local file system and be specified as as an absolute file path.");
    System.out.println("                       : e.g. => /Users/hadoop-user/postex_diagnosis_tests.xml. If not specified default file will be used");
    System.out.println("                       : from the hadoop-{ver}-vaidya.jar in a classpath.");
    System.out.println("                       : For user to view or make local copy of default tests, file is available at $HADOOP_HOME/contrib/vaidya/conf/postex_diagnosis_tests.xml");
    System.out.println();
    System.out.println("-report <filepath>     : Optional file path for for storing diagnostic report in a XML format. Path should be available");
    System.out.println("                       : on local file system and be specified as as an absolute file path.");
    System.out.println("                       : e.g. => /Users/hadoop-user/postex_diagnosis_report.xml. If not specified report will be printed on console");
    System.out.println();
    System.out.println("-help                  : prints this usage");
    System.out.println();
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    
    String jobconffile = null;
    String joblogfile = null;
    InputStream testsconffileis = null;
    String reportfile = null; 
    
    /*
     * Parse the command line arguments
     */
    try {
      for (int i=0; i<args.length-1; i=i+2) {
        if (args[i].equalsIgnoreCase("-jobconf")) {
          jobconffile = args[i+1];
        } else if (args[i].equalsIgnoreCase("-joblog")) {
          joblogfile = args[i+1];
        } else if (args[i].equalsIgnoreCase("-testconf")) {
          testsconffileis = new FileInputStream(new java.io.File(args[i+1]));
        } else if (args[i].equalsIgnoreCase("-report")) {
          reportfile = args[i+1];
        } else if (args[i].equalsIgnoreCase("-help")) {
          printHelp(); return;
        } else {
          printHelp(); return;
        }
      }
    } catch (Exception e) {
      System.err.println ("Invalid arguments.");
      e.printStackTrace();
      System.err.println();
      printHelp();
    }
    
    // Check if required arguments are specified
    if (jobconffile == null || joblogfile  == null) {
      System.err.println ("Invalid arguments: -jobconf or -joblog arguments are missing");
      printHelp();
      return;
    }
    
    try {
      /*
       * Create performance advisor and read job execution statistics
       */
      PostExPerformanceDiagnoser pa = new PostExPerformanceDiagnoser(jobconffile,joblogfile,testsconffileis,reportfile);
      
      /*
       * Read the diagnostic tests configuration file (xml)
       */
      if (pa.getTestsConfFileIs() == null) {
        java.io.InputStream testsconfis = Thread.currentThread().getContextClassLoader().getResourceAsStream("postex_diagnosis_tests.xml");
        pa.setTestsConfFileIs(testsconfis);
      }
      
      /*
       * Parse the tests configuration file
       */
      Document rulesDoc = XMLUtils.parse(pa.getTestsConfFileIs());
      
      /* 
       * Read the diagnostic rule entries from the config file.
       * For every rule read and load the rule class name
       * Execute the Run() method of the class and get the report element
       */
      NodeList list = rulesDoc.getElementsByTagName("DiagnosticTest");
      int list_size = list.getLength();
      for (int i=0;i<list_size; i++) {
        Element dRule = (Element)list.item(i);
        NodeList cNodeList = dRule.getElementsByTagName("ClassName");
        Element cn = (Element)cNodeList.item(0);
        String className = cn.getFirstChild().getNodeValue().trim();
        Class rc = Class.forName(className);
        DiagnosticTest test = (DiagnosticTest)rc.newInstance();
        test.initGlobals(pa.getJobExecutionStatistics(), (Element)list.item(i));
        test.run();
        NodeList nodelist = pa.getReport().getElementsByTagName("PostExPerformanceDiagnosticReport");
        Element root = (Element)nodelist.item(0);
        //root.appendChild(rule.getReportElement(pa.getReport(), root)); 
        Element re = test.getReportElement(pa.getReport(), root, i);
        //XMLUtils.printDOM(re);
      } 
      
      //Optionally print or save the report
      if (pa.getReportFile() == null) {
        pa.printReport();
      } else {
        pa.saveReport(pa.getReportFile());
      }
    }catch (Exception e) {
      System.err.print("Exception:"+e);
      e.printStackTrace();
    }
  }
}
