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
package org.apache.hadoop.vaidya;

import java.lang.Runnable;
import java.sql.Timestamp;
import org.apache.hadoop.vaidya.statistics.job.*;
import org.apache.hadoop.vaidya.util.*;
import org.w3c.dom.Node;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.apache.hadoop.vaidya.statistics.job.JobStatisticsInterface.JobKeys;

/*
 * This is an abstract base class to be extended by each diagnostic test 
 * class. It implements Runnable interface so that if required multiple tests 
 * can be run in parallel. 
 */
public abstract class DiagnosticTest implements Runnable {
  
  private static final double HIGHVAL = 0.99;
  private static final double MEDIUMVAL = 0.66;
  private static final double LOWVAL = 0.33;
  
  /*
   * Job statistics are passed to this class against which this diagnostic 
   * test is evaluated.
   */
  private JobStatistics _jobExecutionStats;
  private Element _testConfigElement;
  private double _impactLevel;
  private boolean _evaluated;
  private boolean _testPassed; 
  
  /* 
   * Checks if test is already evaluated against job execution statistics
   * @return - true if test is already evaluated once.
   */
  public boolean isEvaluated() {
    return _evaluated;
  }

  /*
   * If impact level (returned by evaluate method) is less than success threshold 
   * then test is passed (NEGATIVE) else failed (POSITIVE) which inturn indicates the 
   * problem with job performance  
   */
  public boolean istestPassed() {
    return this._testPassed;
  }
  
  
  /*
   * Initialize the globals
   */
  public void initGlobals (JobStatistics jobExecutionStats, Element testConfigElement) {
    this._jobExecutionStats = jobExecutionStats;
    this._testConfigElement = testConfigElement;
  }
  
  /*
   * Returns a prescription/advice (formated text) based on the evaluation of 
   * diagnostic test condition (evaluate method). Individual test should override 
   * and implement it. If the value returned is null then the prescription advice
   * is printed as provided in the test config file.  
   */
  public abstract String getPrescription();
  
  /*
   * This method prints any reference details to support the test result. Individual
   * test needs to override and implement it and information printed is specific 
   * to individual test. 
   */
  public abstract String getReferenceDetails ();
  
  /*
   * Evaluates diagnostic condition and returns impact level (value [0..1])
   * Typically this method calculates the impact of a diagnosed condition on the job performance
   * (Note: for boolean conditions it is either 0 or 1).
   */
  public abstract double evaluate (JobStatistics jobExecutionStats);
  
  /*
   * Get the Title information for this test as set in the test config file
   */
  public String getTitle() throws Exception {
    return XMLUtils.getElementValue("Title", this._testConfigElement);
  }
  
  /*
   * Get the Description information as set in the test config file.
   */
  public String getDescription() throws Exception {
    return XMLUtils.getElementValue("Description", this._testConfigElement);
  }
  
  /*
   * Get the Importance value as set in the test config file.
   */
  public double getImportance() throws Exception {  
    if (XMLUtils.getElementValue("Importance", this._testConfigElement).equalsIgnoreCase("high")) {
      return HIGHVAL;
    } else if (XMLUtils.getElementValue("Importance", this._testConfigElement).equalsIgnoreCase("medium")) {
      return MEDIUMVAL;
    } else {
      return LOWVAL;
    }
  }
  
  /*
   * Returns the impact level of this test condition. This value is calculated and
   * returned by evaluate method.
   */
  public double getImpactLevel() throws Exception {
    if (!this.isEvaluated()) {
      throw new Exception("Test has not been evaluated");
    }
    return truncate(this._impactLevel);
  }

  /* 
   * Get the severity level as specified in the test config file.
   */
  public double getSeverityLevel() throws Exception {
    return truncate ((double)(getImportance()*getImpactLevel()));
  }

  /*
   * Get Success Threshold as specified in the test config file.
   */
  public double getSuccessThreshold() throws Exception {
    double x = Double.parseDouble(XMLUtils.getElementValue("SuccessThreshold", this._testConfigElement));
    return truncate (x);
  }
  
  /*
   * Creates and returns the report element for this test based on the 
   * test evaluation results.
   */
  public Element getReportElement(Document doc, Node parent, int i) throws Exception {

    /* 
     * If test is not evaluated yet then throw exception
     */
    if (!this.isEvaluated()) {
      throw new Exception("Test has not been evaluated");
    }

    /* 
     * If i == 0, means first test, then print job information
     * before it.
    */
    if (i == 0) {
      Node reportElementx = doc.createElement("JobInformationElement");
      parent.appendChild(reportElementx);

      // Insert JOBTRACKERID
      Node itemx = doc.createElement("JobTrackerID");
      reportElementx.appendChild(itemx);
      Node valuex = doc.createTextNode(this._jobExecutionStats.getStringValue(JobKeys.JOBTRACKERID));
      itemx.appendChild(valuex);

      // Insert JOBNAME
      itemx = doc.createElement("JobName");
      reportElementx.appendChild(itemx);
      valuex = doc.createTextNode(this._jobExecutionStats.getStringValue(JobKeys.JOBNAME));
      itemx.appendChild(valuex);

      // Insert JOBTYPE
      itemx = doc.createElement("JobType");
      reportElementx.appendChild(itemx);
      valuex = doc.createTextNode(this._jobExecutionStats.getStringValue(JobKeys.JOBTYPE));
      itemx.appendChild(valuex);

      // Insert USER
      itemx = doc.createElement("User");
      reportElementx.appendChild(itemx);
      valuex = doc.createTextNode(this._jobExecutionStats.getStringValue(JobKeys.USER));
      itemx.appendChild(valuex);

      // Insert SUBMIT_TIME
      itemx = doc.createElement("SubmitTime");
      reportElementx.appendChild(itemx);
      String st1 = (new Timestamp(Long.parseLong(this._jobExecutionStats.getStringValue(JobKeys.SUBMIT_TIME))).toString());
      valuex = doc.createTextNode(st1);
      itemx.appendChild(valuex);

      // Insert LAUNCH_TIME
      itemx = doc.createElement("LaunchTime");
      reportElementx.appendChild(itemx);
      String st2 = (new Timestamp(Long.parseLong(this._jobExecutionStats.getStringValue(JobKeys.LAUNCH_TIME))).toString());
      valuex = doc.createTextNode(st2);
      itemx.appendChild(valuex);

      // Insert FINISH_TIME
      itemx = doc.createElement("FinishTime");
      reportElementx.appendChild(itemx);
      String st3 = (new Timestamp(Long.parseLong(this._jobExecutionStats.getStringValue(JobKeys.FINISH_TIME))).toString());
      valuex = doc.createTextNode(st3);
      itemx.appendChild(valuex);

      // Insert STATUS
      itemx = doc.createElement("Status");
      reportElementx.appendChild(itemx);
      valuex = doc.createTextNode(this._jobExecutionStats.getStringValue(JobKeys.STATUS));
      itemx.appendChild(valuex);
    }

    /*
     * Construct and return the report element
     */
    // Insert Child ReportElement
    Node reportElement = doc.createElement("TestReportElement");
    parent.appendChild(reportElement);

    // Insert title
    Node item = doc.createElement("TestTitle");
    reportElement.appendChild(item);
    Node value = doc.createTextNode(this.getTitle());
    item.appendChild(value);

    // Insert description
    item = doc.createElement("TestDescription");
    reportElement.appendChild(item);
    value = doc.createTextNode(this.getDescription());
    item.appendChild(value);

    // Insert Importance
    item = doc.createElement("TestImportance");
    reportElement.appendChild(item);
    String imp;
    if (this.getImportance() == HIGHVAL) {
      imp = "HIGH";
    } else if (this.getImportance() == MEDIUMVAL) {
      imp = "MEDIUM";
    } else { 
      imp = "LOW";
    }
    value = doc.createTextNode(imp);
    item.appendChild(value);

    // Insert Importance
    item = doc.createElement("TestResult");
    reportElement.appendChild(item);
    if (this._testPassed) {
      value = doc.createTextNode("NEGATIVE(PASSED)");
    } else {
      value = doc.createTextNode("POSITIVE(FAILED)");
    }
    item.appendChild(value);
      
    // TODO : if (!this._testPassed) {
    // Insert Severity
    item = doc.createElement("TestSeverity");
    reportElement.appendChild(item);
    value = doc.createTextNode(""+this.getSeverityLevel());
    item.appendChild(value);
    
    // Insert Reference Details
    item = doc.createElement("ReferenceDetails");
    reportElement.appendChild(item);
    value = doc.createTextNode(""+this.getReferenceDetails());
    item.appendChild(value);
    
    // Insert Prescription Advice
    item = doc.createElement("TestPrescription");
    String val = this.getPrescription();
    if (val == null) {
      val = XMLUtils.getElementValue("Prescription", this._testConfigElement);
    }
    reportElement.appendChild(item);
    value = doc.createTextNode(""+val);
    item.appendChild(value);
    // }
    return (Element)reportElement;
  }
  
  
  /* 
   * (non-Javadoc)
   * @see java.lang.Runnable#run()
   */
  public void run() {
    /*
     * Evaluate the test
     */
    this._impactLevel = this.evaluate(this._jobExecutionStats);
    this._evaluated = true;
    try {
      if (this._impactLevel >= this.getSuccessThreshold()) {
      this._testPassed = false;
      } else {
      this._testPassed = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  /*
   * Returns value of element of type long part of InputElement of diagnostic 
   * rule
   */
  protected long getInputElementLongValue (String elementName, long defaultValue) {
    Element inputElement = (Element)(this._testConfigElement.getElementsByTagName("InputElement").item(0));
    Element prs = null; long value;
    prs = (Element)inputElement.getElementsByTagName(elementName).item(0);
    if (prs != null) {
      value = Long.parseLong(prs.getFirstChild().getNodeValue().trim());
    } else {
      value = defaultValue;
    }
    return value;
  }
  
  /*
   * Returns value of element of type double part of InputElement of diagnostic rule
   */
  protected double getInputElementDoubleValue(String elementName, double defaultValue) {
    Element inputElement = (Element)(this._testConfigElement.getElementsByTagName("InputElement").item(0));
    Element prs = null; double value;
    prs = (Element)inputElement.getElementsByTagName(elementName).item(0);
    if (prs != null) {
      value = Double.parseDouble(prs.getFirstChild().getNodeValue().trim());
    } else {
      value = defaultValue;
    }
    return value;
  }
  
  /*
   * Returns value of element of type String part of InputElement of diagnostic rule
   */
  protected String getInputElementStringValue(String elementName, String defaultValue) {
    Element inputElement = (Element)(this._testConfigElement.getElementsByTagName("InputElement").item(0));
    Element prs = null; String value;
    prs = (Element)inputElement.getElementsByTagName(elementName).item(0);
    if (prs != null) {
      value = prs.getFirstChild().getNodeValue().trim();
    } else {
      value = defaultValue;
    }
    return value;
  }
  
  /*
   * truncate doubles to 2 digit.
   */
  public static double truncate(double x)
  {
      long y=(long)(x*100);
      return (double)y/100;
  }
}
