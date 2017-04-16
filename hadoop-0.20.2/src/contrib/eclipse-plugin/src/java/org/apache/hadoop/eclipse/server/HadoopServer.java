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

package org.apache.hadoop.eclipse.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.eclipse.Activator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.swt.widgets.Display;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Representation of a Hadoop location, meaning of the master node (NameNode,
 * JobTracker).
 * 
 * <p>
 * This class does not create any SSH connection anymore. Tunneling must be
 * setup outside of Eclipse for now (using Putty or <tt>ssh -D&lt;port&gt;
 * &lt;host&gt;</tt>)
 * 
 * <p>
 * <em> TODO </em>
 * <li> Disable the updater if a location becomes unreachable or fails for
 * tool long
 * <li> Stop the updater on location's disposal/removal
 */

public class HadoopServer {

  /**
   * Frequency of location status observations expressed as the delay in ms
   * between each observation
   * 
   * TODO Add a preference parameter for this
   */
  protected static final long STATUS_OBSERVATION_DELAY = 1500;

  /**
   * 
   */
  public class LocationStatusUpdater extends Job {

    JobClient client = null;

    /**
     * Setup the updater
     */
    public LocationStatusUpdater() {
      super("Map/Reduce location status updater");
      this.setSystem(true);
    }

    /* @inheritDoc */
    @Override
    protected IStatus run(IProgressMonitor monitor) {
      if (client == null) {
        try {
          client = HadoopServer.this.getJobClient();

        } catch (IOException ioe) {
          client = null;
          return new Status(Status.ERROR, Activator.PLUGIN_ID, 0,
              "Cannot connect to the Map/Reduce location: "
                            + HadoopServer.this.getLocationName(),
                            ioe);
        }
      }

      try {
        // Set of all known existing Job IDs we want fresh info of
        Set<JobID> missingJobIds =
            new HashSet<JobID>(runningJobs.keySet());

        JobStatus[] jstatus = client.jobsToComplete();
        for (JobStatus status : jstatus) {

          JobID jobId = status.getJobID();
          missingJobIds.remove(jobId);

          HadoopJob hJob;
          synchronized (HadoopServer.this.runningJobs) {
            hJob = runningJobs.get(jobId);
            if (hJob == null) {
              // Unknown job, create an entry
              RunningJob running = client.getJob(jobId);
              hJob =
                  new HadoopJob(HadoopServer.this, jobId, running, status);
              newJob(hJob);
            }
          }

          // Update HadoopJob with fresh infos
          updateJob(hJob, status);
        }

        // Ask explicitly for fresh info for these Job IDs
        for (JobID jobId : missingJobIds) {
          HadoopJob hJob = runningJobs.get(jobId);
          if (!hJob.isCompleted())
            updateJob(hJob, null);
        }

      } catch (IOException ioe) {
        client = null;
        return new Status(Status.ERROR, Activator.PLUGIN_ID, 0,
            "Cannot retrieve running Jobs on location: "
                          + HadoopServer.this.getLocationName(), ioe);
      }

      // Schedule the next observation
      schedule(STATUS_OBSERVATION_DELAY);

      return Status.OK_STATUS;
    }

    /**
     * Stores and make the new job available
     * 
     * @param data
     */
    private void newJob(final HadoopJob data) {
      runningJobs.put(data.getJobID(), data);

      Display.getDefault().asyncExec(new Runnable() {
        public void run() {
          fireJobAdded(data);
        }
      });
    }

    /**
     * Updates the status of a job
     * 
     * @param job the job to update
     */
    private void updateJob(final HadoopJob job, JobStatus status) {
      job.update(status);

      Display.getDefault().asyncExec(new Runnable() {
        public void run() {
          fireJobChanged(job);
        }
      });
    }

  }

  static Logger log = Logger.getLogger(HadoopServer.class.getName());

  /**
   * Hadoop configuration of the location. Also contains specific parameters
   * for the plug-in. These parameters are prefix with eclipse.plug-in.*
   */
  private Configuration conf;

  /**
   * Jobs listeners
   */
  private Set<IJobListener> jobListeners = new HashSet<IJobListener>();

  /**
   * Jobs running on this location. The keys of this map are the Job IDs.
   */
  private transient Map<JobID, HadoopJob> runningJobs =
      Collections.synchronizedMap(new TreeMap<JobID, HadoopJob>());

  /**
   * Status updater for this location
   */
  private LocationStatusUpdater statusUpdater;

  // state and status - transient
  private transient String state = "";

  /**
   * Creates a new default Hadoop location
   */
  public HadoopServer() {
    this.conf = new Configuration();
    this.addPluginConfigDefaultProperties();
  }

  /**
   * Creates a location from a file
   * 
   * @throws IOException
   * @throws SAXException
   * @throws ParserConfigurationException
   */
  public HadoopServer(File file) throws ParserConfigurationException,
      SAXException, IOException {

    this.conf = new Configuration();
    this.addPluginConfigDefaultProperties();
    this.loadFromXML(file);
  }

  /**
   * Create a new Hadoop location by copying an already existing one.
   * 
   * @param source the location to copy
   */
  public HadoopServer(HadoopServer existing) {
    this();
    this.load(existing);
  }

  public void addJobListener(IJobListener l) {
    jobListeners.add(l);
  }

  public void dispose() {
    // TODO close DFS connections?
  }

  /**
   * List all elements that should be present in the Server window (all
   * servers and all jobs running on each servers)
   * 
   * @return collection of jobs for this location
   */
  public Collection<HadoopJob> getJobs() {
    startStatusUpdater();
    return this.runningJobs.values();
  }

  /**
   * Remove the given job from the currently running jobs map
   * 
   * @param job the job to remove
   */
  public void purgeJob(final HadoopJob job) {
    runningJobs.remove(job.getJobID());
    Display.getDefault().asyncExec(new Runnable() {
      public void run() {
        fireJobRemoved(job);
      }
    });
  }

  /**
   * Returns the {@link Configuration} defining this location.
   * 
   * @return the location configuration
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Gets a Hadoop configuration property value
   * 
   * @param prop the configuration property
   * @return the property value
   */
  public String getConfProp(ConfProp prop) {
    return prop.get(conf);
  }

  /**
   * Gets a Hadoop configuration property value
   * 
   * @param propName the property name
   * @return the property value
   */
  public String getConfProp(String propName) {
    return this.conf.get(propName);
  }

  public String getLocationName() {
    return ConfProp.PI_LOCATION_NAME.get(conf);
  }

  /**
   * Returns the master host name of the Hadoop location (the Job tracker)
   * 
   * @return the host name of the Job tracker
   */
  public String getMasterHostName() {
    return getConfProp(ConfProp.PI_JOB_TRACKER_HOST);
  }

  public String getState() {
    return state;
  }

  /**
   * Overwrite this location with the given existing location
   * 
   * @param existing the existing location
   */
  public void load(HadoopServer existing) {
    this.conf = new Configuration(existing.conf);
  }

  /**
   * Overwrite this location with settings available in the given XML file.
   * The existing configuration is preserved if the XML file is invalid.
   * 
   * @param file the file path of the XML file
   * @return validity of the XML file
   * @throws ParserConfigurationException
   * @throws IOException
   * @throws SAXException
   */
  public boolean loadFromXML(File file) throws ParserConfigurationException,
      SAXException, IOException {

    Configuration newConf = new Configuration(this.conf);

    DocumentBuilder builder =
        DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document document = builder.parse(file);

    Element root = document.getDocumentElement();
    if (!"configuration".equals(root.getTagName()))
      return false;
    NodeList props = root.getChildNodes();
    for (int i = 0; i < props.getLength(); i++) {
      Node propNode = props.item(i);
      if (!(propNode instanceof Element))
        continue;
      Element prop = (Element) propNode;
      if (!"property".equals(prop.getTagName()))
        return false;
      NodeList fields = prop.getChildNodes();
      String attr = null;
      String value = null;
      for (int j = 0; j < fields.getLength(); j++) {
        Node fieldNode = fields.item(j);
        if (!(fieldNode instanceof Element))
          continue;
        Element field = (Element) fieldNode;
        if ("name".equals(field.getTagName()))
          attr = ((Text) field.getFirstChild()).getData();
        if ("value".equals(field.getTagName()) && field.hasChildNodes())
          value = ((Text) field.getFirstChild()).getData();
      }
      if (attr != null && value != null)
        newConf.set(attr, value);
    }

    this.conf = newConf;
    return true;
  }

  /**
   * Sets a Hadoop configuration property value
   * 
   * @param prop the property
   * @param propvalue the property value
   */
  public void setConfProp(ConfProp prop, String propValue) {
    prop.set(conf, propValue);
  }

  /**
   * Sets a Hadoop configuration property value
   * 
   * @param propName the property name
   * @param propValue the property value
   */
  public void setConfProp(String propName, String propValue) {
    this.conf.set(propName, propValue);
  }

  public void setLocationName(String newName) {
    ConfProp.PI_LOCATION_NAME.set(conf, newName);
  }

  /**
   * Write this location settings to the given output stream
   * 
   * @param out the output stream
   * @throws IOException
   */
  public void storeSettingsToFile(File file) throws IOException {
    FileOutputStream fos = new FileOutputStream(file);
    this.conf.writeXml(fos);
    fos.close();
  }

  /* @inheritDoc */
  @Override
  public String toString() {
    return this.getLocationName();
  }

  /**
   * Fill the configuration with valid default values
   */
  private void addPluginConfigDefaultProperties() {
    for (ConfProp prop : ConfProp.values()) {
      if (conf.get(prop.name) == null)
        conf.set(prop.name, prop.defVal);
    }
  }

  /**
   * Starts the location status updater
   */
  private synchronized void startStatusUpdater() {
    if (statusUpdater == null) {
      statusUpdater = new LocationStatusUpdater();
      statusUpdater.schedule();
    }
  }

  /*
   * Rewrite of the connecting and tunneling to the Hadoop location
   */

  /**
   * Provides access to the default file system of this location.
   * 
   * @return a {@link FileSystem}
   */
  public FileSystem getDFS() throws IOException {
    return FileSystem.get(this.conf);
  }

  /**
   * Provides access to the Job tracking system of this location
   * 
   * @return a {@link JobClient}
   */
  public JobClient getJobClient() throws IOException {
    JobConf jconf = new JobConf(this.conf);
    return new JobClient(jconf);
  }

  /*
   * Listeners handling
   */

  protected void fireJarPublishDone(JarModule jar) {
    for (IJobListener listener : jobListeners) {
      listener.publishDone(jar);
    }
  }

  protected void fireJarPublishStart(JarModule jar) {
    for (IJobListener listener : jobListeners) {
      listener.publishStart(jar);
    }
  }

  protected void fireJobAdded(HadoopJob job) {
    for (IJobListener listener : jobListeners) {
      listener.jobAdded(job);
    }
  }

  protected void fireJobRemoved(HadoopJob job) {
    for (IJobListener listener : jobListeners) {
      listener.jobRemoved(job);
    }
  }

  protected void fireJobChanged(HadoopJob job) {
    for (IJobListener listener : jobListeners) {
      listener.jobChanged(job);
    }
  }

}
