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

import java.io.PrintWriter;
import java.io.Writer;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Queue.QueueState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;

/**
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * 
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some 
 * schedulers work only with one queue, some schedulers support multiple 
 * queues.
 *  
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with 
 * queues. Where such a case exists, the queue name must be used to link 
 * the common properties with the scheduler specific ones.  
 */
class QueueManager {
  
  private static final Log LOG = LogFactory.getLog(QueueManager.class);
  
  static final String QUEUE_STATE_SUFFIX = "state";
  /** Prefix in configuration for queue related keys */
  static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = "mapred.queue.";

  // Continue to add this resource, to avoid incompatible change
  static final String QUEUE_ACLS_FILE_NAME = "mapred-queue-acls.xml";

  /** Whether ACLs are enabled in the system or not. */
  private boolean aclsEnabled;
  /** Map of a queue name and Queue object */
  final HashMap<String,Queue> queues = new HashMap<String,Queue>();
  
  /**
   * Enum representing an AccessControlList that drives set of operations that
   * can be performed on a queue.
   */
  static enum QueueACL {
    SUBMIT_JOB ("acl-submit-job"),
    ADMINISTER_JOBS ("acl-administer-jobs");
    // Currently this ACL acl-administer-jobs is checked for the operations
    // FAIL_TASK, KILL_TASK, KILL_JOB, SET_JOB_PRIORITY and VIEW_JOB.

    // TODO: Add ACL for LIST_JOBS when we have ability to authenticate 
    //       users in UI
    // TODO: Add ACL for CHANGE_ACL when we have an admin tool for 
    //       configuring queues.
    
    private final String aclName;
    
    QueueACL(String aclName) {
      this.aclName = aclName;
    }

    final String getAclName() {
      return aclName;
    }
    
  }
  
  /**
   * Construct a new QueueManager using configuration specified in the passed
   * in {@link org.apache.hadoop.conf.Configuration} object.
   * 
   * @param conf Configuration object where queue configuration is specified.
   */
  public QueueManager(Configuration conf) {
    checkDeprecation(conf);
    conf.addResource(QUEUE_ACLS_FILE_NAME);
    
    // Get configured ACLs and state for each queue
    aclsEnabled = conf.getBoolean("mapred.acls.enabled", false);

    queues.putAll(parseQueues(conf)); 
  }
  
  synchronized private Map<String, Queue> parseQueues(Configuration conf) {
    Map<String, Queue> queues = new HashMap<String, Queue>();
    // First get the queue names
    String[] queueNameValues = conf.getStrings("mapred.queue.names",
        new String[]{JobConf.DEFAULT_QUEUE_NAME});
    for (String name : queueNameValues) {
      queues.put(name, new Queue(name, getQueueAcls(name, conf),
          getQueueState(name, conf)));
    }
    
    return queues;
  }
  
  /**
   * Return the set of queues configured in the system.
   * 
   * The number of queues configured should be dependent on the Scheduler 
   * configured. Note that some schedulers work with only one queue, whereas
   * others can support multiple queues.
   *  
   * @return Set of queue names.
   */
  public synchronized Set<String> getQueues() {
    return queues.keySet();
  }
  
  /**
   * Return true if the given user is part of the ACL for the given
   * {@link QueueACL} name for the given queue.
   * 
   * An operation is allowed if all users are provided access for this
   * operation, or if either the user or any of the groups specified is
   * provided access.
   * 
   * @param queueName Queue on which the operation needs to be performed. 
   * @param qACL The queue ACL name to be checked
   * @param ugi The user and groups who wish to perform the operation.
   * 
   * @return true if the operation is allowed, false otherwise.
   */
  public synchronized boolean hasAccess(String queueName, QueueACL qACL,
      UserGroupInformation ugi) {
    if (!aclsEnabled) {
      return true;
    }
    final Queue q = queues.get(queueName);
    if (null == q) {
      LOG.info("Queue " + queueName + " is not present");
      return false;
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("checking access for : " +
          toFullPropertyName(queueName, qACL.getAclName()));
    }

    AccessControlList acl =
      q.getAcls().get(toFullPropertyName(queueName, qACL.getAclName()));

    // Check if user is part of the ACL
    return acl != null && acl.isUserAllowed(ugi);
  }

  /**
   * Checks whether the given queue is running or not.
   * @param queueName name of the queue
   * @return true, if the queue is running.
   */
  synchronized boolean isRunning(String queueName) {
    Queue q = queues.get(queueName);
    return q != null && Queue.QueueState.RUNNING.equals(q.getState());
  }

  /**
   * Set a generic Object that represents scheduling information relevant
   * to a queue.
   * 
   * A string representation of this Object will be used by the framework
   * to display in user facing applications like the JobTracker web UI and
   * the hadoop CLI.
   * 
   * @param queueName queue for which the scheduling information is to be set. 
   * @param queueInfo scheduling information for this queue.
   */
  public synchronized void setSchedulerInfo(String queueName, 
                                              Object queueInfo) {
    Queue q = queues.get(queueName);
    if (q != null) {
      q.setSchedulingInfo(queueInfo);
    }
  }
  
  /**
   * Return the scheduler information configured for this queue.
   * 
   * @param queueName queue for which the scheduling information is required.
   * @return The scheduling information for this queue.
   * 
   * @see #setSchedulerInfo(String, Object)
   */
  public synchronized Object getSchedulerInfo(String queueName) {
    Queue q = queues.get(queueName);
    return (q != null)
      ? q.getSchedulingInfo()
      : null;
  }
  
  /**
   * Refresh the acls for the configured queues in the system by reading
   * it from mapred-queue-acls.xml.
   * 
   * The previous acls are removed. Previously configured queues and
   * if or not acl is disabled is retained.
   * 
   * @throws IOException when queue ACL configuration file is invalid.
   */
  synchronized void refreshQueues(Configuration conf) throws IOException {
    
    // First check if things are configured in mapred-site.xml,
    // so we can print out a deprecation warning.
    // This check is needed only until we support the configuration
    // in mapred-site.xml
    checkDeprecation(conf);

    // Add the queue configuration file. Values from mapred-site.xml
    // will be overridden.
    conf.addResource(QUEUE_ACLS_FILE_NAME);

    // Now parse the queues and check to ensure no queue has been deleted
    Map<String, Queue> newQueues = parseQueues(conf);
    checkQueuesForDeletion(queues, newQueues);

    // Now we refresh the properties of the queues. Note that we
    // do *not* refresh the queue names or the acls flag. Instead
    // we use the older values configured for them.
    queues.clear();
    queues.putAll(newQueues);
    LOG.info("Queues acls, state and configs refreshed: " + 
        queues.size() + " queues present now.");
  }

  private void checkQueuesForDeletion(Map<String, Queue> currentQueues,
      Map<String, Queue> newQueues) {
    for (String queue : currentQueues.keySet()) {
      if (!newQueues.containsKey(queue)) {
        throw new IllegalArgumentException("Couldn't find queue '" + queue + 
            "' during refresh!");
      }
    }
    
    // Mark new queues as STOPPED
    for (String queue : newQueues.keySet()) {
      if (!currentQueues.containsKey(queue)) {
        newQueues.get(queue).setState(QueueState.STOPPED);
      }
    }
  }
  
  private void checkDeprecation(Configuration conf) {
    // check if queues are defined.
    String[] queues = conf.getStrings("mapred.queue.names");
    // check if acls are defined
    if (queues != null) {
      for (String queue : queues) {
        for (QueueACL oper : QueueACL.values()) {
          String aclString =
            conf.get(toFullPropertyName(queue, oper.getAclName()));
          if (aclString != null) {
            LOG.warn("Configuring queue ACLs in mapred-site.xml or " +
                "hadoop-site.xml is deprecated. Configure queue ACLs in " +
                QUEUE_ACLS_FILE_NAME);
            // even if one string is configured, it is enough for printing
            // the warning. so we can return from here.
            return;
          }
        }
      }
    }
  }

  /** Parse ACLs for the queue from the configuration. */
  HashMap<String, AccessControlList> getQueueAcls(
      String name, Configuration conf) {
    HashMap<String,AccessControlList> map =
      new HashMap<String,AccessControlList>();
    for (QueueACL oper : QueueACL.values()) {
      String aclKey = toFullPropertyName(name, oper.getAclName());
      map.put(aclKey, new AccessControlList(conf.get(aclKey, "*")));
    }
    return map;
  }

  /** Parse state of the queue from the configuration. */
  Queue.QueueState getQueueState(String name, Configuration conf) {
    return conf.getEnum(
        toFullPropertyName(name, QueueManager.QUEUE_STATE_SUFFIX),
        Queue.QueueState.RUNNING);
  }

  static final String toFullPropertyName(String queue, String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }
  
  synchronized JobQueueInfo getJobQueueInfo(String queue) {
    Queue q = queues.get(queue);
    if (q != null) {
      JobQueueInfo qInfo = new JobQueueInfo();
      qInfo.setQueueName(q.getName());
      qInfo.setQueueState(q.getState().getStateName());
      Object schedInfo = q.getSchedulingInfo();
      qInfo.setSchedulingInfo(schedInfo == null ? null : schedInfo.toString());
      return qInfo;
    }
    return null;
  }

  synchronized JobQueueInfo[] getJobQueueInfos() {
    ArrayList<JobQueueInfo> ret = new ArrayList<JobQueueInfo>();
    for (String qName : getQueues()) {
      ret.add(getJobQueueInfo(qName));
    }
    return (JobQueueInfo[]) ret.toArray(new JobQueueInfo[ret.size()]);
  }

  /**
   * Generates the array of QueueAclsInfo object. The array consists of only those queues
   * for which user <ugi.getShortUserName()> has acls
   *
   * @return QueueAclsInfo[]
   * @throws java.io.IOException
   */
  synchronized QueueAclsInfo[] getQueueAcls(UserGroupInformation ugi)
      throws IOException {
    //List of all QueueAclsInfo objects , this list is returned
    ArrayList<QueueAclsInfo> queueAclsInfolist = new ArrayList<QueueAclsInfo>();
    QueueACL[] acls = QueueACL.values();
    for (String queueName : getQueues()) {
      QueueAclsInfo queueAclsInfo = null;
      ArrayList<String> operationsAllowed = null;
      for (QueueACL qACL : acls) {
        if (hasAccess(queueName, qACL, ugi)) {
          if (operationsAllowed == null) {
            operationsAllowed = new ArrayList<String>();
          }
          operationsAllowed.add(qACL.getAclName());
        }
      }
      if (operationsAllowed != null) {
        //There is atleast 1 operation supported for queue <queueName>
        //, hence initialize queueAclsInfo
        queueAclsInfo = new QueueAclsInfo(queueName, operationsAllowed.toArray(
              new String[operationsAllowed.size()]));
        queueAclsInfolist.add(queueAclsInfo);
      }
    }
    return
      queueAclsInfolist.toArray(new QueueAclsInfo[queueAclsInfolist.size()]);
  }

  /**
   * Returns the specific queue ACL for the given queue.
   * Returns null if the given queue does not exist or the acl is not
   * configured for that queue.
   * If acls are disabled(mapred.acls.enabled set to false), returns ACL with
   * all users.
   */
  synchronized AccessControlList getQueueACL(String queueName, QueueACL qACL) {
    if (aclsEnabled) {
      Queue q = queues.get(queueName);
      assert q != null;
      return q.getAcls().get(toFullPropertyName(queueName, qACL.getAclName()));
    }
    return new AccessControlList("*");
  }

  /**
   * prints the configuration of QueueManager in Json format.
   * The method should be modified accordingly whenever
   * QueueManager(Configuration) constructor is modified.
   * @param writer {@link}Writer object to which the configuration properties 
   * are printed in json format
   * @throws IOException
   */
  static void dumpConfiguration(Writer writer) throws IOException {
    Configuration conf = new Configuration(false);
    conf.addResource(QUEUE_ACLS_FILE_NAME);
    Configuration.dumpConfiguration(conf, writer);
  }

}
