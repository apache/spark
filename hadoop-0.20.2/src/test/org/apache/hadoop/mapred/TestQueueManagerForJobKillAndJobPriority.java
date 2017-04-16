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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.security.UserGroupInformation;

public class TestQueueManagerForJobKillAndJobPriority extends TestQueueManager {

  public void testOwnerAllowedForJobKill() 
  throws IOException, InterruptedException {
    try {
      final UserGroupInformation ugi = createNecessaryUsers();
    
      ugi.doAs(new PrivilegedExceptionAction<Object>() {

                 @Override
                   public Object run() throws Exception {

                   JobConf conf
                     = setupConf(QueueManager.toFullPropertyName
                                 ("default", adminAcl), "junk-user");
                   verifyJobKill(ugi, conf, true);
                   return null;
                 }
               });
    } finally {
      tearDownCluster();
    }
  }
  
  public void testUserDisabledACLForJobKill() 
  throws IOException, InterruptedException {
    try {
      UserGroupInformation ugi = createNecessaryUsers();
      // create other user who will try to kill the job of ugi.
      final UserGroupInformation otherUGI = UserGroupInformation.
        createUserForTesting("user1", new String [] {"group1"});

      ugi.doAs(new PrivilegedExceptionAction<Object>() {

                 @Override
                   public Object run() throws Exception {
                   //setup a cluster allowing a user to submit
                   JobConf conf
                     = setupConf(QueueManager.toFullPropertyName
                                 ("default", adminAcl), "dummy-user");
                   // Run job as ugi and try to kill job as user1, who (obviously)
                   // should not able to kill the job.
                   verifyJobKill(otherUGI, conf, false);
                   return null;
                 }
               });
    } finally {
      tearDownCluster();
    }
  }
  
  public void testUserEnabledACLForJobKill() 
  throws IOException, LoginException, InterruptedException {
    try {
      UserGroupInformation ugi = createNecessaryUsers();
      // create other user who will try to kill the job of ugi.
      final UserGroupInformation otherUGI = UserGroupInformation.
        createUserForTesting("user1", new String [] {"group1"});

      ugi.doAs(new PrivilegedExceptionAction<Object>() {
                 @Override
                   public Object run() throws Exception {
                   UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                   JobConf conf
                     = setupConf(QueueManager.toFullPropertyName
                                 ("default", adminAcl), "user1");
                   // user1 should be able to kill the job
                   verifyJobKill(otherUGI, conf, true);
                   return null;
                 }
               });
    } finally {
      tearDownCluster();
    }
  }

  public void testUserDisabledForJobPriorityChange() 
  throws IOException, InterruptedException {
    try {
      UserGroupInformation ugi = createNecessaryUsers();
      // create other user who will try to change priority of the job of ugi.
      final UserGroupInformation otherUGI = UserGroupInformation.
        createUserForTesting("user1", new String [] {"group1"});

      ugi.doAs(new PrivilegedExceptionAction<Object>() {

                 @Override
                   public Object run() throws Exception {

                   JobConf conf
                     = setupConf(QueueManager.toFullPropertyName
                                 ("default", adminAcl), "junk-user");
                   verifyJobPriorityChangeAsOtherUser(otherUGI, conf, false);
                   return null;
                 }
               });
    } finally {
      tearDownCluster();
    }
  }

  /**
   * Test to verify refreshing of queue properties by using MRAdmin tool.
   * 
   * @throws Exception
   */
  public void testACLRefresh() throws Exception {
    try {
      String queueConfigPath =
        System.getProperty("test.build.extraconf", "build/test/extraconf");
      File queueConfigFile =
        new File(queueConfigPath, QueueManager.QUEUE_ACLS_FILE_NAME);
      File hadoopConfigFile = new File(queueConfigPath, "mapred-site.xml");
      try {
        //Setting up default mapred-site.xml
        Properties hadoopConfProps = new Properties();
        //these properties should be retained.
        hadoopConfProps.put("mapred.queue.names", "default,q1,q2");
        hadoopConfProps.put(JobConf.MR_ACLS_ENABLED, "true");
        //These property should always be overridden
        hadoopConfProps.put(QueueManager.toFullPropertyName
                            ("default", submitAcl), "u1");
        hadoopConfProps.put(QueueManager.toFullPropertyName
                            ("q1", submitAcl), "u2");
        hadoopConfProps.put(QueueManager.toFullPropertyName
                            ("q2", submitAcl), "u1");
        UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);
      
        //Actual property which would be used.
        Properties queueConfProps = new Properties();
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("default", submitAcl), " ");
        //Writing out the queue configuration file.
        UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);
      
        //Create a new configuration to be used with QueueManager
        JobConf conf = new JobConf();
        QueueManager queueManager = new QueueManager(conf);
        UserGroupInformation ugi = UserGroupInformation.
          createUserForTesting("user1", new String [] {"group1"});

        //Job Submission should fail because ugi to be used is set to blank.
        assertFalse("User Job Submission Succeeded before refresh.",
                    queueManager.hasAccess("default", QueueACL.SUBMIT_JOB, ugi));
        assertFalse("User Job Submission Succeeded before refresh.",
                    queueManager.hasAccess("q1", QueueACL.SUBMIT_JOB, ugi));
        assertFalse("User Job Submission Succeeded before refresh.",
                    queueManager.hasAccess("q2", QueueACL.SUBMIT_JOB, ugi));
      
        //Test job submission as alternate user.
        UserGroupInformation alternateUgi = 
          UserGroupInformation.createUserForTesting("u1", new String[]{"user"});
        assertTrue("Alternate User Job Submission failed before refresh.",
                   queueManager.hasAccess("q2", QueueACL.SUBMIT_JOB, alternateUgi));
      
        //Set acl for user1.
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("default", submitAcl), ugi.getShortUserName());
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("q1", submitAcl), ugi.getShortUserName());
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("q2", submitAcl), ugi.getShortUserName());
        //write out queue-acls.xml.
        UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);
        //refresh configuration
        queueManager.refreshQueues(conf);
        //Submission should succeed
        assertTrue("User Job Submission failed after refresh.",
                   queueManager.hasAccess("default", QueueACL.SUBMIT_JOB, ugi));
        assertTrue("User Job Submission failed after refresh.",
                   queueManager.hasAccess("q1", QueueACL.SUBMIT_JOB, ugi));
        assertTrue("User Job Submission failed after refresh.",
                   queueManager.hasAccess("q2", QueueACL.SUBMIT_JOB, ugi));
        assertFalse("Alternate User Job Submission succeeded after refresh.",
                    queueManager.hasAccess("q2", QueueACL.SUBMIT_JOB, alternateUgi));
        //delete the ACL file.
        queueConfigFile.delete();
      
        //rewrite the mapred-site.xml
        hadoopConfProps.put(JobConf.MR_ACLS_ENABLED, "true");
        hadoopConfProps.put(QueueManager.toFullPropertyName
                            ("q1", submitAcl), ugi.getShortUserName());
        UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);
        queueManager.refreshQueues(conf);
        assertTrue("User Job Submission allowed after refresh and no queue acls file.",
                   queueManager.hasAccess("q1", QueueACL.SUBMIT_JOB, ugi));
      } finally{
        if(queueConfigFile.exists()) {
          queueConfigFile.delete();
        }
        if(hadoopConfigFile.exists()) {
          hadoopConfigFile.delete();
        }
      }
    } finally {
      tearDownCluster();
    }
  }

  public void testQueueAclRefreshWithInvalidConfFile() throws IOException {
    try {
      String queueConfigPath =
        System.getProperty("test.build.extraconf", "build/test/extraconf");
      File queueConfigFile =
        new File(queueConfigPath, QueueManager.QUEUE_ACLS_FILE_NAME );
      File hadoopConfigFile = new File(queueConfigPath, "hadoop-site.xml");
      try {
        // queue properties with which the cluster is started.
        Properties hadoopConfProps = new Properties();
        hadoopConfProps.put("mapred.queue.names", "default,q1,q2");
        hadoopConfProps.put(JobConf.MR_ACLS_ENABLED, "true");
        UtilsForTests.setUpConfigFile(hadoopConfProps, hadoopConfigFile);
      
        //properties for mapred-queue-acls.xml
        Properties queueConfProps = new Properties();
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("default", submitAcl), ugi.getShortUserName());
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("q1", submitAcl), ugi.getShortUserName());
        queueConfProps.put(QueueManager.toFullPropertyName
                           ("q2", submitAcl), ugi.getShortUserName());
        UtilsForTests.setUpConfigFile(queueConfProps, queueConfigFile);
      
        Configuration conf = new JobConf();
        QueueManager queueManager = new QueueManager(conf);
        //Testing access to queue.
        assertTrue("User Job Submission failed.",
                   queueManager.hasAccess("default", QueueACL.SUBMIT_JOB, ugi));
        assertTrue("User Job Submission failed.",
                   queueManager.hasAccess("q1", QueueACL.SUBMIT_JOB, ugi));
        assertTrue("User Job Submission failed.",
                   queueManager.hasAccess("q2", QueueACL.SUBMIT_JOB, ugi));
      
        //Write out a new incomplete invalid configuration file.
        PrintWriter writer = new PrintWriter(new FileOutputStream(queueConfigFile));
        writer.println("<configuration>");
        writer.println("<property>");
        writer.flush();
        writer.close();
        try {
          //Exception to be thrown by queue manager because configuration passed
          //is invalid.
          queueManager.refreshQueues(conf);
          fail("Refresh of ACLs should have failed with invalid conf file.");
        } catch (Exception e) {
        }
        assertTrue("User Job Submission failed after invalid conf file refresh.",
                   queueManager.hasAccess("default", QueueACL.SUBMIT_JOB, ugi));
        assertTrue("User Job Submission failed after invalid conf file refresh.",
                   queueManager.hasAccess("q1", QueueACL.SUBMIT_JOB, ugi));
        assertTrue("User Job Submission failed after invalid conf file refresh.",
                   queueManager.hasAccess("q2", QueueACL.SUBMIT_JOB, ugi));
      } finally {
        //Cleanup the configuration files in all cases
        if(hadoopConfigFile.exists()) {
          hadoopConfigFile.delete();
        }
        if(queueConfigFile.exists()) {
          queueConfigFile.delete();
        }
      }
    } finally {
      tearDownCluster();
    }
  }

  public void testGroupsEnabledACLForJobSubmission() 
  throws IOException, LoginException, InterruptedException {
    try {
      // login as self, get one group, and add in allowed list.
      UserGroupInformation ugi = createNecessaryUsers();

      String[] groups = ugi.getGroupNames();
      JobConf conf
        = setupConf(QueueManager.toFullPropertyName
                    ("default", submitAcl), "3698-junk-user1,3698-junk-user2 " 
                    + groups[groups.length-1] 
                    + ",3698-junk-group");
      verifyJobSubmissionToDefaultQueue
        (conf, true, ugi.getShortUserName()+","+groups[groups.length-1]);
    } finally {
      tearDownCluster();
    }
  }

}
