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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.filecache.TaskDistributedCacheManager;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.RunJar;

/**
 * Internal class responsible for initializing the job, not intended for users.
 * Creates the following hierarchy:
 *   <li>$mapred.local.dir/taskTracker/$user</li>
 *   <li>$mapred.local.dir/taskTracker/$user/jobcache</li>
 *   <li>$mapred.local.dir/taskTracker/$user/jobcache/$jobid/work</li>
 *   <li>$mapred.local.dir/taskTracker/$user/jobcache/$jobid/jars</li>
 *   <li>$mapred.local.dir/taskTracker/$user/jobcache/$jobid/jars/job.jar</li>
 *   <li>$mapred.local.dir/taskTracker/$user/jobcache/$jobid/job.xml</li>
 *   <li>$mapred.local.dir/taskTracker/$user/jobcache/$jobid/jobToken</li>
 *   <li>$mapred.local.dir/taskTracker/$user/distcache</li>
 */
public class JobLocalizer {

  static final Log LOG = LogFactory.getLog(JobLocalizer.class);

  private static final FsPermission urwx =
    FsPermission.createImmutable((short) 0700);
  private static final FsPermission urwx_gx =
    FsPermission.createImmutable((short) 0710);
  private static final FsPermission urw_gr =
    FsPermission.createImmutable((short) 0640);

  private final String user;
  private final String jobid;
  private final FileSystem lfs;
  private final List<Path> localDirs;
  private final LocalDirAllocator lDirAlloc;
  private final JobConf ttConf;

  private final String JOBDIR;
  private final String DISTDIR;
  private final String WORKDIR;
  private final String JARDST;
  private final String JOBCONF;
  private final String JOBTOKEN;
  private static final String JOB_LOCAL_CTXT = "mapred.job.local.dir";

  public JobLocalizer(JobConf ttConf, String user, String jobid)
      throws IOException {
    this(ttConf, user, jobid,
        ttConf.getTrimmedStrings(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
  }

  public JobLocalizer(JobConf ttConf, String user, String jobid,
      String... localDirs) throws IOException {
    if (null == user) {
      throw new IOException("Cannot initialize for null user");
    }
    this.user = user;
    if (null == jobid) {
      throw new IOException("Cannot initialize for null jobid");
    }
    this.jobid = jobid;
    this.ttConf = ttConf;
    lfs = FileSystem.getLocal(ttConf).getRaw();
    this.localDirs = createPaths(user, localDirs);
    ttConf.setStrings(JOB_LOCAL_CTXT, localDirs);
    Collections.shuffle(this.localDirs);
    lDirAlloc = new LocalDirAllocator(JOB_LOCAL_CTXT);
    JOBDIR = TaskTracker.JOBCACHE + Path.SEPARATOR + jobid;
    DISTDIR = JOBDIR + "/" + TaskTracker.DISTCACHEDIR;
    WORKDIR = JOBDIR + "/work";
    JARDST = JOBDIR + "/" + TaskTracker.JARSDIR + "/job.jar";
    JOBCONF = JOBDIR + "/" + TaskTracker.JOBFILE;
    JOBTOKEN = JOBDIR + "/" + TaskTracker.JOB_TOKEN_FILE;
  }

  private static List<Path> createPaths(String user, final String[] str)
      throws IOException {
    if (null == str || 0 == str.length) {
      throw new IOException("mapred.local.dir contains no entries");
    }
    final List<Path> ret = new ArrayList<Path>(str.length);
    for (int i = 0; i < str.length; ++i) {
      final Path p = new Path(str[i], TaskTracker.getUserDir(user));
      ret.add(p);
      str[i] = p.toString();
    }
    return ret;
  }

  public void createLocalDirs() throws IOException {
    boolean userDirStatus = false;
    // create all directories as rwx------
    for (Path localDir : localDirs) {
      // create $mapred.local.dir/taskTracker/$user
      if (!lfs.mkdirs(localDir, urwx)) {
        LOG.warn("Unable to create the user directory : " + localDir);
        continue;
      }
      userDirStatus = true;
    }
    if (!userDirStatus) {
      throw new IOException("Not able to initialize user directories "
          + "in any of the configured local directories for user " + user);
    }
  }

  /**
   * Initialize the local directories for a particular user on this TT. This
   * involves creation and setting permissions of the following directories
   * <ul>
   * <li>$mapred.local.dir/taskTracker/$user</li>
   * <li>$mapred.local.dir/taskTracker/$user/jobcache</li>
   * <li>$mapred.local.dir/taskTracker/$user/distcache</li>
   * </ul>
   */
  public void createUserDirs() throws IOException {
    LOG.info("Initializing user " + user + " on this TT.");

    boolean jobCacheDirStatus = false;
    boolean distributedCacheDirStatus = false;

    // create all directories as rwx------
    for (Path localDir : localDirs) {
      // create $mapred.local.dir/taskTracker/$user/jobcache
      final Path jobDir =
        new Path(localDir, TaskTracker.JOBCACHE);
      if (!lfs.mkdirs(jobDir, urwx)) {
        LOG.warn("Unable to create job cache directory : " + jobDir);
      } else {
        jobCacheDirStatus = true;
      }
      // create $mapred.local.dir/taskTracker/$user/distcache
      final Path distDir =
        new Path(localDir, TaskTracker.DISTCACHEDIR);
      if (!lfs.mkdirs(distDir, urwx)) {
        LOG.warn("Unable to create distributed-cache directory : " + distDir);
      } else {
        distributedCacheDirStatus = true;
      }
    }
    if (!jobCacheDirStatus) {
      throw new IOException("Not able to initialize job-cache directories "
          + "in any of the configured local directories for user " + user);
    }
    if (!distributedCacheDirStatus) {
      throw new IOException(
          "Not able to initialize distributed-cache directories "
              + "in any of the configured local directories for user "
              + user);
    }
  }

  /**
   * Prepare the job directories for a given job. To be called by the job
   * localization code, only if the job is not already localized.
   * <br>
   * Here, we set 700 permissions on the job directories created on all disks.
   * This we do so as to avoid any misuse by other users till the time
   * {@link TaskController#initializeJob} is run at a
   * later time to set proper private permissions on the job directories. <br>
   */
  public void createJobDirs() throws IOException {
    boolean initJobDirStatus = false;
    for (Path localDir : localDirs) {
      Path fullJobDir = new Path(localDir, JOBDIR);
      if (lfs.exists(fullJobDir)) {
        // this will happen on a partial execution of localizeJob. Sometimes
        // copying job.xml to the local disk succeeds but copying job.jar might
        // throw out an exception. We should clean up and then try again.
        lfs.delete(fullJobDir, true);
      }
      // create $mapred.local.dir/taskTracker/$user/jobcache/$jobid
      if (!lfs.mkdirs(fullJobDir, urwx)) {
        LOG.warn("Not able to create job directory " + fullJobDir.toString());
      } else {
        initJobDirStatus = true;
      }
    }
    if (!initJobDirStatus) {
      throw new IOException("Not able to initialize job directories "
          + "in any of the configured local directories for job "
          + jobid.toString());
    }
  }

  /**
   * Create job log directory and set appropriate permissions for the directory.
   */
  public void initializeJobLogDir() throws IOException {
    Path jobUserLogDir = new Path(TaskLog.getJobDir(jobid).toURI().toString());
    if (!lfs.mkdirs(jobUserLogDir, urwx_gx)) {
      throw new IOException(
          "Could not create job user log directory: " + jobUserLogDir);
    }
  }

  /**
   * Download the job jar file from FS to the local file system and unjar it.
   * Set the local jar file in the passed configuration.
   *
   * @param localJobConf
   * @throws IOException
   */
  private void localizeJobJarFile(JobConf localJobConf) throws IOException {
    // copy Jar file to the local FS and unjar it.
    String jarFile = localJobConf.getJar();
    FileStatus status = null;
    long jarFileSize = -1;
    if (jarFile != null) {
      Path jarFilePath = new Path(jarFile);
      FileSystem userFs = jarFilePath.getFileSystem(localJobConf);
      try {
        status = userFs.getFileStatus(jarFilePath);
        jarFileSize = status.getLen();
      } catch (FileNotFoundException fe) {
        jarFileSize = -1;
      }
      // Here we check for five times the size of jarFileSize to accommodate for
      // unjarring the jar file in the jars directory
      Path localJarFile =
        lDirAlloc.getLocalPathForWrite(JARDST, 5 * jarFileSize, ttConf);

      //Download job.jar
      userFs.copyToLocalFile(jarFilePath, localJarFile);
      localJobConf.setJar(localJarFile.toString());
      // also unjar the parts of the job.jar that need to end up on the
      // classpath, or explicitly requested by the user.
      RunJar.unJar(
        new File(localJarFile.toString()),
        new File(localJarFile.getParent().toString()),
        localJobConf.getJarUnpackPattern());
      try {
        FileUtil.chmod(localJarFile.getParent().toString(), "ugo+rx", true);
      } catch (InterruptedException ie) {
        // This exception is never actually thrown, but the signature says
        // it is, and we can't make the incompatible change within CDH
        throw new IOException("Interrupted while chmodding", ie);
      }
    }
  }

  /**
   * The permissions to use for the private distributed cache objects.
   * It is already protected by the user directory, so keep the group and other
   * the same so that LocalFileSystem will use the java File methods to
   * set permission.
   */
  private static final FsPermission privateCachePerms =
    FsPermission.createImmutable((short) 0755);
  
  /**
   * Given a list of objects, download each one.
   * @param conf the job's configuration
   * @param sources the list of objects to download from
   * @param dests the list of paths to download them to
   * @param times the desired modification times
   * @param isPublic are the objects in the public cache?
   * @param isArchive are these archive files?
   * @throws IOException
   * @return for archives, return the list of each of the sizes.
   */
  private static long[] downloadPrivateCacheObjects(Configuration conf,
                                             URI[] sources,
                                             Path[] dests,
                                             long[] times,
                                             boolean[] isPublic,
                                             boolean isArchive
                                             ) throws IOException {
    if (null == sources && null == dests && null == times && null == isPublic) {
      return null;
    }
    if (sources.length != dests.length ||
        sources.length != times.length ||
        sources.length != isPublic.length) {
      throw new IOException("Distributed cache entry arrays have different " +
                            "lengths: " + sources.length + ", " + dests.length +
                            ", " + times.length + ", " + isPublic.length);
    }
    long[] result = new long[sources.length];
    for(int i=0; i < sources.length; i++) {
      // public objects are already downloaded by the Task Tracker, we
      // only need to handle the private ones here
      if (!isPublic[i]) {
        result[i] = 
          TrackerDistributedCacheManager.downloadCacheObject(conf, sources[i], 
                                                             dests[i], 
                                                             times[i], 
                                                             isArchive, 
                                                             privateCachePerms);
      }
    }
    return result;
  }

  /**
   * Download the parts of the distributed cache that are private.
   * @param conf the job's configuration
   * @throws IOException
   * @return the size of the archive objects
   */
  public static long[] downloadPrivateCache(Configuration conf) throws IOException {
    downloadPrivateCacheObjects(conf,
                                DistributedCache.getCacheFiles(conf),
                                DistributedCache.getLocalCacheFiles(conf),
                                DistributedCache.getFileTimestamps(conf),
                                TrackerDistributedCacheManager.
                                  getFileVisibilities(conf),
                                false);
    return 
      downloadPrivateCacheObjects(conf,
                                  DistributedCache.getCacheArchives(conf),
                                  DistributedCache.getLocalCacheArchives(conf),
                                  DistributedCache.getArchiveTimestamps(conf),
                                  TrackerDistributedCacheManager.
                                    getArchiveVisibilities(conf),
                                  true);
  }

  public void localizeJobFiles(JobID jobid, JobConf jConf,
      Path localJobTokenFile, TaskUmbilicalProtocol taskTracker)
      throws IOException, InterruptedException {
    localizeJobFiles(jobid, jConf,
        lDirAlloc.getLocalPathForWrite(JOBCONF, ttConf), localJobTokenFile,
        taskTracker);
  }

  public void localizeJobFiles(final JobID jobid, JobConf jConf,
      Path localJobFile, Path localJobTokenFile,
      final TaskUmbilicalProtocol taskTracker) 
  throws IOException, InterruptedException {
    // Download the job.jar for this job from the system FS
    localizeJobJarFile(jConf);

    jConf.set(JOB_LOCAL_CTXT, ttConf.get(JOB_LOCAL_CTXT));

    //update the config some more
    jConf.set(TokenCache.JOB_TOKENS_FILENAME, localJobTokenFile.toString());
    jConf.set(JobConf.MAPRED_LOCAL_DIR_PROPERTY, 
        ttConf.get(JobConf.MAPRED_LOCAL_DIR_PROPERTY));
    TaskTracker.resetNumTasksPerJvm(jConf);

    //setup the distributed cache
    final long[] sizes = downloadPrivateCache(jConf);
    if (sizes != null) {
      //the following doAs is required because the DefaultTaskController
      //calls the localizeJobFiles method in the context of the TaskTracker
      //process. The JVM authorization check would fail without this
      //doAs. In the LinuxTC case, this doesn't harm.
      UserGroupInformation ugi = 
        UserGroupInformation.createRemoteUser(jobid.toString());
      ugi.doAs(new PrivilegedExceptionAction<Object>() { 
        public Object run() throws IOException {
          taskTracker.updatePrivateDistributedCacheSizes(jobid, sizes);
          return null;
        }
      });
      
    }

    // Create job-acls.xml file in job userlog dir and write the needed
    // info for authorization of users for viewing task logs of this job.
    writeJobACLs(jConf, new Path(TaskLog.getJobDir(jobid).toURI().toString()));

    //write the updated jobConf file in the job directory
    JobLocalizer.writeLocalJobFile(localJobFile, jConf);
  }

  /**
   *  Creates job-acls.xml under the given directory logDir and writes
   *  job-view-acl, queue-admins-acl, jobOwner name and queue name into this
   *  file.
   *  queue name is the queue to which the job was submitted to.
   *  queue-admins-acl is the queue admins ACL of the queue to which this
   *  job was submitted to.
   * @param conf   job configuration
   * @param logDir job userlog dir
   * @throws IOException
   */
  private void writeJobACLs(JobConf conf, Path logDir) throws IOException {
    JobConf aclConf = new JobConf(false);

    // set the job view acl in aclConf
    String jobViewACL = conf.get(JobContext.JOB_ACL_VIEW_JOB, " ");
    aclConf.set(JobContext.JOB_ACL_VIEW_JOB, jobViewACL);

    // set the job queue name in aclConf
    String queue = conf.getQueueName();
    aclConf.setQueueName(queue);

    // set the queue admins acl in aclConf
    String qACLName = QueueManager.toFullPropertyName(queue,
        QueueACL.ADMINISTER_JOBS.getAclName());
    String queueAdminsACL = conf.get(qACLName, " ");
    aclConf.set(qACLName, queueAdminsACL);

    // set jobOwner as user.name in aclConf
    aclConf.set("user.name", user);

    OutputStream out = null;
    Path aclFile = new Path(logDir, TaskTracker.jobACLsFile);
    try {
      out = lfs.create(aclFile);
      aclConf.writeXml(out);
    } finally {
      IOUtils.cleanup(LOG, out);
    }
    lfs.setPermission(aclFile, urw_gr);
  }

  public void createWorkDir(JobConf jConf) throws IOException {
    // create $mapred.local.dir/taskTracker/$user/jobcache/$jobid/work
    final Path workDir = lDirAlloc.getLocalPathForWrite(WORKDIR, ttConf);
    if (!lfs.mkdirs(workDir)) {
      throw new IOException("Mkdirs failed to create "
          + workDir.toString());
    }
    jConf.set(TaskTracker.JOB_LOCAL_DIR, workDir.toUri().getPath());
  }

  public Path findCredentials() throws IOException {
    return lDirAlloc.getLocalPathToRead(JOBTOKEN, ttConf);
  }

  public int runSetup(String user, String jobid, Path localJobTokenFile,
                      TaskUmbilicalProtocol taskTracker) throws IOException, 
                      InterruptedException {
    // load user credentials, configuration
    // ASSUME
    // let $x = $mapred.local.dir
    // forall $x, exists $x/$user
    // exists $x/$user/jobcache/$jobid/job.xml
    // exists $x/$user/jobcache/$jobid/jobToken
    // exists $logdir/userlogs/$jobid
    final Path localJobFile = lDirAlloc.getLocalPathToRead(JOBCONF, ttConf);
    final JobConf cfgJob = new JobConf(localJobFile);
    createWorkDir(cfgJob);
    localizeJobFiles(JobID.forName(jobid), cfgJob, localJobFile,
        localJobTokenFile, taskTracker);

    // $mapred.local.dir/taskTracker/$user/distcache
    return 0;
  }

  public static void main(String[] argv)
      throws IOException, InterruptedException {
    // $logdir
    // let $x = $root/tasktracker for some $mapred.local.dir
    //   create $x/$user/jobcache/$jobid/work
    //   fetch  $x/$user/jobcache/$jobid/jars/job.jar
    //   setup  $x/$user/distcache
    //   verify $x/distcache
    //   write  $x/$user/jobcache/$jobid/job.xml
    final String user = argv[0];
    final String jobid = argv[1];
    final InetSocketAddress ttAddr = 
      new InetSocketAddress(argv[2], Integer.parseInt(argv[3]));
    final String uid = UserGroupInformation.getCurrentUser().getShortUserName();
    if (!user.equals(uid)) {
      LOG.warn("Localization running as " + uid + " not " + user);
    }

    // Pull in user's tokens to complete setup
    final JobConf conf = new JobConf();
    final JobLocalizer localizer =
      new JobLocalizer(conf, user, jobid);
    final Path jobTokenFile = localizer.findCredentials();
    final Credentials creds = TokenCache.loadTokens(
        jobTokenFile.toUri().toString(), conf);
    LOG.debug("Loaded tokens from " + jobTokenFile);
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    for (Token<? extends TokenIdentifier> token : creds.getAllTokens()) {
      ugi.addToken(token);
    }
    
    UserGroupInformation ugiJob = UserGroupInformation.createRemoteUser(jobid);
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(creds);
    jt.setService(new Text(ttAddr.getAddress().getHostAddress() + ":"
        + ttAddr.getPort()));
    ugiJob.addToken(jt);

    final TaskUmbilicalProtocol taskTracker = 
      ugiJob.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        public TaskUmbilicalProtocol run() throws IOException {
          TaskUmbilicalProtocol taskTracker =
            (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
                TaskUmbilicalProtocol.versionID,
                ttAddr, conf);
          return taskTracker;
        }
      });
    System.exit(
      ugi.doAs(new PrivilegedExceptionAction<Integer>() {
        public Integer run() {
          try {
            return localizer.runSetup(user, jobid, jobTokenFile, taskTracker);
          } catch (Throwable e) {
            e.printStackTrace(System.out);
            return -1;
          }
        }
      }));
  }

  /**
   * Write the task specific job-configuration file.
   * @throws IOException
   */
  public static void writeLocalJobFile(Path jobFile, JobConf conf)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(jobFile);
    OutputStream out = null;
    try {
      out = FileSystem.create(localFs, jobFile, urw_gr);
      conf.writeXml(out);
    } finally {
      IOUtils.cleanup(LOG, out);
    }
  }

}
