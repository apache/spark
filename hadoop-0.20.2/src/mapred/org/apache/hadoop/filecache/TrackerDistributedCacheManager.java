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
package org.apache.hadoop.filecache;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskController;
import org.apache.hadoop.mapred.TaskTracker;
import org.apache.hadoop.util.MRAsyncDiskService;
import org.apache.hadoop.filecache.TaskDistributedCacheManager.CacheFile;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 * Manages a single machine's instance of a cross-job
 * cache.  This class would typically be instantiated
 * by a TaskTracker (or something that emulates it,
 * like LocalJobRunner).
 * 
 * <b>This class is internal to Hadoop, and should not be treated as a public
 * interface.</b>
 */
public class TrackerDistributedCacheManager {
  // cacheID to cacheStatus mapping
  private TreeMap<String, CacheStatus> cachedArchives = 
    new TreeMap<String, CacheStatus>();
  private Map<JobID, TaskDistributedCacheManager> jobArchives =
    Collections.synchronizedMap(
        new HashMap<JobID, TaskDistributedCacheManager>());
  private final TaskController taskController;
  private static final FsPermission PUBLIC_CACHE_OBJECT_PERM =
    FsPermission.createImmutable((short) 0755);

  // For holding the properties of each cache directory
  static class CacheDir {
    long size;
    long subdirs;
  }
  private TreeMap<Path, CacheDir> baseDirProperties =
    new TreeMap<Path, CacheDir>();

  // default total cache size (10GB)
  private static final long DEFAULT_CACHE_SIZE = 10737418240L;
  private static final long DEFAULT_CACHE_SUBDIR_LIMIT = 10000;
  private long allowedCacheSize;
  private long allowedCacheSubdirs;

  private static final Log LOG =
    LogFactory.getLog(TrackerDistributedCacheManager.class);

  private final LocalFileSystem localFs;
  
  private LocalDirAllocator lDirAllocator;
  
  private Configuration trackerConf;
  
  private static final Random random = new Random();

  private MRAsyncDiskService asyncDiskService;

  public TrackerDistributedCacheManager(Configuration conf,
      TaskController controller) throws IOException {
    this.localFs = FileSystem.getLocal(conf);
    this.trackerConf = conf;
    this.lDirAllocator = new LocalDirAllocator("mapred.local.dir");

    // setting the cache size to a default of 10GB
    this.allowedCacheSize = conf.getLong
      ("local.cache.size", DEFAULT_CACHE_SIZE);
    // setting the cache number of subdirectories limit to a default of 10000
    this.allowedCacheSubdirs = conf.getLong
      ("mapreduce.tasktracker.cache.local.numberdirectories",
       DEFAULT_CACHE_SUBDIR_LIMIT);
    this.taskController = controller;
  }

  /**
   * Creates a TrackerDistributedCacheManager with a MRAsyncDiskService.
   * @param asyncDiskService Provides a set of ThreadPools for async disk 
   *                         operations.  
   */
  public TrackerDistributedCacheManager(Configuration conf,
      TaskController taskController,
      MRAsyncDiskService asyncDiskService)
      throws IOException {
    this(conf, taskController);
    this.asyncDiskService = asyncDiskService;
  }

  /**
  /**
   * Get the locally cached file or archive; it could either be
   * previously cached (and valid) or copy it from the {@link FileSystem} now.
   *
   * @param cache the cache to be localized, this should be specified as
   * new URI(scheme://scheme-specific-part/absolute_path_to_file#LINKNAME).
   * @param conf The Configuration file which contains the filesystem
   * @param subDir The base cache subDir where you want to localize the 
   *  files/archives
   * @param fileStatus The file status on the dfs.
   * @param isArchive if the cache is an archive or a file. In case it is an
   *  archive with a .zip or .jar or .tar or .tgz or .tar.gz extension it will
   *  be unzipped/unjarred/untarred automatically
   *  and the directory where the archive is unzipped/unjarred/untarred is
   *  returned as the Path.
   *  In case of a file, the path to the file is returned
   * @param confFileStamp this is the hdfs file modification timestamp to verify
   * that the file to be cached hasn't changed since the job started
   * @param isPublic to know the cache file is accessible to public or private
   * @return the path to directory where the archives are unjarred in case of
   * archives, the path to the file where the file is copied locally
   * @throws IOException
   */
  Path getLocalCache(URI cache, Configuration conf,
                     String subDir, FileStatus fileStatus,
                     boolean isArchive, long confFileStamp,
                     boolean isPublic, CacheFile file) throws IOException {
    String key;
    String user = getLocalizedCacheOwner(isPublic);
    key = getKey(cache, conf, confFileStamp, user);
    CacheStatus lcacheStatus;
    Path localizedPath = null;
    Path localPath = null;
    synchronized (cachedArchives) {
      lcacheStatus = cachedArchives.get(key);
      if (lcacheStatus == null) {
        // was never localized
        String uniqueString
          = (String.valueOf(random.nextLong())
             + "_" + cache.hashCode()
             + "_" + (confFileStamp % Integer.MAX_VALUE));
        String cachePath = new Path (subDir, 
          new Path(uniqueString, makeRelative(cache, conf))).toString();
        localPath = lDirAllocator.getLocalPathForWrite(cachePath,
          fileStatus.getLen(), trackerConf, isPublic);
        lcacheStatus = 
          new CacheStatus(new Path(localPath.toString().replace(cachePath, "")), 
                          localPath, new Path(subDir), uniqueString, 
                          isPublic ? null : user);
        cachedArchives.put(key, lcacheStatus);
      }

      //mark the cache for use.
      file.setStatus(lcacheStatus);
      synchronized (lcacheStatus) {
        lcacheStatus.refcount++;
      }
    }
    
    try {
      // do the localization, after releasing the global lock
      synchronized (lcacheStatus) {
        if (!lcacheStatus.isInited()) {
          if (isPublic) {
            localizedPath = localizePublicCacheObject(conf, 
                                                      cache, 
                                                      confFileStamp,
                                                      lcacheStatus, fileStatus, 
                                                      isArchive);
          } else {
            localizedPath = localPath;
            if (!isArchive) {
              //for private archives, the lengths come over RPC from the 
              //JobLocalizer since the JobLocalizer is the one who expands
              //archives and gets the total length
              lcacheStatus.size = fileStatus.getLen();

              // Increase the size and sub directory count of the cache
              // from baseDirSize and baseDirNumberSubDir.
              addCacheInfoUpdate(lcacheStatus);
            }
          }
          lcacheStatus.initComplete();
        } else {
          localizedPath = checkCacheStatusValidity(conf, cache, confFileStamp,
                                                   lcacheStatus, fileStatus, isArchive);            
        }
      }

      // try deleting stuff if you can
      long size = 0;
      long numberSubdirs = 0;
      synchronized (lcacheStatus) {
        synchronized (baseDirProperties) {
          CacheDir cacheDir = baseDirProperties.get(lcacheStatus.getBaseDir());
          if (cacheDir != null) {
            size = cacheDir.size;
            numberSubdirs = cacheDir.subdirs;
          } else {
            LOG.warn("Cannot find size and number of subdirectories of" +
                     " baseDir: " + lcacheStatus.getBaseDir());
          }
        }
      }

      if (allowedCacheSize < size || allowedCacheSubdirs < numberSubdirs) {
        // try some cache deletions
        compactCache(conf);
      }
    } catch (IOException ie) {
      synchronized (lcacheStatus) {
        // release this cache
        lcacheStatus.refcount -= 1;
        throw ie;
      }
    }
    return localizedPath;
  }

  /**
   * This is the opposite of getlocalcache. When you are done with
   * using the cache, you need to release the cache
   * @param cache The cache URI to be released
   * @param conf configuration which contains the filesystem the cache
   * is contained in.
   * @param timeStamp the timestamp on the file represented by the cache URI
   * @param owner the owner of the localized file
   * @throws IOException
   */
  void releaseCache(CacheStatus status) throws IOException {
	synchronized (status) {
      status.refcount--;
    }
  }

  void setSize(CacheStatus status, long size) throws IOException {
    if (size != 0) {
      synchronized (status) {
        status.size = size;
        addCacheInfoUpdate(status);
      }
    }
  }

  /*
   * This method is called from unit tests. 
   */
  int getReferenceCount(CacheStatus status) throws IOException {
	synchronized (status) {
	  return status.refcount;
	}
  }

  /**
   * Get the user who should "own" the localized distributed cache file.
   * If the cache is public, the tasktracker user is the owner. If private,
   * the user that the task is running as, is the owner.
   * @param isPublic
   * @return the owner as a shortname string
   * @throws IOException
   */
  static String getLocalizedCacheOwner(boolean isPublic) throws IOException {  
    String user;
    if (isPublic) {
      user = UserGroupInformation.getLoginUser().getShortUserName();
    } else {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    }
    return user;
  }


  // To delete the caches which have a refcount of zero

  private void compactCache(Configuration conf) throws IOException {
    List<CacheStatus> deleteList = new LinkedList<CacheStatus>();
    // try deleting cache Status with refcount of zero
    synchronized (cachedArchives) {
      for (Iterator<String> it = cachedArchives.keySet().iterator(); 
          it.hasNext();) {
        String cacheId = it.next();
        CacheStatus lcacheStatus = cachedArchives.get(cacheId);
        // if reference count is zero 
        // mark the cache for deletion
        if (lcacheStatus.refcount == 0) {
          // delete this cache entry from the global list 
          // and mark the localized file for deletion
          deleteList.add(lcacheStatus);
          it.remove();
        }
      }
    }
    
    // do the deletion, after releasing the global lock
    for (CacheStatus lcacheStatus : deleteList) {
      synchronized (lcacheStatus) {
        Path localizedDir = lcacheStatus.getLocalizedUniqueDir();
        if (lcacheStatus.user == null) {
          // Update the maps baseDirSize and baseDirNumberSubDir
          LOG.info("Deleted path " + localizedDir);

          try {
            deleteLocalPath(asyncDiskService, localFs, lcacheStatus.getLocalizedUniqueDir());
          } catch (IOException e) {
            LOG.warn("Could not delete distributed cache empty directory "
                     + lcacheStatus.getLocalizedUniqueDir());
          }
        } else {
          LOG.info("Deleted path " + localizedDir + " as " + lcacheStatus.user);
          String base = lcacheStatus.getBaseDir().toString();
          String userDir = TaskTracker.getUserDir(lcacheStatus.user);
          int skip = base.length() + 1 + userDir.length() + 1;
          String relative = localizedDir.toString().substring(skip);
          taskController.deleteAsUser(lcacheStatus.user, relative);
        }

        deleteCacheInfoUpdate(lcacheStatus);
      }
    }
  }

  /**
   * Delete a local path with asyncDiskService if available,
   * or otherwise synchronously with local file system.
   */
  private static void deleteLocalPath(MRAsyncDiskService asyncDiskService,
      LocalFileSystem fs, Path path) throws IOException {
    boolean deleted = false;
    if (asyncDiskService != null) {
      // Try to delete using asyncDiskService
      String localPathToDelete = 
        path.toUri().getPath();
      deleted = asyncDiskService.moveAndDeleteAbsolutePath(localPathToDelete);
      if (!deleted) {
        LOG.warn("Cannot find DistributedCache path " + localPathToDelete
            + " on any of the asyncDiskService volumes!");
      }
    }
    if (!deleted) {
      // If no asyncDiskService, we will delete the files synchronously
      fs.delete(path, true);
    }
    LOG.info("Deleted path " + path);
  }

  /*
   * Returns the relative path of the dir this cache will be localized in
   * relative path that this cache will be localized in. For
   * hdfs://hostname:port/absolute_path -- the relative path is
   * hostname/absolute path -- if it is just /absolute_path -- then the
   * relative path is hostname of DFS this mapred cluster is running
   * on/absolute_path
   */
  String makeRelative(URI cache, Configuration conf)
    throws IOException {
    String host = cache.getHost();
    if (host == null) {
      host = cache.getScheme();
    }
    if (host == null) {
      URI defaultUri = FileSystem.get(conf).getUri();
      host = defaultUri.getHost();
      if (host == null) {
        host = defaultUri.getScheme();
      }
    }
    String path = host + cache.getPath();
    path = path.replace(":/","/");                // remove windows device colon
    return path;
  }

  private Path checkCacheStatusValidity(Configuration conf,
      URI cache, long confFileStamp,
      CacheStatus cacheStatus,
      FileStatus fileStatus,
      boolean isArchive
      ) throws IOException {
    FileSystem fs = FileSystem.get(cache, conf);
    // Has to be
    if (!ifExistsAndFresh(conf, fs, cache, confFileStamp,
        cacheStatus, fileStatus)) {
      throw new IOException("Stale cache file: " + cacheStatus.localizedLoadPath + 
                            " for cache-file: " + cache);
    }
    LOG.info(String.format("Using existing cache of %s->%s",
             cache.toString(), cacheStatus.localizedLoadPath));
    return cacheStatus.localizedLoadPath;
  }
  
  /**
   * Returns a boolean to denote whether a cache file is visible to all(public)
   * or not
   * @param conf
   * @param uri
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  static boolean isPublic(Configuration conf, URI uri) throws IOException {
    FileSystem fs = FileSystem.get(uri, conf);
    Path current = new Path(uri.getPath());
    //the leaf level file should be readable by others
    if (!checkPermissionOfOther(fs, current, FsAction.READ)) {
      return false;
    }
    return ancestorsHaveExecutePermissions(fs, current.getParent());
  }

  /**
   * Returns true if all ancestors of the specified path have the 'execute'
   * permission set for all users (i.e. that other users can traverse
   * the directory heirarchy to the given path)
   */
  static boolean ancestorsHaveExecutePermissions(FileSystem fs, Path path)
    throws IOException {
    Path current = path;
    while (current != null) {
      //the subdirs in the path should have execute permissions for others
      if (!checkPermissionOfOther(fs, current, FsAction.EXECUTE)) {
        return false;
      }
      current = current.getParent();
    }
    return true;
  }

  /**
   * Checks for a given path whether the Other permissions on it 
   * imply the permission in the passed FsAction
   * @param fs
   * @param path
   * @param action
   * @return true if the path in the uri is visible to all, false otherwise
   * @throws IOException
   */
  private static boolean checkPermissionOfOther(FileSystem fs, Path path,
      FsAction action) throws IOException {
    FileStatus status = fs.getFileStatus(path);
    FsPermission perms = status.getPermission();
    FsAction otherAction = perms.getOtherAction();
    if (otherAction.implies(action)) {
      return true;
    }
    return false;
  }

  private static Path createRandomPath(Path base) throws IOException {
    return new Path(base.toString() + "-work-" + random.nextLong());
  }

  /**
   * Download a given path to the local file system.
   * @param conf the job's configuration
   * @param source the source to copy from
   * @param destination where to copy the file. must be local fs
   * @param desiredTimestamp the required modification timestamp of the source
   * @param isArchive is this an archive that should be expanded
   * @param permission the desired permissions of the file.
   * @return for archives, the number of bytes in the unpacked directory
   * @throws IOException
   */
  public static long downloadCacheObject(Configuration conf,
                                         URI source,
                                         Path destination,
                                         long desiredTimestamp,
                                         boolean isArchive,
                                         FsPermission permission
                                         ) throws IOException {
    FileSystem sourceFs = FileSystem.get(source, conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    
    Path sourcePath = new Path(source.getPath());
    long modifiedTime = 
      sourceFs.getFileStatus(sourcePath).getModificationTime();
    if (modifiedTime != desiredTimestamp) {
      DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, 
                                                     DateFormat.SHORT);
      throw new IOException("The distributed cache object " + source + 
                            " changed during the job from " + 
                            df.format(new Date(desiredTimestamp)) + " to " +
                            df.format(new Date(modifiedTime)));
    }
    
    Path parchive = null;
    if (isArchive) {
      parchive = new Path(destination, destination.getName());
    } else {
      parchive = destination;
    }
    // if the file already exists, we are done
    if (localFs.exists(parchive)) {
      return 0;
    }
    // the final directory for the object
    Path finalDir = parchive.getParent();
    // the work directory for the object
    Path workDir = createRandomPath(finalDir);
    LOG.info("Creating " + destination.getName() + " in " + workDir + " with " + 
            permission);
    if (!localFs.mkdirs(workDir, permission)) {
      throw new IOException("Mkdirs failed to create directory " + workDir);
    }
    Path workFile = new Path(workDir, parchive.getName());
    sourceFs.copyToLocalFile(sourcePath, workFile);
    localFs.setPermission(workFile, permission);
    if (isArchive) {
      String tmpArchive = workFile.getName().toLowerCase();
      File srcFile = new File(workFile.toString());
      File destDir = new File(workDir.toString());
      LOG.info(String.format("Extracting %s to %s",
               srcFile.toString(), destDir.toString()));
      if (tmpArchive.endsWith(".jar")) {
        RunJar.unJar(srcFile, destDir);
      } else if (tmpArchive.endsWith(".zip")) {
        FileUtil.unZip(srcFile, destDir);
      } else if (isTarFile(tmpArchive)) {
        FileUtil.unTar(srcFile, destDir);
      } else {
        LOG.warn(String.format(
            "Cache file %s specified as archive, but not valid extension.",
            srcFile.toString()));
        // else will not do anyhting
        // and copy the file into the dir as it is
      }
      try {
        FileUtil.chmod(destDir.toString(), "ugo+rx", true);
      } catch (InterruptedException ie) {
        // This exception is never actually thrown, but the signature says
        // it is, and we can't make the incompatible change within CDH
        throw new IOException("Interrupted while chmodding", ie);
      }
    }
    // promote the output to the final location
    if (!localFs.rename(workDir, finalDir)) {
      localFs.delete(workDir, true);
      if (!localFs.exists(finalDir)) {
        throw new IOException("Failed to promote distributed cache object " +
                              workDir + " to " + finalDir);
      }
      // someone else promoted first
      return 0;
    }

    LOG.info(String.format("Cached %s as %s",
             source.toString(), destination.toString()));
    long cacheSize = 
      FileUtil.getDU(new File(parchive.getParent().toString()));
    return cacheSize;
  }

  //the method which actually copies the caches locally and unjars/unzips them
  // and does chmod for the files
  Path localizePublicCacheObject(Configuration conf,
                                 URI cache, long confFileStamp,
                                 CacheStatus cacheStatus,
                                 FileStatus fileStatus,
                                 boolean isArchive) throws IOException {
    long size = downloadCacheObject(conf, cache, cacheStatus.localizedLoadPath,
                                    confFileStamp, isArchive, 
                                    PUBLIC_CACHE_OBJECT_PERM);
    cacheStatus.size = size;
    
    // Increase the size and sub directory count of the cache
    // from baseDirSize and baseDirNumberSubDir.
    addCacheInfoUpdate(cacheStatus);

    LOG.info(String.format("Cached %s as %s",
             cache.toString(), cacheStatus.localizedLoadPath));
    return cacheStatus.localizedLoadPath;
  }

  private static boolean isTarFile(String filename) {
    return (filename.endsWith(".tgz") || filename.endsWith(".tar.gz") ||
           filename.endsWith(".tar"));
  }

  // Checks if the cache has already been localized and is fresh
  private boolean ifExistsAndFresh(Configuration conf, FileSystem fs,
                                          URI cache, long confFileStamp,
                                          CacheStatus lcacheStatus,
                                          FileStatus fileStatus)
  throws IOException {
    long dfsFileStamp;
    if (fileStatus != null) {
      dfsFileStamp = fileStatus.getModificationTime();
    } else {
      dfsFileStamp = DistributedCache.getTimestamp(conf, cache);
    }

    // ensure that the file on hdfs hasn't been modified since the job started
    if (dfsFileStamp != confFileStamp) {
      LOG.fatal("File: " + cache + " has changed on HDFS since job started");
      throw new IOException("File: " + cache +
      " has changed on HDFS since job started");
    }

    return true;
  }

  String getKey(URI cache, Configuration conf, long timeStamp, String user) 
      throws IOException {
    return makeRelative(cache, conf) + String.valueOf(timeStamp) + user;
  }
  
  /**
   * This method create symlinks for all files in a given dir in another 
   * directory.
   * 
   * Should not be used outside of DistributedCache code.
   * 
   * @param conf the configuration
   * @param jobCacheDir the target directory for creating symlinks
   * @param workDir the directory in which the symlinks are created
   * @throws IOException
   */
  public static void createAllSymlink(Configuration conf, File jobCacheDir, 
      File workDir)
    throws IOException{
    if ((jobCacheDir == null || !jobCacheDir.isDirectory()) ||
           workDir == null || (!workDir.isDirectory())) {
      return;
    }
    boolean createSymlink = DistributedCache.getSymlink(conf);
    if (createSymlink){
      File[] list = jobCacheDir.listFiles();
      for (int i=0; i < list.length; i++){
        String target = list[i].getAbsolutePath();
        String link = new File(workDir, list[i].getName()).toString();
        LOG.info(String.format("Creating symlink: %s <- %s", target, link));
        int ret = FileUtil.symLink(target, link);
        if (ret != 0) {
          LOG.warn(String.format("Failed to create symlink: %s <- %s", target,
              link));
        }
      }
    }
  }

  static class CacheStatus {
    // the local load path of this cache
    Path localizedLoadPath;

    //the base dir where the cache lies
    Path localizedBaseDir;

    //the size of this cache
    long size;

    // number of instances using this cache
    int refcount;

    // is it initialized ?
    boolean inited = false;

    // The sub directory (tasktracker/archive or tasktracker/user/archive),
    // under which the file will be localized
    Path subDir;
    
    // unique string used in the construction of local load path
    String uniqueString;
    
    // The user that owns the cache entry or null if it is public
    final String user;

    public CacheStatus(Path baseDir, Path localLoadPath, Path subDir,
                       String uniqueString, String user) {
      super();
      this.localizedLoadPath = localLoadPath;
      this.refcount = 0;
      this.localizedBaseDir = baseDir;
      this.size = 0;
      this.subDir = subDir;
      this.uniqueString = uniqueString;
      this.user = user;
    }
    
    Path getBaseDir(){
      return this.localizedBaseDir;
    }
    
    // mark it as initialized
    void initComplete() {
      inited = true;
    }

    // is it initialized?
    boolean isInited() {
      return inited;
    }
    
    Path getLocalizedUniqueDir() {
      return new Path(localizedBaseDir, new Path(subDir, uniqueString));
    }
  }

  /**
   * Clear the entire contents of the cache and delete the backing files. This
   * should only be used when the server is reinitializing, because the users
   * are going to lose their files.
   */
  public void purgeCache() {
    synchronized (cachedArchives) {
      for (Map.Entry<String,CacheStatus> f: cachedArchives.entrySet()) {
        try {
          deleteLocalPath(asyncDiskService, localFs, f.getValue().localizedLoadPath);
        } catch (IOException ie) {
          LOG.debug("Error cleaning up cache", ie);
        }
      }
      cachedArchives.clear();
    }
  }

  public TaskDistributedCacheManager 
  newTaskDistributedCacheManager(JobID jobId,
                                 Configuration taskConf) throws IOException {
    TaskDistributedCacheManager result = 
      new TaskDistributedCacheManager(this, taskConf);
    jobArchives.put(jobId, result);
    return result;
  }

  public void setArchiveSizes(JobID jobId, long[] sizes) throws IOException {
    TaskDistributedCacheManager mgr = jobArchives.get(jobId);
    if (mgr != null) {
      mgr.setSizes(sizes);
    }
  }

  public void removeTaskDistributedCacheManager(JobID jobId) {
    jobArchives.remove(jobId);
  }

  /*
   * This method is called from unit tests.
   */
  protected TaskDistributedCacheManager getTaskDistributedCacheManager(
      JobID jobId) {
    return jobArchives.get(jobId);
  }

  /**
   * Determines timestamps of files to be cached, and stores those
   * in the configuration.  This is intended to be used internally by JobClient
   * after all cache files have been added.
   * 
   * This is an internal method!
   * 
   * @param job Configuration of a job.
   * @throws IOException
   */
  public static void determineTimestamps(Configuration job) throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      FileStatus status = DistributedCache.getFileStatus(job, tarchives[0]);
      StringBuffer archiveFileSizes = 
        new StringBuffer(String.valueOf(status.getLen()));      
      StringBuffer archiveTimestamps = 
        new StringBuffer(String.valueOf(status.getModificationTime()));
      for (int i = 1; i < tarchives.length; i++) {
        status = DistributedCache.getFileStatus(job, tarchives[i]);
        archiveFileSizes.append(",");
        archiveFileSizes.append(String.valueOf(status.getLen()));
        archiveTimestamps.append(",");
        archiveTimestamps.append(String.valueOf(
            status.getModificationTime()));
      }
      job.set(DistributedCache.CACHE_ARCHIVES_SIZES, 
          archiveFileSizes.toString());
      DistributedCache.setArchiveTimestamps(job, archiveTimestamps.toString());
    }
  
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      FileStatus status = DistributedCache.getFileStatus(job, tfiles[0]);
      StringBuffer fileSizes = 
        new StringBuffer(String.valueOf(status.getLen()));      
      StringBuffer fileTimestamps = new StringBuffer(String.valueOf(
          status.getModificationTime()));
      for (int i = 1; i < tfiles.length; i++) {
        status = DistributedCache.getFileStatus(job, tfiles[i]);
        fileSizes.append(",");
        fileSizes.append(String.valueOf(status.getLen()));
        fileTimestamps.append(",");
        fileTimestamps.append(String.valueOf(status.getModificationTime()));
      }
      job.set(DistributedCache.CACHE_FILES_SIZES, fileSizes.toString());
      DistributedCache.setFileTimestamps(job, fileTimestamps.toString());
    }
  }
  /**
   * Determines the visibilities of the distributed cache files and 
   * archives. The visibility of a cache path is "public" if the leaf component
   * has READ permissions for others, and the parent subdirs have 
   * EXECUTE permissions for others
   * @param job
   * @throws IOException
   */
  public static void determineCacheVisibilities(Configuration job) 
  throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    if (tarchives != null) {
      StringBuffer archiveVisibilities = 
        new StringBuffer(String.valueOf(isPublic(job, tarchives[0])));
      for (int i = 1; i < tarchives.length; i++) {
        archiveVisibilities.append(",");
        archiveVisibilities.append(String.valueOf(isPublic(job, tarchives[i])));
      }
      setArchiveVisibilities(job, archiveVisibilities.toString());
    }
    URI[] tfiles = DistributedCache.getCacheFiles(job);
    if (tfiles != null) {
      StringBuffer fileVisibilities = 
        new StringBuffer(String.valueOf(isPublic(job, tfiles[0])));
      for (int i = 1; i < tfiles.length; i++) {
        fileVisibilities.append(",");
        fileVisibilities.append(String.valueOf(isPublic(job, tfiles[i])));
      }
      setFileVisibilities(job, fileVisibilities.toString());
    }
  }
  
  private static boolean[] parseBooleans(String[] strs) {
    if (null == strs) {
      return null;
    }
    boolean[] result = new boolean[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Boolean.parseBoolean(strs[i]);
    }
    return result;
  }

  /**
   * Get the booleans on whether the files are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return array of booleans 
   * @throws IOException
   */
  public static boolean[] getFileVisibilities(Configuration conf) {
    return parseBooleans(conf.getStrings(JobContext.CACHE_FILE_VISIBILITIES));
  }

  /**
   * Get the booleans on whether the archives are public or not.  Used by 
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return array of booleans 
   */
  public static boolean[] getArchiveVisibilities(Configuration conf) {
    return parseBooleans(conf.getStrings(JobContext.
                                           CACHE_ARCHIVES_VISIBILITIES));
  }

  /**
   * This is to check the public/private visibility of the archives to be
   * localized.
   * 
   * @param conf Configuration which stores the timestamp's
   * @param booleans comma separated list of booleans (true - public)
   * The order should be the same as the order in which the archives are added.
   */
  static void setArchiveVisibilities(Configuration conf, String booleans) {
    conf.set(JobContext.CACHE_ARCHIVES_VISIBILITIES, booleans);
  }

  /**
   * This is to check the public/private visibility of the files to be localized
   * 
   * @param conf Configuration which stores the timestamp's
   * @param booleans comma separated list of booleans (true - public)
   * The order should be the same as the order in which the files are added.
   */
  static void setFileVisibilities(Configuration conf, String booleans) {
    conf.set(JobContext.CACHE_FILE_VISIBILITIES, booleans);
  }
  
  /**
   * For each archive or cache file - get the corresponding delegation token
   * @param job
   * @param credentials
   * @throws IOException
   */
  public static void getDelegationTokens(Configuration job, 
                                         Credentials credentials) 
  throws IOException {
    URI[] tarchives = DistributedCache.getCacheArchives(job);
    URI[] tfiles = DistributedCache.getCacheFiles(job);

    int size = (tarchives!=null? tarchives.length : 0) + (tfiles!=null ? tfiles.length :0);
    Path[] ps = new Path[size];

    int i = 0;
    if (tarchives != null) {
      for (i=0; i < tarchives.length; i++) {
        ps[i] = new Path(tarchives[i].toString());
      }
    }

    if (tfiles != null) {
      for(int j=0; j< tfiles.length; j++) {
        ps[i+j] = new Path(tfiles[j].toString());
      }
    }

    TokenCache.obtainTokensForNamenodes(credentials, ps, job);
  }

  /** 
   * This is part of the framework API.  It's called within the job
   * submission code only, not by users.  In the non-error case it has
   * no side effects and returns normally.  If there's a URI in both
   * mapred.cache.files and mapred.cache.archives, it throws its
   * exception. 
   * @param conf a {@link Configuration} to be cheked for duplication
   * in cached URIs 
   * @throws InvalidJobConfException
   **/
  public static void validate(Configuration conf)
                          throws InvalidJobConfException {
    final String[] archiveStrings
      = conf.getStrings(DistributedCache.CACHE_ARCHIVES);
    final String[] fileStrings = conf.getStrings(DistributedCache.CACHE_FILES);

    Path thisSubject = null;

    if (archiveStrings != null && fileStrings != null) {
      final Set<Path> archivesSet = new HashSet<Path>();

      for (String archiveString : archiveStrings) {
        archivesSet.add(coreLocation(archiveString, conf));
      }

      for (String fileString : fileStrings) {
        thisSubject = coreLocation(fileString, conf);

        if (archivesSet.contains(thisSubject)) {
          throw new InvalidJobConfException
            ("The core URI, \""
             + thisSubject
             + "\" is listed both in " + DistributedCache.CACHE_FILES
             + " and in " + DistributedCache.CACHE_ARCHIVES + " .");
        }
      }
    }
  }

  private static Path coreLocation(String uriString, Configuration conf) 
       throws InvalidJobConfException {
    // lose the fragment, if it's likely to be a symlink name
    if (DistributedCache.getSymlink(conf)) {
      try {
        URI uri = new URI(uriString);
        uriString
          = (new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(),
                     null, null)
             .toString());
      } catch (URISyntaxException e) {
        throw new InvalidJobConfException
          ("Badly formatted URI: " + uriString, e);
      }
    }
        
    Path path = new Path(uriString);

    try {
      path = path.makeQualified(path.getFileSystem(conf));
    } catch (IOException e) {
      throw new InvalidJobConfException
        ("Invalid file system in distributed cache for the URI: "
         + uriString, e);
    }

    return path;
  }
  
  /**
   * Decrement the size and sub directory count of the cache from baseDirSize
   * and baseDirNumberSubDir. Have to lock lcacheStatus before calling this.
   * @param cacheStatus cache status of the cache is deleted
   */
  private void deleteCacheInfoUpdate(CacheStatus cacheStatus) {
    if (!cacheStatus.inited) {
      // if it is not created yet, do nothing.
      return;
    }
    // decrement the size of the cache from baseDirSize
    synchronized (baseDirProperties) {
      CacheDir cacheDir = baseDirProperties.get(cacheStatus.getBaseDir());
      if (cacheDir != null) {
        cacheDir.size -= cacheStatus.size;
        cacheDir.subdirs--;
      } else {
        LOG.warn("Cannot find size and number of subdirectories of" +
                 " baseDir: " + cacheStatus.getBaseDir());
      }
    }
  }
  
  /**
   * Update the maps baseDirSize and baseDirNumberSubDir when adding cache.
   * Increase the size and sub directory count of the cache from baseDirSize
   * and baseDirNumberSubDir. Have to lock lcacheStatus before calling this.
   * @param cacheStatus cache status of the cache is added
   */
  private void addCacheInfoUpdate(CacheStatus cacheStatus) {
    long cacheSize = cacheStatus.size;
    // decrement the size of the cache from baseDirSize
    synchronized (baseDirProperties) {
      CacheDir cacheDir = baseDirProperties.get(cacheStatus.getBaseDir());
      if (cacheDir != null) {
        cacheDir.size += cacheSize;
        cacheDir.subdirs++;
      } else {
        cacheDir = new CacheDir();
        cacheDir.size = cacheSize;
        cacheDir.subdirs = 1;
        baseDirProperties.put(cacheStatus.getBaseDir(), cacheDir);
      }
    }
  }
}
