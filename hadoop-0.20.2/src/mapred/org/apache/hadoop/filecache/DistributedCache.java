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

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * Distribute application-specific large, read-only files efficiently.
 * 
 * <p><code>DistributedCache</code> is a facility provided by the Map-Reduce
 * framework to cache files (text, archives, jars etc.) needed by applications.
 * </p>
 * 
 * <p>Applications specify the files, via urls (hdfs:// or http://) to be cached 
 * via the {@link org.apache.hadoop.mapred.JobConf}.
 * The <code>DistributedCache</code> assumes that the
 * files specified via hdfs:// urls are already present on the 
 * {@link FileSystem} at the path specified by the url.</p>
 * 
 * <p>The framework will copy the necessary files on to the slave node before 
 * any tasks for the job are executed on that node. Its efficiency stems from 
 * the fact that the files are only copied once per job and the ability to 
 * cache archives which are un-archived on the slaves.</p> 
 *
 * <p><code>DistributedCache</code> can be used to distribute simple, read-only
 * data/text files and/or more complex types such as archives, jars etc. 
 * Archives (zip, tar and tgz/tar.gz files) are un-archived at the slave nodes. 
 * Jars may be optionally added to the classpath of the tasks, a rudimentary 
 * software distribution mechanism.  Files have execution permissions.
 * Optionally users can also direct it to symlink the distributed cache file(s)
 * into the working directory of the task.</p>
 * 
 * <p><code>DistributedCache</code> tracks modification timestamps of the cache 
 * files. Clearly the cache files should not be modified by the application 
 * or externally while the job is executing.</p>
 * 
 * <p>Here is an illustrative example on how to use the 
 * <code>DistributedCache</code>:</p>
 * <p><blockquote><pre>
 *     // Setting up the cache for the application
 *     
 *     1. Copy the requisite files to the <code>FileSystem</code>:
 *     
 *     $ bin/hadoop fs -copyFromLocal lookup.dat /myapp/lookup.dat  
 *     $ bin/hadoop fs -copyFromLocal map.zip /myapp/map.zip  
 *     $ bin/hadoop fs -copyFromLocal mylib.jar /myapp/mylib.jar
 *     $ bin/hadoop fs -copyFromLocal mytar.tar /myapp/mytar.tar
 *     $ bin/hadoop fs -copyFromLocal mytgz.tgz /myapp/mytgz.tgz
 *     $ bin/hadoop fs -copyFromLocal mytargz.tar.gz /myapp/mytargz.tar.gz
 *     
 *     2. Setup the application's <code>JobConf</code>:
 *     
 *     JobConf job = new JobConf();
 *     DistributedCache.addCacheFile(new URI("/myapp/lookup.dat#lookup.dat"), 
 *                                   job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/map.zip", job);
 *     DistributedCache.addFileToClassPath(new Path("/myapp/mylib.jar"), job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytar.tar", job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytgz.tgz", job);
 *     DistributedCache.addCacheArchive(new URI("/myapp/mytargz.tar.gz", job);
 *     
 *     3. Use the cached files in the {@link org.apache.hadoop.mapred.Mapper}
 *     or {@link org.apache.hadoop.mapred.Reducer}:
 *     
 *     public static class MapClass extends MapReduceBase  
 *     implements Mapper&lt;K, V, K, V&gt; {
 *     
 *       private Path[] localArchives;
 *       private Path[] localFiles;
 *       
 *       public void configure(JobConf job) {
 *         // Get the cached archives/files
 *         localArchives = DistributedCache.getLocalCacheArchives(job);
 *         localFiles = DistributedCache.getLocalCacheFiles(job);
 *       }
 *       
 *       public void map(K key, V value, 
 *                       OutputCollector&lt;K, V&gt; output, Reporter reporter) 
 *       throws IOException {
 *         // Use data from the cached archives/files here
 *         // ...
 *         // ...
 *         output.collect(k, v);
 *       }
 *     }
 *     
 * </pre></blockquote></p>
 * It is also very common to use the DistributedCache by using
 * {@link org.apache.hadoop.util.GenericOptionsParser}.
 *
 * This class includes methods that should be used by users
 * (specifically those mentioned in the example above, as well
 * as {@link DistributedCache#addArchiveToClassPath(Path, Configuration)}),
 * as well as methods intended for use by the MapReduce framework
 * (e.g., {@link org.apache.hadoop.mapred.JobClient}).  For implementation
 * details, see {@link TrackerDistributedCacheManager} and
 * {@link TaskDistributedCacheManager}.
 *
 * @see TrackerDistributedCacheManager
 * @see TaskDistributedCacheManager
 * @see org.apache.hadoop.mapred.JobConf
 * @see org.apache.hadoop.mapred.JobClient
 */
public class DistributedCache {
  /**
   * Warning: {@link #CACHE_FILES_SIZES} is not a *public* constant.
   */
  public static final String CACHE_FILES_SIZES = "mapred.cache.files.filesizes";
  
  /**
   * Warning: {@link #CACHE_ARCHIVES_SIZES} is not a *public* constant.
   */
  public static final String CACHE_ARCHIVES_SIZES = 
    "mapred.cache.archives.filesizes";

  /**
   * Warning: {@link #CACHE_ARCHIVES_TIMESTAMPS} is not a *public* constant.
   **/
  public static final String CACHE_ARCHIVES_TIMESTAMPS = "mapred.cache.archives.timestamps";

  /**
   * Warning: {@link #CACHE_FILES_TIMESTAMPS} is not a *public* constant.
   **/
  public static final String CACHE_FILES_TIMESTAMPS = "mapred.cache.files.timestamps";

  /**
   * Warning: {@link #CACHE_ARCHIVES} is not a *public* constant.
   **/
  public static final String CACHE_ARCHIVES = "mapred.cache.archives";

  /**
   * Warning: {@link #CACHE_FILES} is not a *public* constant.
   **/
  public static final String CACHE_FILES = "mapred.cache.files";

  /**
   * Warning: {@link #CACHE_LOCALARCHIVES} is not a *public* constant.
   **/
  public static final String CACHE_LOCALARCHIVES = "mapred.cache.localArchives";

  /**
   * Warning: {@link #CACHE_LOCALFILES} is not a *public* constant.
   **/
  public static final String CACHE_LOCALFILES = "mapred.cache.localFiles";

  /**
   * Warning: {@link #CACHE_SYMLINK} is not a *public* constant.
   **/
  public static final String CACHE_SYMLINK = "mapred.create.symlink";
  
  /**
   * Returns {@link FileStatus} of a given cache file on hdfs.
   * @param conf configuration
   * @param cache cache file 
   * @return <code>FileStatus</code> of a given cache file on hdfs
   * @throws IOException
   */
  public static FileStatus getFileStatus(Configuration conf, URI cache)
    throws IOException {
    FileSystem fileSystem = FileSystem.get(cache, conf);
    return fileSystem.getFileStatus(new Path(cache.getPath()));
  }
  
  /**
   * Returns mtime of a given cache file on hdfs. Internal to MapReduce.
   * @param conf configuration
   * @param cache cache file 
   * @return mtime of a given cache file on hdfs
   * @throws IOException
   */
  public static long getTimestamp(Configuration conf, URI cache)
    throws IOException {
    return getFileStatus(conf, cache).getModificationTime();
  }
  
  /**
   * This method create symlinks for all files in a given dir in another directory
   * @param conf the configuration
   * @param jobCacheDir the target directory for creating symlinks
   * @param workDir the directory in which the symlinks are created
   * @throws IOException
   * @deprecated Internal to MapReduce framework.  Use DistributedCacheManager
   * instead.
   */
  public static void createAllSymlink(Configuration conf, File jobCacheDir, File workDir)
    throws IOException{
    TrackerDistributedCacheManager.createAllSymlink(conf, jobCacheDir, workDir);
  }
  
  /**
   * Set the configuration with the given set of archives. Intended
   * to be used by user code.
   * @param archives The list of archives that need to be localized
   * @param conf Configuration which will be changed
   */
  public static void setCacheArchives(URI[] archives, Configuration conf) {
    String sarchives = StringUtils.uriToString(archives);
    conf.set(CACHE_ARCHIVES, sarchives);
  }

  /**
   * Set the configuration with the given set of files.  Intended to be
   * used by user code.
   * @param files The list of files that need to be localized
   * @param conf Configuration which will be changed
   */
  public static void setCacheFiles(URI[] files, Configuration conf) {
    String sfiles = StringUtils.uriToString(files);
    conf.set(CACHE_FILES, sfiles);
  }

  private static Path[] parsePaths(String[] strs) {
    if (strs == null) {
      return null;
    }
    Path[] result = new Path[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = new Path(strs[i]);
    }
    return result;
  }

  /**
   * Get cache archives set in the Configuration.  Used by
   * internal DistributedCache and MapReduce code.
   * @param conf The configuration which contains the archives
   * @return An array of the caches set in the Configuration
   * @throws IOException
   */
  public static URI[] getCacheArchives(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings(CACHE_ARCHIVES));
  }

  /**
   * Get cache files set in the Configuration.  Used by internal
   * DistributedCache and MapReduce code.
   * @param conf The configuration which contains the files
   * @return Am array of the files set in the Configuration
   * @throws IOException
   */
  public static URI[] getCacheFiles(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings(CACHE_FILES));
  }

  /**
   * Return the path array of the localized caches.  Intended to be used
   * by user code.
   * @param conf Configuration that contains the localized archives
   * @return A path array of localized caches
   * @throws IOException
   */
  public static Path[] getLocalCacheArchives(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf
                                    .getStrings(CACHE_LOCALARCHIVES));
  }

  /**
   * Return the path array of the localized files.  Intended to be used
   * by user code.
   * @param conf Configuration that contains the localized files
   * @return A path array of localized files
   * @throws IOException
   */
  public static Path[] getLocalCacheFiles(Configuration conf)
    throws IOException {
    return StringUtils.stringToPath(conf.getStrings(CACHE_LOCALFILES));
  }

  /**
   * Parse a list of strings into longs.
   * @param strs the list of strings to parse
   * @return a list of longs that were parsed. same length as strs.
   */
  private static long[] parseTimestamps(String[] strs) {
    if (strs == null) {
      return null;
    }
    long[] result = new long[strs.length];
    for(int i=0; i < strs.length; ++i) {
      result[i] = Long.parseLong(strs[i]);
    }
    return result;
  }

  /**
   * Get the timestamps of the archives.  Used by internal
   * DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a long array of timestamps 
   * @throws IOException
   */
  public static long[] getArchiveTimestamps(Configuration conf) {
    return parseTimestamps(conf.getStrings(CACHE_ARCHIVES_TIMESTAMPS));
  }

  /**
   * Get the timestamps of the files.  Used by internal
   * DistributedCache and MapReduce code.
   * @param conf The configuration which stored the timestamps
   * @return a long array of timestamps 
   * @throws IOException
   */
  public static long[] getFileTimestamps(Configuration conf) {
    return parseTimestamps(conf.getStrings(CACHE_FILES_TIMESTAMPS));
  }

  /**
   * This is to check the timestamp of the archives to be localized.
   * Used by internal MapReduce code.
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of archives.
   * The order should be the same as the order in which the archives are added.
   */
  public static void setArchiveTimestamps(Configuration conf, String timestamps) {
    conf.set(CACHE_ARCHIVES_TIMESTAMPS, timestamps);
  }

  /**
   * This is to check the timestamp of the files to be localized.
   * Used by internal MapReduce code.
   * @param conf Configuration which stores the timestamp's
   * @param timestamps comma separated list of timestamps of files.
   * The order should be the same as the order in which the files are added.
   */
  public static void setFileTimestamps(Configuration conf, String timestamps) {
    conf.set(CACHE_FILES_TIMESTAMPS, timestamps);
  }
  
  /**
   * Set the conf to contain the location for localized archives.  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  public static void setLocalArchives(Configuration conf, String str) {
    conf.set(CACHE_LOCALARCHIVES, str);
  }

  /**
   * Set the conf to contain the location for localized files.  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  public static void setLocalFiles(Configuration conf, String str) {
    conf.set(CACHE_LOCALFILES, str);
  }
  
  /**
   * Add a archive that has been localized to the conf.  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local archives
   */
  public static void addLocalArchives(Configuration conf, String str) {
    String archives = conf.get(CACHE_LOCALARCHIVES);
    conf.set(CACHE_LOCALARCHIVES, archives == null ? str
        : archives + "," + str);
  }

  /**
   * Add a file that has been localized to the conf..  Used
   * by internal DistributedCache code.
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma separated list of local files
   */
  public static void addLocalFiles(Configuration conf, String str) {
    String files = conf.get(CACHE_LOCALFILES);
    conf.set(CACHE_LOCALFILES, files == null ? str
        : files + "," + str);
  }

  /**
   * Add a archives to be localized to the conf.  Intended to
   * be used by user code.
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addCacheArchive(URI uri, Configuration conf) {
    String archives = conf.get(CACHE_ARCHIVES);
    conf.set(CACHE_ARCHIVES, archives == null ? uri.toString()
             : archives + "," + uri.toString());
  }
  
  /**
   * Add a file to be localized to the conf.  Intended
   * to be used by user code.
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addCacheFile(URI uri, Configuration conf) {
    String files = conf.get(CACHE_FILES);
    conf.set(CACHE_FILES, files == null ? uri.toString() : files + ","
             + uri.toString());
  }

  /**
   * Add a file path to the current set of classpath entries. It adds the file
   * to cache as well.  Intended to be used by user code.
   *
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   */
  public static void addFileToClassPath(Path file, Configuration conf)
        throws IOException {
    addFileToClassPath(file, conf, FileSystem.get(conf));
  }

  /**
   * Add a file path to the current set of classpath entries. It adds the file
   * to cache as well.  Intended to be used by user code.
   * 
   * @param file Path of the file to be added
   * @param conf Configuration that contains the classpath setting
   * @param fs FileSystem with respect to which {@code archivefile} should
   *              be interpreted. 
   */
  public static void addFileToClassPath
           (Path file, Configuration conf, FileSystem fs)
        throws IOException {
    String classpath = conf.get("mapred.job.classpath.files");
    conf.set("mapred.job.classpath.files", classpath == null ? file.toString()
             : classpath
                 + System.getProperty("path.separator") + file.toString());
    URI uri = fs.makeQualified(file).toUri();
    addCacheFile(uri, conf);
  }

  /**
   * Get the file entries in classpath as an array of Path.
   * Used by internal DistributedCache code.
   * 
   * @param conf Configuration that contains the classpath setting
   */
  public static Path[] getFileClassPaths(Configuration conf) {
    String classpath = conf.get("mapred.job.classpath.files");
    if (classpath == null)
      return null;
    ArrayList<Object> list = Collections.list(new StringTokenizer(classpath, System
                                                          .getProperty("path.separator")));
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path((String) list.get(i));
    }
    return paths;
  }

  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.  Intended to be used by user code.
   *
   * @param archive Path of the archive to be added
   * @param conf Configuration that contains the classpath setting
   */
  public static void addArchiveToClassPath
         (Path archive, Configuration conf)
      throws IOException {
    addArchiveToClassPath(archive, conf, FileSystem.get(conf));
  }

  /**
   * Add an archive path to the current set of classpath entries. It adds the
   * archive to cache as well.  Intended to be used by user code.
   * 
   * @param archive Path of the archive to be added
   * @param conf Configuration that contains the classpath setting
   * @param fs FileSystem with respect to which {@code archive} should be interpreted. 
   */
  public static void addArchiveToClassPath
         (Path archive, Configuration conf, FileSystem fs)
      throws IOException {
    String classpath = conf.get("mapred.job.classpath.archives");
    conf.set("mapred.job.classpath.archives", classpath == null ? archive
             .toString() : classpath + System.getProperty("path.separator")
             + archive.toString());
    URI uri = fs.makeQualified(archive).toUri();

    addCacheArchive(uri, conf);
  }

  /**
   * Get the archive entries in classpath as an array of Path.
   * Used by internal DistributedCache code.
   * 
   * @param conf Configuration that contains the classpath setting
   */
  public static Path[] getArchiveClassPaths(Configuration conf) {
    String classpath = conf.get("mapred.job.classpath.archives");
    if (classpath == null)
      return null;
    ArrayList<Object> list = Collections.list(new StringTokenizer(classpath, System
                                                          .getProperty("path.separator")));
    Path[] paths = new Path[list.size()];
    for (int i = 0; i < list.size(); i++) {
      paths[i] = new Path((String) list.get(i));
    }
    return paths;
  }

  /**
   * This method allows you to create symlinks in the current working directory
   * of the task to all the cache files/archives.
   * Intended to be used by user code.
   * @param conf the jobconf 
   */
  public static void createSymlink(Configuration conf){
    conf.set(CACHE_SYMLINK, "yes");
  }
  
  /**
   * This method checks to see if symlinks are to be create for the 
   * localized cache files in the current working directory 
   * Used by internal DistributedCache code.
   * @param conf the jobconf
   * @return true if symlinks are to be created- else return false
   */
  public static boolean getSymlink(Configuration conf){
    String result = conf.get(CACHE_SYMLINK);
    if ("yes".equals(result)){
      return true;
    }
    return false;
  }

  /**
   * This method checks if there is a conflict in the fragment names 
   * of the uris. Also makes sure that each uri has a fragment. It 
   * is only to be called if you want to create symlinks for 
   * the various archives and files.  May be used by user code.
   * @param uriFiles The uri array of urifiles
   * @param uriArchives the uri array of uri archives
   */
  public static boolean checkURIs(URI[]  uriFiles, URI[] uriArchives) {
    if ((uriFiles == null) && (uriArchives == null)){
      return true;
    }
    // check if fragment is null for any uri
    // also check if there are any conflicts in fragment names
    Set<String> fragments = new HashSet<String>();
    
    // iterate over file uris
    if (uriFiles != null) {
      for (int i = 0; i < uriFiles.length; i++) {
        String fragment = uriFiles[i].getFragment();
        if (fragment == null) {
          return false;
        }
        String lowerCaseFragment = fragment.toLowerCase();
        if (fragments.contains(lowerCaseFragment)) {
          return false;
        }
        fragments.add(lowerCaseFragment);
      }
    }
    
    // iterate over archive uris
    if (uriArchives != null) {
      for (int i = 0; i < uriArchives.length; i++) {
        String fragment = uriArchives[i].getFragment();
        if (fragment == null) {
          return false;
        }
        String lowerCaseFragment = fragment.toLowerCase();
        if (fragments.contains(lowerCaseFragment)) {
          return false;
        }
        fragments.add(lowerCaseFragment);
      }
    }
    return true;
  }

}
