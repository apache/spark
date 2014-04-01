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
package org.apache.hadoop.fs;

import java.text.*;
import java.io.*;
import java.util.Date;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.StringUtils;

/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
public class Trash extends Configured {
  private static final Log LOG =
    LogFactory.getLog(Trash.class);

  private static final Path CURRENT = new Path("Current");
  private static final Path TRASH = new Path(".Trash/");
  private static final Path HOMES = new Path("/user/");

  private static final FsPermission PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final DateFormat CHECKPOINT = new SimpleDateFormat("yyMMddHHmm");
  private static final int MSECS_PER_MINUTE = 60*1000;

  private final FileSystem fs;
  private final Path trash;
  private final Path current;
  private final long interval;

  /** Construct a trash can accessor.
   * @param conf a Configuration
   */
  public Trash(Configuration conf) throws IOException {
    this(FileSystem.get(conf), conf);
  }

  /**
   * Construct a trash can accessor for the FileSystem provided.
   */
  public Trash(FileSystem fs, Configuration conf) throws IOException {
    super(conf);
    this.fs = fs;
    this.trash = new Path(fs.getHomeDirectory(), TRASH);
    this.current = new Path(trash, CURRENT);
    this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
  }

  private Trash(Path home, Configuration conf) throws IOException {
    super(conf);
    this.fs = home.getFileSystem(conf);
    this.trash = new Path(home, TRASH);
    this.current = new Path(trash, CURRENT);
    this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
  }
  
  private Path makeTrashRelativePath(Path basePath, Path rmFilePath) {
    return new Path(basePath + rmFilePath.toUri().getPath());
  }

  /** Move a file or directory to the current trash directory.
   * @return false if the item is already in the trash or trash is disabled
   */ 
  public boolean moveToTrash(Path path) throws IOException {
    if (interval == 0)
      return false;

    if (!path.isAbsolute())                       // make path absolute
      path = new Path(fs.getWorkingDirectory(), path);

    if (!fs.exists(path))                         // check that path exists
      throw new FileNotFoundException(path.toString());

    String qpath = path.makeQualified(fs).toString();

    if (qpath.startsWith(trash.toString())) {
      return false;                               // already in trash
    }

    if (trash.getParent().toString().startsWith(qpath)) {
      throw new IOException("Cannot move \"" + path +
                            "\" to the trash, as it contains the trash");
    }

    Path trashPath = makeTrashRelativePath(current, path);
    Path baseTrashPath = makeTrashRelativePath(current, path.getParent());
    
    IOException cause = null;

    // try twice, in case checkpoint between the mkdirs() & rename()
    for (int i = 0; i < 2; i++) {
      try {
        if (!fs.mkdirs(baseTrashPath, PERMISSION)) {      // create current
          LOG.warn("Can't create(mkdir) trash directory: "+baseTrashPath);
          return false;
        }
      } catch (IOException e) {
        LOG.warn("Can't create trash directory: "+baseTrashPath);
        cause = e;
        break;
      }
      try {
        //
        // if the target path in Trash already exists, then append with 
        // a number. Start from 1.
        //
        String orig = trashPath.toString();
        for (int j = 1; fs.exists(trashPath); j++) {
          trashPath = new Path(orig + "." + j);
        }
        if (fs.rename(path, trashPath))           // move to current trash
          return true;
      } catch (IOException e) {
        cause = e;
      }
    }
    throw (IOException)
      new IOException("Failed to move to trash: "+path).initCause(cause);
  }

  /** Create a trash checkpoint. */
  public void checkpoint() throws IOException {
    if (!fs.exists(current))                      // no trash, no checkpoint
      return;

    Path checkpoint;
    synchronized (CHECKPOINT) {
      checkpoint = new Path(trash, CHECKPOINT.format(new Date()));
    }

    if (fs.rename(current, checkpoint)) {
      LOG.info("Created trash checkpoint: "+checkpoint.toUri().getPath());
    } else {
      throw new IOException("Failed to checkpoint trash: "+checkpoint);
    }
  }

  /** Delete old checkpoints. */
  public void expunge() throws IOException {
    FileStatus[] dirs = fs.listStatus(trash);            // scan trash sub-directories
    if( dirs == null){
      return;
    }
    long now = System.currentTimeMillis();
    for (int i = 0; i < dirs.length; i++) {
      Path path = dirs[i].getPath();
      String dir = path.toUri().getPath();
      String name = path.getName();
      if (name.equals(CURRENT.getName()))         // skip current
        continue;

      long time;
      try {
        synchronized (CHECKPOINT) {
          time = CHECKPOINT.parse(name).getTime();
        }
      } catch (ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - interval) > time) {
        if (fs.delete(path, true)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }

  //
  // get the current working directory
  //
  Path getCurrentTrashDir() {
    return current;
  }

  /** Return a {@link Runnable} that periodically empties the trash of all
   * users, intended to be run by the superuser.  Only one checkpoint is kept
   * at a time.
   */
  public Runnable getEmptier() throws IOException {
    return new Emptier(getConf());
  }

  private static class Emptier implements Runnable {

    private Configuration conf;
    private FileSystem fs;
    private long interval;

    public Emptier(Configuration conf) throws IOException {
      this.conf = conf;
      this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
      this.fs = FileSystem.get(conf);
    }

    public void run() {
      if (interval == 0)
        return;                                   // trash disabled

      long now = System.currentTimeMillis();
      long end;
      while (true) {
        end = ceiling(now, interval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          return;                                 // exit on interrupt
        }
          
        try {
          now = System.currentTimeMillis();
          if (now >= end) {

            FileStatus[] homes = null;
            try {
              homes = fs.listStatus(HOMES);         // list all home dirs
            } catch (IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            if (homes == null)
              continue;

            for (FileStatus home : homes) {         // dump each trash
              if (!home.isDir())
                continue;
              try {
                Trash trash = new Trash(home.getPath(), conf);
                trash.expunge();
                trash.checkpoint();
              } catch (IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              } 
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run() " + 
                   StringUtils.stringifyException(e));
        }
      }
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

  }

  /** Run an emptier.*/
  public static void main(String[] args) throws Exception {
    new Trash(new Configuration()).getEmptier().run();
  }

}
