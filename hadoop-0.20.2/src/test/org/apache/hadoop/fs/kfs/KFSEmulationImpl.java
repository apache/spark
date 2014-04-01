/**
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * @author: Sriram Rao (Kosmix Corp.)
 * 
 * We need to provide the ability to the code in fs/kfs without really
 * having a KFS deployment.  For this purpose, use the LocalFileSystem
 * as a way to "emulate" KFS.
 */

package org.apache.hadoop.fs.kfs;

import java.io.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;


public class KFSEmulationImpl implements IFSImpl {
    FileSystem localFS;
    
    public KFSEmulationImpl(Configuration conf) throws IOException {
        localFS = FileSystem.getLocal(conf);
    }

    public boolean exists(String path) throws IOException {
        return localFS.exists(new Path(path));
    }
    public boolean isDirectory(String path) throws IOException {
        return localFS.isDirectory(new Path(path));
    }
    public boolean isFile(String path) throws IOException {
        return localFS.isFile(new Path(path));
    }

    public String[] readdir(String path) throws IOException {
        FileStatus[] p = localFS.listStatus(new Path(path));
        String[] entries = null;

        if (p == null) {
            return null;
        }

        entries = new String[p.length];
        for (int i = 0; i < p.length; i++)
            entries[i] = p[i].getPath().toString();
        return entries;
    }

    public FileStatus[] readdirplus(Path path) throws IOException {
        return localFS.listStatus(path);
    }

    public int mkdirs(String path) throws IOException {
        if (localFS.mkdirs(new Path(path)))
            return 0;

        return -1;
    }

    public int rename(String source, String dest) throws IOException {
        if (localFS.rename(new Path(source), new Path(dest)))
            return 0;
        return -1;
    }

    public int rmdir(String path) throws IOException {
        if (isDirectory(path)) {
            // the directory better be empty
            String[] dirEntries = readdir(path);
            if ((dirEntries.length <= 2) && (localFS.delete(new Path(path), true)))
                return 0;
        }
        return -1;
    }

    public int remove(String path) throws IOException {
        if (isFile(path) && (localFS.delete(new Path(path), true)))
            return 0;
        return -1;
    }

    public long filesize(String path) throws IOException {
        return localFS.getLength(new Path(path));
    }
    public short getReplication(String path) throws IOException {
        return 1;
    }
    public short setReplication(String path, short replication) throws IOException {
        return 1;
    }
    public String[][] getDataLocation(String path, long start, long len) throws IOException {
        BlockLocation[] blkLocations = 
          localFS.getFileBlockLocations(localFS.getFileStatus(new Path(path)),
              start, len);
          if ((blkLocations == null) || (blkLocations.length == 0)) {
            return new String[0][];     
          }
          int blkCount = blkLocations.length;
          String[][]hints = new String[blkCount][];
          for (int i=0; i < blkCount ; i++) {
            String[] hosts = blkLocations[i].getHosts();
            hints[i] = new String[hosts.length];
            hints[i] = hosts;
          }
          return hints;
    }

    public long getModificationTime(String path) throws IOException {
        FileStatus s = localFS.getFileStatus(new Path(path));
        if (s == null)
            return 0;

        return s.getModificationTime();
    }

    public FSDataOutputStream create(String path, short replication, int bufferSize) throws IOException {
        // besides path/overwrite, the other args don't matter for
        // testing purposes.
        return localFS.create(new Path(path));
    }

    public FSDataInputStream open(String path, int bufferSize) throws IOException {
        return localFS.open(new Path(path));
    }

    
};
