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
 * Provide the implementation of KFS which turn into calls to KfsAccess.
 */

package org.apache.hadoop.fs.kfs;

import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.kosmix.kosmosfs.access.KfsAccess;
import org.kosmix.kosmosfs.access.KfsFileAttr;

class KFSImpl implements IFSImpl {
    private KfsAccess kfsAccess = null;
    private FileSystem.Statistics statistics;

    @Deprecated
    public KFSImpl(String metaServerHost, int metaServerPort
                   ) throws IOException {
      this(metaServerHost, metaServerPort, null);
    }

    public KFSImpl(String metaServerHost, int metaServerPort, 
                   FileSystem.Statistics stats) throws IOException {
        kfsAccess = new KfsAccess(metaServerHost, metaServerPort);
        statistics = stats;
    }

    public boolean exists(String path) throws IOException {
        return kfsAccess.kfs_exists(path);
    }

    public boolean isDirectory(String path) throws IOException {
        return kfsAccess.kfs_isDirectory(path);
    }

    public boolean isFile(String path) throws IOException {
        return kfsAccess.kfs_isFile(path);
    }

    public String[] readdir(String path) throws IOException {
        return kfsAccess.kfs_readdir(path);
    }

    public FileStatus[] readdirplus(Path path) throws IOException {
        String srep = path.toUri().getPath();
        KfsFileAttr[] fattr = kfsAccess.kfs_readdirplus(srep);
        if (fattr == null)
            return null;
        int numEntries = 0;
        for (int i = 0; i < fattr.length; i++) {
            if ((fattr[i].filename.compareTo(".") == 0) || (fattr[i].filename.compareTo("..") == 0))
                continue;
            numEntries++;
        }
        FileStatus[] fstatus = new FileStatus[numEntries];
        int j = 0;
        for (int i = 0; i < fattr.length; i++) {
            if ((fattr[i].filename.compareTo(".") == 0) || (fattr[i].filename.compareTo("..") == 0))
                continue;
            Path fn = new Path(path, fattr[i].filename);

            if (fattr[i].isDirectory)
                fstatus[j] = new FileStatus(0, true, 1, 0, fattr[i].modificationTime, fn);
            else
                fstatus[j] = new FileStatus(fattr[i].filesize, fattr[i].isDirectory,
                                            fattr[i].replication,
                                            (long)
                                            (1 << 26),
                                            fattr[i].modificationTime,
                                            fn);

            j++;
        }
        return fstatus;
    }


    public int mkdirs(String path) throws IOException {
        return kfsAccess.kfs_mkdirs(path);
    }

    public int rename(String source, String dest) throws IOException {
        return kfsAccess.kfs_rename(source, dest);
    }

    public int rmdir(String path) throws IOException {
        return kfsAccess.kfs_rmdir(path);
    }

    public int remove(String path) throws IOException {
        return kfsAccess.kfs_remove(path);
    }

    public long filesize(String path) throws IOException {
        return kfsAccess.kfs_filesize(path);
    }

    public short getReplication(String path) throws IOException {
        return kfsAccess.kfs_getReplication(path);
    }

    public short setReplication(String path, short replication) throws IOException {
        return kfsAccess.kfs_setReplication(path, replication);
    }

    public String[][] getDataLocation(String path, long start, long len) throws IOException {
        return kfsAccess.kfs_getDataLocation(path, start, len);
    }

    public long getModificationTime(String path) throws IOException {
        return kfsAccess.kfs_getModificationTime(path);
    }

    public FSDataOutputStream create(String path, short replication, int bufferSize) throws IOException {
        return new FSDataOutputStream(new KFSOutputStream(kfsAccess, path, replication), 
                                      statistics);
    }

    public FSDataInputStream open(String path, int bufferSize) throws IOException {
        return new FSDataInputStream(new KFSInputStream(kfsAccess, path, 
                                                        statistics));
    }
}
