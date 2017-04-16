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
 * Implements the Hadoop FSInputStream interfaces to allow applications to read
 * files in Kosmos File System (KFS).
 */

package org.apache.hadoop.fs.kfs;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSInputStream;

import org.kosmix.kosmosfs.access.KfsAccess;
import org.kosmix.kosmosfs.access.KfsInputChannel;

class KFSInputStream extends FSInputStream {

    private KfsInputChannel kfsChannel;
    private FileSystem.Statistics statistics;
    private long fsize;

    @Deprecated
    public KFSInputStream(KfsAccess kfsAccess, String path) {
      this(kfsAccess, path, null);
    }

    public KFSInputStream(KfsAccess kfsAccess, String path,
                            FileSystem.Statistics stats) {
        this.statistics = stats;
        this.kfsChannel = kfsAccess.kfs_open(path);
        if (this.kfsChannel != null)
            this.fsize = kfsAccess.kfs_filesize(path);
        else
            this.fsize = 0;
    }

    public long getPos() throws IOException {
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        return kfsChannel.tell();
    }

    public synchronized int available() throws IOException {
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        return (int) (this.fsize - getPos());
    }

    public synchronized void seek(long targetPos) throws IOException {
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        kfsChannel.seek(targetPos);
    }

    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    public synchronized int read() throws IOException {
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
        byte b[] = new byte[1];
        int res = read(b, 0, 1);
        if (res == 1) {
          if (statistics != null) {
            statistics.incrementBytesRead(1);
          }
          return ((int) (b[0] & 0xff));
        }
        return -1;
    }

    public synchronized int read(byte b[], int off, int len) throws IOException {
        if (kfsChannel == null) {
            throw new IOException("File closed");
        }
	int res;

	res = kfsChannel.read(ByteBuffer.wrap(b, off, len));
	// Use -1 to signify EOF
	if (res == 0)
	    return -1;
	if (statistics != null) {
	  statistics.incrementBytesRead(res);
	}
	return res;
    }

    public synchronized void close() throws IOException {
        if (kfsChannel == null) {
            return;
        }

        kfsChannel.close();
        kfsChannel = null;
    }

    public boolean markSupported() {
        return false;
    }

    public void mark(int readLimit) {
        // Do nothing
    }

    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }

}
