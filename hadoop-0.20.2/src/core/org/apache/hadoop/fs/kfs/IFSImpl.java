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
 * having a KFS deployment.  In particular, the glue code that wraps
 * around calls to KfsAccess object.  This is accomplished by defining a
 * filesystem implementation interface:  
 *   -- for testing purposes, a dummy implementation of this interface
 * will suffice; as long as the dummy implementation is close enough
 * to doing what KFS does, we are good.
 *   -- for deployment purposes with KFS, this interface is
 * implemented by the KfsImpl object.
 */

package org.apache.hadoop.fs.kfs;

import java.io.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

interface IFSImpl {
    public boolean exists(String path) throws IOException;
    public boolean isDirectory(String path) throws IOException;
    public boolean isFile(String path) throws IOException;
    public String[] readdir(String path) throws IOException;
    public FileStatus[] readdirplus(Path path) throws IOException;

    public int mkdirs(String path) throws IOException;
    public int rename(String source, String dest) throws IOException;

    public int rmdir(String path) throws IOException; 
    public int remove(String path) throws IOException;
    public long filesize(String path) throws IOException;
    public short getReplication(String path) throws IOException;
    public short setReplication(String path, short replication) throws IOException;
    public String[][] getDataLocation(String path, long start, long len) throws IOException;

    public long getModificationTime(String path) throws IOException;
    public FSDataOutputStream create(String path, short replication, int bufferSize) throws IOException;
    public FSDataInputStream open(String path, int bufferSize) throws IOException;
    
};
